"""Command line interface: `bocr-scrape <command>`."""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

from .config import Settings
from .state import StateStore
from .storage import open_storage

SOURCES = ("ia", "nctb", "finepdfs")


def configure_logging(log_file: Path | None) -> None:
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8", errors="backslashreplace")
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_file, encoding="utf-8"))
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(threadName)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=handlers,
        force=True,
    )
    for noisy in ("urllib3", "google", "internetarchive"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def build_source(args: argparse.Namespace, settings: Settings):
    if args.source == "ia":
        from .sources.internet_archive import DEFAULT_QUERY, InternetArchiveSource

        identifiers = [i.strip() for i in (args.identifiers or "").split(",") if i.strip()]
        return InternetArchiveSource(query=args.query or DEFAULT_QUERY,
                                     max_items=args.max_items, identifiers=identifiers or None)
    if args.source == "nctb":
        from .sources.nctb import NCTBSource

        classes = [c.strip() for c in (args.classes or "").split(",") if c.strip()]
        return NCTBSource(classes=classes or None, max_items=args.max_items)
    if args.source == "finepdfs":
        from .sources.finepdfs import FinePDFsSource

        manifest = ensure_finepdfs_manifest(args, settings)
        return FinePDFsSource(manifest, max_items=args.max_items,
                              include_live=not args.no_live)
    raise SystemExit(f"unknown source {args.source}")


def ensure_finepdfs_manifest(args: argparse.Namespace, settings: Settings) -> Path:
    """Build the FinePDFs manifest once per run and keep a copy in core/sources/."""
    from .sources.finepdfs import build_manifest

    path = Path(args.manifest) if args.manifest else (
        settings.work_dir / "finepdfs" / "manifest.parquet")
    if not path.exists():
        logging.getLogger(__name__).info("Building FinePDFs manifest at %s", path)
        build_manifest(path, limit=args.manifest_limit,
                       ca_cert_file=os.getenv("DUCKDB_CA_CERT_FILE"))
        storage = open_storage(settings.data_uri)
        key = f"core/sources/finepdfs/manifest-{path.stat().st_size}.parquet"
        storage.put_file(path, key, content_type="application/vnd.apache.parquet",
                         if_absent=True)
    return path


def cmd_ingest(args: argparse.Namespace, settings: Settings) -> int:
    from .engine import IngestEngine, restore_checkpoint

    storage = open_storage(settings.data_uri)
    if args.resume_run:
        restore_checkpoint(settings, storage, args.source, args.resume_run)
    source = build_source(args, settings)
    engine = IngestEngine(settings, source, storage, cli_args=vars(args))
    return engine.run()


def cmd_status(args: argparse.Namespace, settings: Settings) -> int:
    path = settings.work_dir / args.source / "state" / "ingest.sqlite"
    if not path.exists():
        print(f"No local state at {path}")
        return 1
    state = StateStore(path)
    try:
        report = {"run_id": state.get_meta("run_id"), **state.stats()}
    finally:
        state.close()
    print(json.dumps(report, indent=2))
    return 0


def cmd_retry_failed(args: argparse.Namespace, settings: Settings) -> int:
    path = settings.work_dir / args.source / "state" / "ingest.sqlite"
    state = StateStore(path)
    try:
        print(f"Reset {state.reset_permanent_failures()} permanently failed documents to pending")
    finally:
        state.close()
    return 0


def cmd_verify(args: argparse.Namespace, settings: Settings) -> int:
    from .verify import verify

    report = verify(open_storage(settings.data_uri), args.source, args.run,
                    check_pages=args.check_pages, split_val=settings.split_val,
                    split_test=settings.split_test)
    print(json.dumps(report, indent=2, ensure_ascii=False))
    return 1 if report["problems"] else 0


def cmd_rehydrate(args: argparse.Namespace, settings: Settings) -> int:
    from .rehydrate import rehydrate_sample

    report = rehydrate_sample(settings, open_storage(settings.data_uri), args.source,
                              args.pages, args.out)
    print(json.dumps(report, indent=2))
    return 0


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(prog="bocr-scrape", description=__doc__)
    root.add_argument("--env", type=Path, default=Path(".env"), help="settings file (.env)")
    sub = root.add_subparsers(dest="command", required=True)

    ingest = sub.add_parser("ingest", help="discover, render and upload one source")
    ingest.add_argument("source", choices=SOURCES)
    ingest.add_argument("--max-items", type=int, default=0,
                        help="stop discovery after examining N new source items (0 = all)")
    ingest.add_argument("--identifiers", help="IA only: comma-separated identifiers to ingest")
    ingest.add_argument("--query", help="IA only: search query (default language:Bengali)")
    ingest.add_argument("--classes", help="NCTB only: e.g. class-6,class-9-10")
    ingest.add_argument("--manifest", help="FinePDFs only: manifest parquet to use")
    ingest.add_argument("--manifest-limit", type=int, default=0,
                        help="FinePDFs only: build the manifest from the first N rows (tests)")
    ingest.add_argument("--no-live", action="store_true",
                        help="FinePDFs only: skip documents that need the original website")
    ingest.add_argument("--resume-run", help="restore local state from this run's checkpoint")

    status = sub.add_parser("status", help="show local progress for a source")
    status.add_argument("source", choices=SOURCES)

    retry = sub.add_parser("retry-failed", help="re-queue permanently failed documents")
    retry.add_argument("source", choices=SOURCES)

    check = sub.add_parser("verify", help="check catalog and objects for consistency")
    check.add_argument("--source", choices=SOURCES)
    check.add_argument("--run", help="limit to one run id (requires --source)")
    check.add_argument("--check-pages", type=int, default=200,
                       help="pages to read back from their shards and hash-check (-1 = all)")

    rebuild = sub.add_parser("rehydrate", help="rebuild a sample of pages from the catalog "
                             "and compare them with the stored images")
    rebuild.add_argument("source", choices=SOURCES)
    rebuild.add_argument("--pages", type=int, default=1000)
    rebuild.add_argument("--out", type=Path, help="also write the rebuilt JPEGs here")
    return root


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    if args.env and args.env.exists():
        load_dotenv(args.env)
    settings = Settings.from_env()
    log_file = None
    if args.command == "ingest":
        log_file = settings.work_dir / args.source / "logs" / "ingest.log"
    configure_logging(log_file)
    handlers = {"ingest": cmd_ingest, "status": cmd_status, "retry-failed": cmd_retry_failed,
                "verify": cmd_verify, "rehydrate": cmd_rehydrate}
    return handlers[args.command](args, settings)


if __name__ == "__main__":
    raise SystemExit(main())
