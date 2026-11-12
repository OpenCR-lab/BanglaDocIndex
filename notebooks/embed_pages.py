# %% [markdown]
# # Embed page shards with EmbeddingGemma 2 (Colab)
#
# Reads page shards from Google Drive (where the uploader VM puts them), embeds every page
# image, and writes one Parquet file per shard back to Drive under
# `<root>/core/derived/embeddings/model=embeddinggemma-2/tokens=<budget>/v=1/<shard>.parquet`;
# the uploader VM copies these into GCS (core/ stays primary there).
#
# No interactive sign-in: Drive is accessed with the Google Drive API (Python client) using
# the uploader's limited `drive.file` credentials (only files the pipeline itself created),
# read from the Colab secret RCLONE_COLAB_CONF (or, if CONF_URL is set, a short-lived signed
# URL). Resumable: shards with an existing output are skipped, so the notebook can be
# re-run after a disconnect or once a day as new shards arrive. Use an L4 or A100 runtime
# (bfloat16; the model must not run in float16).

# %%
# --- settings --------------------------------------------------------------------------
CONF_URL = ""  # empty: read the Colab secret RCLONE_COLAB_CONF; or paste a signed URL
DRIVE_ROOT = "bangla-ocr-dataset"
MODEL_ID = "google/embeddinggemma-2"
MAX_SOFT_TOKENS = 70  # vision token budget per page (decided: 70; DESIGN.md section 11)
BATCH = 64
MAX_HOURS = 11.0  # stop starting new shards after this long (session limits)
MAX_SHARDS = 0  # 0 = no limit
DECODE_THREADS = 8

# %%
import configparser
import json
import os
import subprocess
import sys
import time
import urllib.request

subprocess.run([sys.executable, "-m", "pip", "install", "-q", "-U",
                "sentence-transformers>=6.1", "transformers", "pyarrow"], check=True)

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload

config = configparser.ConfigParser()
# Settings file: from the Colab secret RCLONE_COLAB_CONF (base64 of rclone-colab.conf; no expiry)
# when CONF_URL is empty, otherwise from a short-lived signed URL.
if CONF_URL and not CONF_URL.startswith("__"):
    conf_text = urllib.request.urlopen(CONF_URL).read().decode()
else:
    import base64
    from google.colab import userdata
    conf_text = base64.b64decode(userdata.get("RCLONE_COLAB_CONF")).decode()
config.read_string(conf_text)
section = config["drive"]
token = json.loads(section["token"])
creds = Credentials(None, refresh_token=token["refresh_token"],
                    token_uri="https://oauth2.googleapis.com/token",
                    client_id=section["client_id"], client_secret=section["client_secret"],
                    scopes=["https://www.googleapis.com/auth/drive.file"])
service = build("drive", "v3", credentials=creds, cache_discovery=False)
FOLDER = "application/vnd.google-apps.folder"


def children(parent: str, folders_only: bool = False) -> list[dict]:
    query = f"'{parent}' in parents and trashed=false"
    if folders_only:
        query += f" and mimeType='{FOLDER}'"
    items, page = [], None
    while True:
        response = service.files().list(q=query, pageSize=1000, pageToken=page,
                                        fields="nextPageToken, files(id, name, mimeType)"
                                        ).execute()
        items += response["files"]
        page = response.get("nextPageToken")
        if not page:
            return items


def folder(path: str, create: bool = False) -> str | None:
    parent = "root"
    for name in path.strip("/").split("/"):
        match = [f for f in children(parent, folders_only=True) if f["name"] == name]
        if match:
            parent = match[0]["id"]
        elif create:
            parent = service.files().create(
                body={"name": name, "mimeType": FOLDER, "parents": [parent]},
                fields="id").execute()["id"]
        else:
            return None
    return parent


def walk_files(parent: str, prefix: str = "") -> list[tuple[str, str]]:
    out = []
    for item in children(parent):
        if item["mimeType"] == FOLDER:
            out += walk_files(item["id"], f"{prefix}{item['name']}/")
        else:
            out.append((f"{prefix}{item['name']}", item["id"]))
    return out


def download(file_id: str, local: str) -> None:
    with open(local, "wb") as handle:
        downloader = MediaIoBaseDownload(handle, service.files().get_media(fileId=file_id),
                                         chunksize=64 * 1024 * 1024)
        done = False
        while not done:
            _, done = downloader.next_chunk()


def exists(folder_id: str, name: str) -> bool:
    query = f"'{folder_id}' in parents and name='{name}' and trashed=false"
    return bool(service.files().list(q=query, fields="files(id)").execute()["files"])


def upload(local: str, folder_id: str, name: str) -> None:
    # A read timeout on the upload response ended a whole session on 2026-10-30 although Drive
    # had stored the file. Retry a few times; before each retry, check whether the previous
    # attempt landed, so a retry never leaves two files with the same name.
    for attempt in range(5):
        try:
            if attempt and exists(folder_id, name):
                return
            service.files().create(body={"name": name, "parents": [folder_id]},
                                   media_body=MediaFileUpload(local, resumable=True),
                                   fields="id").execute()
            return
        except Exception as exc:  # noqa: BLE001 - network errors come in many types
            if attempt == 4:
                raise
            print(f"upload of {name} failed ({exc!r}); retrying")
            time.sleep(10 * (attempt + 1))


root_id = folder(DRIVE_ROOT)
print("dataset folder:", DRIVE_ROOT, root_id)

# %%
import concurrent.futures as cf
import io
import tarfile
import time

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import torch
from PIL import Image
from sentence_transformers import SentenceTransformer

dtype = torch.bfloat16 if torch.cuda.is_bf16_supported() else torch.float32
model = SentenceTransformer(MODEL_ID, device="cuda", model_kwargs={"torch_dtype": dtype},
                            config_kwargs={"audio_config": None})
gpu = torch.cuda.get_device_name(0)
print("GPU:", gpu, "dtype:", dtype)

out_dir = f"core/derived/embeddings/model=embeddinggemma-2/tokens={MAX_SOFT_TOKENS}/v=1"
out_id = folder(f"{DRIVE_ROOT}/{out_dir}", create=True)
done = {name.removesuffix(".parquet") for name, _ in walk_files(out_id)}
pages_id = folder(f"{DRIVE_ROOT}/blobs/pages")
shards = sorted(f for f in walk_files(pages_id) if f[0].endswith(".tar"))
todo = [s for s in shards if os.path.basename(s[0]).removesuffix(".tar") not in done]
print(f"{len(shards)} shards on Drive, {len(done)} already embedded, {len(todo)} to do")

# %%
# At 70 soft tokens the image processor works on ~630 patches (about 330 x 490 px), so large
# scans are decoded straight at a reduced JPEG scale (draft mode), keeping the long side at least
# DRAFT_LONG_SIDE. Measured on L4 (2026-10-28): 7.9 MP IA scans 13-18 -> 28-31 pages/s, normal
# pages unchanged; cosine to full-size decoding mean 1.000, 1st percentile >= 0.995.
DRAFT_LONG_SIDE = 1536


def decode(data: bytes) -> Image.Image:
    image = Image.open(io.BytesIO(data))
    width, height = image.size
    scale = DRAFT_LONG_SIDE / max(width, height)
    if scale < 1:
        image.draft("RGB", (max(1, int(width * scale)), max(1, int(height * scale))))
    image.load()
    return image.convert("RGB")


# The next shard is downloaded in a background thread while the current one is embedded, so
# the GPU does not wait for Drive (in the first main-run session, the copy was ~30% of the time).
# The Drive client is not thread-safe, so the downloader thread has its own.
fetch_service = build("drive", "v3", credentials=creds, cache_discovery=False)


def fetch(shard: tuple[str, str]) -> tuple[str, str, float]:
    relative, file_id = shard
    name = os.path.basename(relative).removesuffix(".tar")
    local = f"/content/{name}.tar"
    t0 = time.time()
    # A dropped connection mid-download (http.client.IncompleteRead) ended a whole session on
    # 2026-10-28; retry the shard from the start a few times before giving up.
    for attempt in range(5):
        try:
            with open(local, "wb") as handle:
                downloader = MediaIoBaseDownload(
                    handle, fetch_service.files().get_media(fileId=file_id),
                    chunksize=64 * 1024 * 1024)
                done = False
                while not done:
                    _, done = downloader.next_chunk()
            return name, local, time.time() - t0
        except Exception as exc:  # noqa: BLE001 - network errors come in many types
            if attempt == 4:
                raise
            print(f"download of {name} failed ({exc!r}); retrying")
            time.sleep(10 * (attempt + 1))


def embed_shard(name: str, local: str, copy_s: float) -> dict:
    timings = {"shard": name, "copy_s": copy_s, "bytes": os.path.getsize(local)}
    t0 = time.time()

    ids, payloads = [], []
    with tarfile.open(local, "r:") as tar:
        for member in tar:
            if member.name.endswith(".jpg"):
                ids.append(member.name.removesuffix(".jpg"))
                payloads.append(tar.extractfile(member).read())
    os.remove(local)

    vectors, decode_s, model_s = [], 0.0, 0.0
    with cf.ThreadPoolExecutor(DECODE_THREADS) as pool:
        # Decode the next batch while the model runs on this one; decode_s is only the wait.
        starts = list(range(0, len(payloads), BATCH))
        upcoming = [pool.submit(decode, p) for p in payloads[0:BATCH]]
        for start in starts:
            t = time.time()
            images = [f.result() for f in upcoming]
            decode_s += time.time() - t
            upcoming = [pool.submit(decode, p) for p in payloads[start + BATCH:start + 2 * BATCH]]
            t = time.time()
            vectors.append(model.encode(
                [{"image": im} for im in images], batch_size=BATCH,
                processing_kwargs={"image": {"max_soft_tokens": MAX_SOFT_TOKENS}},
                normalize_embeddings=True, convert_to_numpy=True))
            model_s += time.time() - t
    matrix = np.concatenate(vectors).astype(np.float16)
    table = pa.table({
        "page_id": pa.array(ids, pa.string()),
        "embedding": pa.FixedSizeListArray.from_arrays(
            pa.array(matrix.reshape(-1), pa.float16()), matrix.shape[1]),
    })
    out_local = f"/content/{name}.parquet"
    pq.write_table(table, out_local, compression="zstd")
    upload(out_local, out_id, f"{name}.parquet")
    os.remove(out_local)
    timings.update(pages=len(ids), decode_s=decode_s, model_s=model_s,
                   total_s=time.time() - t0, gpu=gpu, tokens=MAX_SOFT_TOKENS, batch=BATCH,
                   decode=f"draft{DRAFT_LONG_SIDE}")
    # total_s excludes copy_s: the copy overlaps the previous shard's embedding.
    timings["pages_per_s"] = len(ids) / timings["total_s"]
    return timings


# %%
started = time.time()
log = []
if MAX_SHARDS:
    todo = todo[:MAX_SHARDS]
with cf.ThreadPoolExecutor(1) as fetcher:
    pending = fetcher.submit(fetch, todo[0]) if todo else None
    for i in range(len(todo)):
        name, local, copy_s = pending.result()
        out_of_time = (time.time() - started) / 3600 > MAX_HOURS
        pending = (fetcher.submit(fetch, todo[i + 1])
                   if i + 1 < len(todo) and not out_of_time else None)
        if out_of_time:
            os.remove(local)
            print("time budget reached; re-run to continue")
            break
        result = embed_shard(name, local, copy_s)
        log.append(result)
        print(json.dumps({k: round(v, 2) if isinstance(v, float) else v
                          for k, v in result.items()}))

stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
with open(f"/content/embed-{stamp}.json", "w") as handle:
    json.dump(log, handle, indent=2)
upload(f"/content/embed-{stamp}.json", folder(f"{DRIVE_ROOT}/core/runs", create=True),
       f"embed-{stamp}.json")
pages = sum(r["pages"] for r in log)
seconds = time.time() - started
print(f"{len(log)} shards, {pages} pages, {seconds/60:.1f} min, "
      f"{pages / max(seconds, 1):.1f} pages/s overall")
