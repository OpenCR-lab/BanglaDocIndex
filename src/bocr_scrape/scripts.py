"""Script composition of text (measured and stored, never used to filter at ingest).

DESIGN.md: ingest keeps every item labelled Bengali by its source; the share of each script
is recorded so language analysis and any later filtering can be reproduced from the catalog.
"""

from __future__ import annotations

import functools

import numpy as np

SCRIPTS = ("bengali", "latin", "arabic", "devanagari", "other")
_RANGES = {
    "bengali": [(0x0980, 0x09FF)],
    "latin": [(0x0000, 0x024F)],  # Basic Latin, Latin-1, Latin Extended-A/B
    "arabic": [(0x0600, 0x06FF), (0x0750, 0x077F), (0x08A0, 0x08FF), (0xFB50, 0xFDFF),
               (0xFE70, 0xFEFF)],
    "devanagari": [(0x0900, 0x097F)],
}


@functools.cache
def _alpha_table() -> np.ndarray:
    """isalpha() for every code point, so counting is vectorized but identical to str.isalpha."""
    return np.fromiter((chr(i).isalpha() for i in range(0x110000)), dtype=bool,
                       count=0x110000)


def script_composition(text: str) -> dict[str, float | int]:
    """Share of alphabetic characters per script, plus the number of letters measured.

    Shares are over alphabetic characters (str.isalpha), the same denominator as the v1
    Bengali ratio, so `bengali` here equals that ratio.
    """
    codes = np.frombuffer(text.encode("utf-32-le", errors="ignore"), dtype=np.uint32)
    letters = codes[_alpha_table()[codes]]
    total = int(letters.size)
    out: dict[str, float | int] = {}
    named = 0
    for name, ranges in _RANGES.items():
        mask = np.zeros(letters.shape, dtype=bool)
        for low, high in ranges:
            mask |= (letters >= low) & (letters <= high)
        count = int(mask.sum())
        named += count
        out[name] = count / total if total else 0.0
    out["other"] = (total - named) / total if total else 0.0
    out["letters"] = total
    return out
