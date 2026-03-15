#!/usr/bin/env python3
"""
check_similar_files.py
----------------------
Core logic for finding exact and near-duplicate files in a folder.
No UI dependencies — import this module from any interface you like.

Public API
----------
    hash_file(filepath)                        -> str
    similarity(a, b)                           -> float  (0.0 – 1.0)
    find_similar_files(folder, threshold=0.85) -> (exact_duplicates, near_duplicates, all_files)

        exact_duplicates : dict[hash_str, list[filepath]]
        near_duplicates  : list[(filepath, filepath, score)]
        all_files        : list[filepath]
"""

import hashlib
import os
from difflib import SequenceMatcher


def hash_file(filepath: str) -> str:
    """Return the MD5 hex digest of a file's binary contents."""
    h = hashlib.md5()
    with open(filepath, "rb") as f:
        while chunk := f.read(8192):
            h.update(chunk)
    return h.hexdigest()


def similarity(a: str, b: str) -> float:
    """Return a 0.0-1.0 similarity score between two strings."""
    return SequenceMatcher(None, a, b).ratio()


def find_similar_files(
    folder: str,
    threshold: float = 0.85,
) -> tuple:
    """
    Scan *folder* (non-recursively) for duplicate files.

    Returns
    -------
    exact_duplicates : dict[md5_hash, list[filepath]]
        Groups of files with identical binary content.
    near_duplicates : list[tuple[filepath, filepath, float]]
        Pairs of files whose basenames are similar (score >= threshold).
        Only one representative per unique-hash group is compared.
    all_files : list[filepath]
        Every file found in the folder.
    """
    all_files = [
        os.path.join(folder, f)
        for f in os.listdir(folder)
        if os.path.isfile(os.path.join(folder, f))
    ]

    hash_map = {}
    for f in all_files:
        h = hash_file(f)
        hash_map.setdefault(h, []).append(f)

    exact_duplicates = {k: v for k, v in hash_map.items() if len(v) > 1}
    unique_files = [v[0] for v in hash_map.values()]

    near_duplicates = []
    checked = set()

    for i, f1 in enumerate(unique_files):
        for j, f2 in enumerate(unique_files):
            if i >= j:
                continue
            pair = (f1, f2)
            if pair in checked:
                continue
            checked.add(pair)
            score = similarity(os.path.basename(f1), os.path.basename(f2))
            if score >= threshold:
                near_duplicates.append((f1, f2, score))

    return exact_duplicates, near_duplicates, all_files
