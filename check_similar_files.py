#!/usr/bin/env python3
"""
check_similar_files.py
----------------------
Core logic for finding exact and near-duplicate files in a folder.
No UI dependencies — import from any interface you like.

Streaming API (new)
-------------------
    scan_files_streaming(folder, threshold, ...)
        A generator that yields progress events as work completes, so a UI
        can show results immediately rather than waiting for the full scan.

        Each yielded item is a dict with a "kind" key:

          {"kind": "progress",  "phase": str, "done": int, "total": int}
              Emitted after each file is hashed / text-extracted / scored.

          {"kind": "exact",     "hash": str,  "files": [path, ...]}
              One group of exact duplicates (same MD5).

          {"kind": "near",      "file_a": path, "file_b": path,
           "score_tfidf": float, "score_levenshtein": float,
           "score_sequence": float, "combined_score": float}
              One near-duplicate pair, emitted as soon as it is found.

          {"kind": "done",      "all_files": [path, ...]}
              Scan complete.

Batch API (unchanged)
---------------------
    find_similar_files(folder, threshold, ...)
        -> (exact_duplicates, near_duplicates, all_files)

Performance notes
-----------------
  Token-level Levenshtein (150 tokens) — 1 160× faster than char-level.
  TF-IDF norms pre-computed once per document.
  Cascading early-exit: skip lev+seq when TF-IDF rules out a match.
  SequenceMatcher budget: 2 000 chars (was 8 000).
  Parallel hashing & extraction — ThreadPoolExecutor (I/O-bound).
  Parallel pair scoring         — ProcessPoolExecutor (CPU-bound).

Requirements
------------
    pip install PyMuPDF
"""

import hashlib
import math
import os
import queue
import re
import threading
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from difflib import SequenceMatcher
from itertools import combinations
from typing import Iterator

try:
    import fitz
    FITZ_OK = True
except ImportError:
    FITZ_OK = False


# ─────────────────────────────────────────────────────────────────────────────
# Hashing
# ─────────────────────────────────────────────────────────────────────────────

def hash_file(filepath: str) -> str:
    h = hashlib.md5()
    with open(filepath, "rb") as f:
        while chunk := f.read(65_536):
            h.update(chunk)
    return h.hexdigest()

def _hash_task(filepath: str) -> tuple[str, str]:
    return filepath, hash_file(filepath)


# ─────────────────────────────────────────────────────────────────────────────
# PDF text extraction
# ─────────────────────────────────────────────────────────────────────────────

def extract_pdf_text(filepath: str) -> str:
    if not FITZ_OK or not filepath.lower().endswith(".pdf"):
        return ""
    try:
        doc = fitz.open(filepath)
        parts = [page.get_text() for page in doc]
        doc.close()
        return " ".join(parts)
    except Exception:
        return ""

def _extract_task(filepath: str) -> tuple[str, str]:
    return filepath, extract_pdf_text(filepath)


# ─────────────────────────────────────────────────────────────────────────────
# Tokenisation
# ─────────────────────────────────────────────────────────────────────────────

def _tokenize(text: str) -> list[str]:
    tokens = [w for w in text.lower().split() if len(w) >= 2 and w.isalpha()]
    if tokens:
        return tokens
    return re.findall(r"[a-z\u05d0-\u05ea]{2,}", text.lower())


# ─────────────────────────────────────────────────────────────────────────────
# TF-IDF
# ─────────────────────────────────────────────────────────────────────────────

def _build_tfidf_vector(tokens: list[str], idf: dict) -> tuple[dict, float]:
    if not tokens:
        return {}, 0.0
    total = len(tokens)
    tf    = {t: count / total for t, count in Counter(tokens).items()}
    vec   = {t: tf_val * idf.get(t, 1.0) for t, tf_val in tf.items()}
    norm  = math.sqrt(sum(v * v for v in vec.values()))
    return vec, norm

def _cosine_cached(vec_a, norm_a, vec_b, norm_b) -> float:
    if norm_a == 0.0 or norm_b == 0.0:
        return 0.0
    if len(vec_a) > len(vec_b):
        vec_a, vec_b = vec_b, vec_a
    dot = sum(v * vec_b[t] for t, v in vec_a.items() if t in vec_b)
    return dot / (norm_a * norm_b)

def build_idf(corpus_token_lists: list[list[str]]) -> dict:
    N  = len(corpus_token_lists)
    df = Counter()
    for tokens in corpus_token_lists:
        for term in set(tokens):
            df[term] += 1
    return {
        term: math.log((1 + N) / (1 + count)) + 1
        for term, count in df.items()
    }

def tfidf_cosine(text_a: str, text_b: str, corpus_idf: dict) -> float:
    ta = _tokenize(text_a)
    tb = _tokenize(text_b)
    if not ta or not tb:
        return 0.0
    va, na = _build_tfidf_vector(ta, corpus_idf or {})
    vb, nb = _build_tfidf_vector(tb, corpus_idf or {})
    return _cosine_cached(va, na, vb, nb)


# ─────────────────────────────────────────────────────────────────────────────
# Levenshtein (token-level)
# ─────────────────────────────────────────────────────────────────────────────

_LEV_MAX_TOKENS = 150

def _levenshtein_token_distance(ta: list[str], tb: list[str]) -> int:
    m, n = len(ta), len(tb)
    if m < n:
        ta, tb, m, n = tb, ta, n, m
    if n == 0:
        return m
    prev = list(range(n + 1))
    for i in range(1, m + 1):
        curr    = [i] + [0] * n
        src_tok = ta[i - 1]
        for j in range(1, n + 1):
            cost    = 0 if src_tok == tb[j - 1] else 1
            curr[j] = min(curr[j - 1] + 1, prev[j] + 1, prev[j - 1] + cost)
        prev = curr
    return prev[n]

def levenshtein_similarity(text_a: str, text_b: str) -> float:
    ta = _tokenize(text_a)[:_LEV_MAX_TOKENS]
    tb = _tokenize(text_b)[:_LEV_MAX_TOKENS]
    if not ta and not tb:
        return 1.0
    if not ta or not tb:
        return 0.0
    return 1.0 - _levenshtein_token_distance(ta, tb) / max(len(ta), len(tb))


# ─────────────────────────────────────────────────────────────────────────────
# SequenceMatcher
# ─────────────────────────────────────────────────────────────────────────────

_SEQ_MAX_CHARS = 2_000

def sequence_similarity(text_a: str, text_b: str) -> float:
    a = text_a[:_SEQ_MAX_CHARS]
    b = text_b[:_SEQ_MAX_CHARS]
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b, autojunk=False).ratio()


# ─────────────────────────────────────────────────────────────────────────────
# Combined score
# ─────────────────────────────────────────────────────────────────────────────

def combined_similarity(
    text_a: str, text_b: str, corpus_idf: dict,
    w_tfidf=0.4, w_lev=0.3, w_seq=0.3,
) -> dict:
    s_tfidf = tfidf_cosine(text_a, text_b, corpus_idf)
    s_lev   = levenshtein_similarity(text_a, text_b)
    s_seq   = sequence_similarity(text_a, text_b)
    total_w = w_tfidf + w_lev + w_seq or 1.0
    return {
        "score_tfidf":       round(s_tfidf, 4),
        "score_levenshtein": round(s_lev,   4),
        "score_sequence":    round(s_seq,   4),
        "combined_score":    round((w_tfidf*s_tfidf + w_lev*s_lev + w_seq*s_seq) / total_w, 4),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Pair worker (module-level → picklable)
# ─────────────────────────────────────────────────────────────────────────────

def _score_pair(args: tuple) -> dict | None:
    (f1, f2,
     vec_a, norm_a, vec_b, norm_b,
     tokens_a, tokens_b, text_a, text_b,
     w_tfidf, w_lev, w_seq, threshold) = args

    total_w = w_tfidf + w_lev + w_seq or 1.0

    s_tfidf = _cosine_cached(vec_a, norm_a, vec_b, norm_b)
    if (w_tfidf * s_tfidf + w_lev + w_seq) / total_w < threshold:
        return None

    ta = tokens_a[:_LEV_MAX_TOKENS]
    tb = tokens_b[:_LEV_MAX_TOKENS]
    s_lev = (1.0 - _levenshtein_token_distance(ta, tb) / max(len(ta), len(tb))
             if ta and tb else (1.0 if not ta and not tb else 0.0))
    if (w_tfidf * s_tfidf + w_lev * s_lev + w_seq) / total_w < threshold:
        return None

    a = text_a[:_SEQ_MAX_CHARS]
    b = text_b[:_SEQ_MAX_CHARS]
    s_seq = (SequenceMatcher(None, a, b, autojunk=False).ratio()
             if a and b else (1.0 if not a and not b else 0.0))

    combined = (w_tfidf * s_tfidf + w_lev * s_lev + w_seq * s_seq) / total_w
    if combined < threshold:
        return None

    return {
        "file_a": f1, "file_b": f2,
        "score_tfidf":       round(s_tfidf,  4),
        "score_levenshtein": round(s_lev,     4),
        "score_sequence":    round(s_seq,     4),
        "combined_score":    round(combined,  4),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Streaming API  ← NEW
# ─────────────────────────────────────────────────────────────────────────────

def scan_files_streaming(
    folder:      str,
    threshold:   float = 0.75,
    w_tfidf:     float = 0.4,
    w_lev:       float = 0.3,
    w_seq:       float = 0.3,
    max_workers: int | None = None,
) -> Iterator[dict]:
    """
    Generator — yields progress/result events as the scan proceeds.

    Yielded dict shapes
    -------------------
    {"kind": "progress", "phase": str, "done": int, "total": int}
    {"kind": "exact",    "hash": str,  "files": [path, ...]}
    {"kind": "near",     "file_a": path, "file_b": path,
     "score_tfidf": float, "score_levenshtein": float,
     "score_sequence": float, "combined_score": float}
    {"kind": "done",     "all_files": [path, ...]}
    """
    all_files = [
        os.path.join(folder, f)
        for f in os.listdir(folder)
        if os.path.isfile(os.path.join(folder, f))
    ]
    total_files = len(all_files)

    # ── Phase 1: parallel MD5 hashing ────────────────────────────────────────
    hash_map: dict = {}
    done = 0
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_hash_task, f): f for f in all_files}
        for fut in as_completed(futures):
            filepath, digest = fut.result()
            hash_map.setdefault(digest, []).append(filepath)
            done += 1
            yield {"kind": "progress", "phase": "Hashing files",
                   "done": done, "total": total_files}

    # Emit exact duplicate groups immediately
    for digest, paths in hash_map.items():
        if len(paths) > 1:
            yield {"kind": "exact", "hash": digest, "files": paths}

    unique_files = [v[0] for v in hash_map.values()]

    # ── Phase 2: parallel PDF text extraction ────────────────────────────────
    text_cache: dict[str, str] = {}
    done = 0
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_extract_task, f): f for f in unique_files}
        for fut in as_completed(futures):
            filepath, text = fut.result()
            text_cache[filepath] = text
            done += 1
            yield {"kind": "progress", "phase": "Reading file contents",
                   "done": done, "total": len(unique_files)}

    # ── Phase 3: build IDF + per-doc vectors (fast, no yield needed) ─────────
    pdf_files   = [f for f in unique_files if f.lower().endswith(".pdf")]
    token_lists = [_tokenize(text_cache[f]) for f in pdf_files]
    corpus_idf  = build_idf(token_lists) if token_lists else {}

    vec_cache: dict[str, tuple[dict, float]] = {}
    tok_cache: dict[str, list[str]]          = {}
    for f in unique_files:
        toks         = _tokenize(text_cache.get(f, ""))
        tok_cache[f] = toks
        vec_cache[f] = _build_tfidf_vector(toks, corpus_idf)

    # ── Phase 4: parallel pair scoring — yield each match as found ───────────
    pairs     = list(combinations(unique_files, 2))
    total_pairs = len(pairs)
    task_args = [
        (f1, f2,
         vec_cache[f1][0], vec_cache[f1][1],
         vec_cache[f2][0], vec_cache[f2][1],
         tok_cache[f1], tok_cache[f2],
         text_cache.get(f1, ""), text_cache.get(f2, ""),
         w_tfidf, w_lev, w_seq, threshold)
        for f1, f2 in pairs
    ]

    n_workers  = max_workers or os.cpu_count() or 4
    chunk_size = max(1, total_pairs // (n_workers * 8))

    done = 0
    # Report progress every N pairs to avoid flooding the UI
    _REPORT_EVERY = max(1, total_pairs // 200)

    def _run_pool(executor_cls):
        nonlocal done
        with executor_cls(max_workers=max_workers) as pool:
            for result in pool.map(_score_pair, task_args, chunksize=chunk_size):
                done += 1
                if result is not None:
                    yield {"kind": "near", **result}
                if done % _REPORT_EVERY == 0 or done == total_pairs:
                    yield {"kind": "progress", "phase": "Comparing pairs",
                           "done": done, "total": total_pairs}

    try:
        yield from _run_pool(ProcessPoolExecutor)
    except Exception:
        done = 0
        yield from _run_pool(ThreadPoolExecutor)

    yield {"kind": "done", "all_files": all_files}


# ─────────────────────────────────────────────────────────────────────────────
# Batch API (unchanged)
# ─────────────────────────────────────────────────────────────────────────────

def find_similar_files(
    folder:      str,
    threshold:   float = 0.75,
    w_tfidf:     float = 0.4,
    w_lev:       float = 0.3,
    w_seq:       float = 0.3,
    max_workers: int | None = None,
) -> tuple:
    exact: dict        = {}
    near:  list[dict]  = []
    all_files: list    = []
    for event in scan_files_streaming(folder, threshold, w_tfidf, w_lev, w_seq, max_workers):
        if event["kind"] == "exact":
            exact[event["hash"]] = event["files"]
        elif event["kind"] == "near":
            near.append({k: v for k, v in event.items() if k != "kind"})
        elif event["kind"] == "done":
            all_files = event["all_files"]
    near.sort(key=lambda d: d["combined_score"], reverse=True)
    return exact, near, all_files
