#!/usr/bin/env python3
"""
check_similar_files.py
----------------------
Core logic for finding exact and near-duplicate PDF files in a folder.
No UI dependencies — import this module from any interface you like.

Near-duplicate detection runs three complementary algorithms on the
extracted PDF text content, then combines their scores into one final
similarity value:

  1. TF-IDF Cosine Similarity
       Best for semantic/topical similarity.  Builds a term-frequency /
       inverse-document-frequency vector for each document and measures
       the cosine of the angle between them.  Two documents that discuss
       the same subject will score high even if they don't share exact
       phrasing.

  2. Levenshtein Distance (normalised)
       Best for catching typos, OCR noise, and near-identical documents
       with small local edits.  Operates on the full text string; the raw
       edit-distance is normalised to a 0-1 similarity score.

  3. SequenceMatcher (difflib, built-in)
       Finds the longest common subsequences between two texts.  Sits
       between the other two: less sensitive to word order than cosine,
       but more structural than pure edit-distance.

The three scores are combined with configurable weights (default: equal
thirds) into a single `combined_score`.  A pair is reported as a
near-duplicate when combined_score >= threshold.

Public API
----------
    hash_file(filepath)                         -> str
    extract_pdf_text(filepath)                  -> str
    tfidf_cosine(text_a, text_b, corpus)        -> float
    levenshtein_similarity(text_a, text_b)      -> float
    sequence_similarity(text_a, text_b)         -> float
    combined_similarity(text_a, text_b, corpus) -> dict
    find_similar_files(
        folder, threshold=0.75,
        w_tfidf=0.4, w_lev=0.3, w_seq=0.3
    ) -> (exact_duplicates, near_duplicates, all_files)

        exact_duplicates : dict[hash_str, list[filepath]]
        near_duplicates  : list[dict]  — each dict has keys:
                             file_a, file_b,
                             score_tfidf, score_levenshtein, score_sequence,
                             combined_score
        all_files        : list[filepath]

Requirements
------------
    pip install PyMuPDF          # fitz  — PDF text extraction
"""

import hashlib
import math
import os
import re
from collections import Counter
from difflib import SequenceMatcher

# ── optional PDF extraction ───────────────────────────────────────────────────
try:
    import fitz  # PyMuPDF
    FITZ_OK = True
except ImportError:
    FITZ_OK = False


# ─────────────────────────────────────────────────────────────────────────────
# Hashing
# ─────────────────────────────────────────────────────────────────────────────

def hash_file(filepath: str) -> str:
    """Return the MD5 hex digest of a file's binary contents."""
    h = hashlib.md5()
    with open(filepath, "rb") as f:
        while chunk := f.read(8192):
            h.update(chunk)
    return h.hexdigest()


# ─────────────────────────────────────────────────────────────────────────────
# PDF text extraction
# ─────────────────────────────────────────────────────────────────────────────

def extract_pdf_text(filepath: str) -> str:
    """
    Extract all text from a PDF using PyMuPDF.
    Returns an empty string on failure or if PyMuPDF is not installed.
    """
    if not FITZ_OK or not filepath.lower().endswith(".pdf"):
        return ""
    try:
        doc = fitz.open(filepath)
        parts = [page.get_text() for page in doc]
        doc.close()
        return " ".join(parts)
    except Exception:
        return ""


# ─────────────────────────────────────────────────────────────────────────────
# Tokenisation (shared by TF-IDF and SequenceMatcher preprocessing)
# ─────────────────────────────────────────────────────────────────────────────

def _tokenize(text: str) -> list:
    """
    Lowercase and extract alphabetic tokens (Latin + Hebrew).
    Minimum token length: 2 characters.
    """
    return re.findall(r"[a-z\u05d0-\u05ea]{2,}", text.lower())


# ─────────────────────────────────────────────────────────────────────────────
# 1. TF-IDF Cosine Similarity
# ─────────────────────────────────────────────────────────────────────────────

def _build_tfidf_vector(tokens: list, idf: dict) -> dict:
    """
    Build a TF-IDF vector for one document.
    TF  = (count of term t in doc) / (total terms in doc)
    IDF = log((1 + N) / (1 + df(t))) + 1   [smooth variant]
    """
    if not tokens:
        return {}
    total  = len(tokens)
    tf     = {t: count / total for t, count in Counter(tokens).items()}
    return {t: tf_val * idf.get(t, 1.0) for t, tf_val in tf.items()}


def _cosine(vec_a: dict, vec_b: dict) -> float:
    """Cosine similarity between two sparse vectors."""
    if not vec_a or not vec_b:
        return 0.0
    common = set(vec_a) & set(vec_b)
    dot    = sum(vec_a[t] * vec_b[t] for t in common)
    norm_a = math.sqrt(sum(v * v for v in vec_a.values()))
    norm_b = math.sqrt(sum(v * v for v in vec_b.values()))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


def build_idf(corpus_token_lists: list) -> dict:
    """
    Compute IDF weights from a corpus of token lists.
    corpus_token_lists : list of token lists, one per document.
    Returns dict[term -> idf_weight].
    """
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
    """
    TF-IDF cosine similarity between two texts.

    Parameters
    ----------
    text_a, text_b : str
        The two documents to compare.
    corpus_idf : dict
        IDF table built from the full corpus with build_idf().
        Pass an empty dict to fall back to plain TF cosine (no IDF weighting).

    Returns
    -------
    float : 0.0 (no overlap) → 1.0 (identical term distribution)
    """
    tokens_a = _tokenize(text_a)
    tokens_b = _tokenize(text_b)

    if not tokens_a or not tokens_b:
        return 0.0

    if corpus_idf:
        vec_a = _build_tfidf_vector(tokens_a, corpus_idf)
        vec_b = _build_tfidf_vector(tokens_b, corpus_idf)
    else:
        # Plain TF cosine fallback
        total_a = len(tokens_a)
        total_b = len(tokens_b)
        vec_a   = {t: c / total_a for t, c in Counter(tokens_a).items()}
        vec_b   = {t: c / total_b for t, c in Counter(tokens_b).items()}

    return _cosine(vec_a, vec_b)


# ─────────────────────────────────────────────────────────────────────────────
# 2. Levenshtein Distance (normalised to 0-1 similarity)
# ─────────────────────────────────────────────────────────────────────────────

def _levenshtein_distance(s: str, t: str) -> int:
    """
    Classic dynamic-programming Levenshtein edit distance.
    Operates on characters; O(m*n) time and O(min(m,n)) space.
    """
    m, n = len(s), len(t)
    if m < n:
        s, t, m, n = t, s, n, m        # ensure m >= n for the space optimisation
    if n == 0:
        return m

    prev = list(range(n + 1))
    for i in range(1, m + 1):
        curr = [i] + [0] * n
        for j in range(1, n + 1):
            cost     = 0 if s[i - 1] == t[j - 1] else 1
            curr[j]  = min(
                curr[j - 1] + 1,        # insertion
                prev[j]     + 1,        # deletion
                prev[j - 1] + cost,     # substitution
            )
        prev = curr
    return prev[n]


# Maximum character budget for Levenshtein (avoids O(n²) blow-up on long PDFs)
_LEV_MAX_CHARS = 4_000


def levenshtein_similarity(text_a: str, text_b: str) -> float:
    """
    Normalised Levenshtein similarity: 1 - distance / max_len.
    Texts are truncated to _LEV_MAX_CHARS characters before comparison to
    keep runtime acceptable for large documents.

    Returns
    -------
    float : 0.0 (completely different) → 1.0 (identical)
    """
    a = text_a[:_LEV_MAX_CHARS]
    b = text_b[:_LEV_MAX_CHARS]
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    max_len = max(len(a), len(b))
    dist    = _levenshtein_distance(a, b)
    return 1.0 - dist / max_len


# ─────────────────────────────────────────────────────────────────────────────
# 3. SequenceMatcher (difflib)
# ─────────────────────────────────────────────────────────────────────────────

# Maximum character budget for SequenceMatcher
_SEQ_MAX_CHARS = 8_000


def sequence_similarity(text_a: str, text_b: str) -> float:
    """
    SequenceMatcher similarity ratio on the full text content.
    Texts are truncated to _SEQ_MAX_CHARS to keep runtime reasonable.

    Uses autojunk=False so every character counts, which is more accurate
    for document comparison than the default 1%-heuristic.

    Returns
    -------
    float : 0.0 → 1.0
    """
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
    text_a: str,
    text_b: str,
    corpus_idf: dict,
    w_tfidf: float = 0.4,
    w_lev:   float = 0.3,
    w_seq:   float = 0.3,
) -> dict:
    """
    Run all three algorithms and return a dict with individual and combined scores.

    Parameters
    ----------
    text_a, text_b : str   — document texts
    corpus_idf     : dict  — IDF table (from build_idf); pass {} for plain TF
    w_tfidf        : float — weight for TF-IDF cosine  (default 0.4)
    w_lev          : float — weight for Levenshtein    (default 0.3)
    w_seq          : float — weight for SequenceMatcher (default 0.3)

    Weights are automatically renormalised if they don't sum to 1.

    Returns
    -------
    {
        "score_tfidf":       float,
        "score_levenshtein": float,
        "score_sequence":    float,
        "combined_score":    float,
    }
    """
    s_tfidf = tfidf_cosine(text_a, text_b, corpus_idf)
    s_lev   = levenshtein_similarity(text_a, text_b)
    s_seq   = sequence_similarity(text_a, text_b)

    total_w = w_tfidf + w_lev + w_seq
    if total_w == 0:
        total_w = 1.0  # guard against zero-weight configs
    combined = (w_tfidf * s_tfidf + w_lev * s_lev + w_seq * s_seq) / total_w

    return {
        "score_tfidf":       round(s_tfidf,   4),
        "score_levenshtein": round(s_lev,      4),
        "score_sequence":    round(s_seq,      4),
        "combined_score":    round(combined,   4),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Main API
# ─────────────────────────────────────────────────────────────────────────────

def find_similar_files(
    folder:    str,
    threshold: float = 0.75,
    w_tfidf:   float = 0.4,
    w_lev:     float = 0.3,
    w_seq:     float = 0.3,
) -> tuple:
    """
    Scan *folder* (non-recursively) for duplicate and near-duplicate files.

    Parameters
    ----------
    folder    : path to scan
    threshold : minimum combined_score to report a pair (default 0.75)
    w_tfidf   : weight for TF-IDF cosine component
    w_lev     : weight for Levenshtein component
    w_seq     : weight for SequenceMatcher component

    Returns
    -------
    exact_duplicates : dict[md5_hash, list[filepath]]
    near_duplicates  : list[dict]
        Each dict contains:
            file_a, file_b           — the two file paths
            score_tfidf              — TF-IDF cosine score
            score_levenshtein        — Levenshtein similarity
            score_sequence           — SequenceMatcher ratio
            combined_score           — weighted average of the three
    all_files : list[filepath]
    """
    # ── collect files ────────────────────────────────────────────────────────
    all_files = [
        os.path.join(folder, f)
        for f in os.listdir(folder)
        if os.path.isfile(os.path.join(folder, f))
    ]

    # ── exact duplicates via MD5 ─────────────────────────────────────────────
    hash_map: dict = {}
    for f in all_files:
        h = hash_file(f)
        hash_map.setdefault(h, []).append(f)

    exact_duplicates = {k: v for k, v in hash_map.items() if len(v) > 1}
    unique_files     = [v[0] for v in hash_map.values()]  # one rep per hash

    # ── extract PDF text (cached) ────────────────────────────────────────────
    text_cache: dict[str, str] = {}
    for f in unique_files:
        text_cache[f] = extract_pdf_text(f)  # returns "" for non-PDFs

    # ── build corpus IDF from all unique PDF texts ───────────────────────────
    pdf_files     = [f for f in unique_files if f.lower().endswith(".pdf")]
    token_lists   = [_tokenize(text_cache[f]) for f in pdf_files]
    corpus_idf    = build_idf(token_lists) if token_lists else {}

    # ── pairwise near-duplicate detection ────────────────────────────────────
    near_duplicates = []
    checked: set    = set()

    for i, f1 in enumerate(unique_files):
        for j, f2 in enumerate(unique_files):
            if i >= j:
                continue
            pair = (f1, f2)
            if pair in checked:
                continue
            checked.add(pair)

            t1 = text_cache.get(f1, "")
            t2 = text_cache.get(f2, "")

            scores = combined_similarity(t1, t2, corpus_idf, w_tfidf, w_lev, w_seq)

            if scores["combined_score"] >= threshold:
                near_duplicates.append({
                    "file_a": f1,
                    "file_b": f2,
                    **scores,
                })

    # Sort by combined score descending so the most similar pairs appear first
    near_duplicates.sort(key=lambda d: d["combined_score"], reverse=True)

    return exact_duplicates, near_duplicates, all_files
