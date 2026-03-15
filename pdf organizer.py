#!/usr/bin/env python3
"""
PDF Organizer Script
--------------------
Accepts either:
  - A FOLDER containing PDF files, or
  - A single LARGE PDF file whose pages contain multiple documents

For a folder:
  1. Detects and moves duplicate PDFs into a "Duplicates" sub-folder,
     keeping exactly one copy of each unique file.
  2. Reads the Hebrew date field 'קבלה' from each PDF.
  3. Renames files with a numeric prefix so the EARLIEST 'קבלה' is first
     and the LATEST 'קבלה' is last.

For a single large PDF:
  1. Splits the file into individual documents. A new document begins
     whenever a page contains a 'קבלה' field.
  2. Saves the split documents into a folder named after the input file.
  3. Applies the same deduplication and sorting steps as above.

Usage:
    python3 pdf_organizer.py <folder_or_pdf_path>
    python3 pdf_organizer.py          # prompts for path interactively

Requirements:
    pip install pdfplumber PyMuPDF
"""

import argparse
import hashlib
import re
import shutil
import sys
from pathlib import Path

import pdfplumber
import fitz  # PyMuPDF

from folder_merger_code import merge_folder


# ---------------------------------------------------------------------------
# Text extraction helpers
# ---------------------------------------------------------------------------

def extract_page_text_pdfplumber(path: Path, page_index: int) -> str:
    """Extract text from a single page (0-based index) using pdfplumber."""
    try:
        with pdfplumber.open(str(path)) as pdf:
            if page_index < len(pdf.pages):
                text = pdf.pages[page_index].extract_text()
                return text or ""
    except Exception:
        pass
    return ""


def extract_page_text_pymupdf(doc: fitz.Document, page_index: int) -> str:
    """Extract text from a single page (0-based index) using PyMuPDF."""
    try:
        return doc[page_index].get_text()
    except Exception:
        return ""


def extract_text_from_pdf_file(path: Path) -> str:
    """Extract all text from a PDF file (used for the folder-mode pipeline)."""
    try:
        with pdfplumber.open(str(path)) as pdf:
            parts = []
            for page in pdf.pages:
                t = page.extract_text()
                if t:
                    parts.append(t)
            if parts:
                return "\n".join(parts)
    except Exception:
        pass

    try:
        doc = fitz.open(str(path))
        parts = [page.get_text() for page in doc]
        doc.close()
        return "\n".join(parts)
    except Exception:
        return ""


# ---------------------------------------------------------------------------
# Date parsing helpers
# ---------------------------------------------------------------------------

def parse_date_value(date_str: str):
    """
    Parse a date string and return a comparable (YYYY, MM, DD) tuple.
    Supported: DD/MM/YYYY, DD-MM-YYYY, DD.MM.YYYY, YYYY-MM-DD, YYYY/MM/DD
    Returns None if unparseable.
    """
    if not date_str:
        return None
    date_str = date_str.strip()

    m = re.match(r"^(\d{1,2})[/.\-](\d{1,2})[/.\-](\d{4})$", date_str)
    if m:
        day, month, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
        return (year, month, day)

    m = re.match(r"^(\d{4})[/.\-](\d{1,2})[/.\-](\d{1,2})$", date_str)
    if m:
        year, month, day = int(m.group(1)), int(m.group(2)), int(m.group(3))
        return (year, month, day)

    return None


def extract_date_fields(text: str) -> dict:
    """
    Extract 'קבלה' and 'שחרור' date values from text.
    Returns dict with keys 'קבלה' and 'שחרור', values are (Y,M,D) tuples or None.
    """
    dates = {"קבלה": None, "שחרור": None}
    for field in ("קבלה", "שחרור"):
        patterns = [
            rf"{re.escape(field)}\s*[:\-]?\s*(\d{{1,2}}[/.\-]\d{{1,2}}[/.\-]\d{{4}})",
            rf"{re.escape(field)}\s*[:\-]?\s*(\d{{4}}[/.\-]\d{{1,2}}[/.\-]\d{{1,2}})",
        ]
        for pat in patterns:
            m = re.search(pat, text)
            if m:
                dates[field] = parse_date_value(m.group(1))
                break
    return dates


def extract_date_fields_from_filename(filename: str) -> dict:
    """
    Extract 'קבלה' and 'שחרור' date values from a filename.
    Handles patterns like 'קבלה 15-11-2021' or 'קבלה: 15/11/2021'.
    For filenames containing 'קבלה <date1> עד <date2>', the first date is
    treated as קבלה and the second as שחרור.
    Returns dict with keys 'קבלה' and 'שחרור', values are (Y,M,D) tuples or None.
    """
    dates = {"קבלה": None, "שחרור": None}

    # Pattern: קבלה <date1> עד <date2>  (common in Israeli medical filenames)
    m = re.search(
        r"קבלה\s*[:\.]?\s*(\d{1,2}[/.\-]\d{1,2}[/.\-]\d{4})\s+עד\s+(\d{1,2}[/.\-]\d{1,2}[/.\-]\d{4})",
        filename,
    )
    if m:
        dates["קבלה"] = parse_date_value(m.group(1))
        dates["שחרור"] = parse_date_value(m.group(2))
        return dates

    # Fallback: look for each field separately in the filename
    for field in ("קבלה", "שחרור"):
        patterns = [
            rf"{re.escape(field)}\s*[:\.]?\s*(\d{{1,2}}[/.\-]\d{{1,2}}[/.\-]\d{{4}})",
            rf"{re.escape(field)}\s*[:\.]?\s*(\d{{4}}[/.\-]\d{{1,2}}[/.\-]\d{{1,2}})",
        ]
        for pat in patterns:
            m = re.search(pat, filename)
            if m:
                dates[field] = parse_date_value(m.group(1))
                break

    return dates


def has_kabala_field(text: str) -> bool:
    """Return True if the text contains a 'קבלה' date field."""
    return bool(re.search(
        r"קבלה\s*[:\-]?\s*\d{1,4}[/.\-]\d{1,2}[/.\-]\d{1,4}", text
    ))


# ---------------------------------------------------------------------------
# Large-PDF splitting
# ---------------------------------------------------------------------------

def split_large_pdf(pdf_path: Path, output_folder: Path) -> list[Path]:
    """
    Split a single large PDF into individual documents.

    A new document begins when a page contains a 'קבלה' field.
    Pages before the first 'קבלה' are grouped together as document 1.
    Each document is saved as a separate PDF in output_folder.

    Returns a list of Paths to the saved PDF files.
    """
    output_folder.mkdir(parents=True, exist_ok=True)

    doc = fitz.open(str(pdf_path))
    total_pages = len(doc)
    print(f"  Total pages in large PDF: {total_pages}")

    # Identify page boundaries: the page indices where a new document starts
    boundaries: list[int] = []
    for i in range(total_pages):
        page_text = doc[i].get_text()
        if has_kabala_field(page_text):
            boundaries.append(i)

    if not boundaries:
        # No קבלה fields found — treat the whole file as one document
        print("  Warning: no 'קבלה' fields detected. Treating entire PDF as one document.")
        boundaries = [0]

    # Build page ranges: each boundary starts a document, ending at the next
    ranges: list[tuple[int, int]] = []
    for idx, start in enumerate(boundaries):
        end = boundaries[idx + 1] if idx + 1 < len(boundaries) else total_pages
        ranges.append((start, end))

    # If there are pages before the first boundary, prepend them to the first doc
    if boundaries[0] > 0:
        first_start, first_end = ranges[0]
        ranges[0] = (0, first_end)

    print(f"  Detected {len(ranges)} document(s) in the large PDF.\n")

    saved: list[Path] = []
    width = len(str(len(ranges)))

    for i, (start, end) in enumerate(ranges, start=1):
        out_doc = fitz.open()
        out_doc.insert_pdf(doc, from_page=start, to_page=end - 1)
        out_name = output_folder / f"doc_{i:0{width}d}.pdf"
        out_doc.save(str(out_name))
        out_doc.close()
        print(f"  Saved pages {start + 1}–{end} -> {out_name.name}")
        saved.append(out_name)

    doc.close()
    return saved


# ---------------------------------------------------------------------------
# Folder-mode pipeline
# ---------------------------------------------------------------------------

def file_hash(path: Path) -> str:
    """Return the SHA-256 hex digest of a file's binary contents."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def deduplicate_folder(folder: Path) -> list[Path]:
    """
    Detect duplicate PDFs in folder, move extras to folder/Duplicates.
    Returns a list of unique PDF files remaining in the folder.
    """
    pdf_files = sorted(folder.glob("*.pdf"))

    if not pdf_files:
        print("No PDF files found in the folder.")
        return []

    duplicates_dir = folder / "Duplicates"
    seen_hashes: dict[str, Path] = {}
    unique_files: list[Path] = []
    moved_count = 0

    for pdf in pdf_files:
        digest = file_hash(pdf)
        if digest in seen_hashes:
            duplicates_dir.mkdir(exist_ok=True)
            dest = duplicates_dir / pdf.name
            counter = 1
            while dest.exists():
                dest = duplicates_dir / f"{pdf.stem}_{counter}{pdf.suffix}"
                counter += 1
            shutil.move(str(pdf), str(dest))
            print(f"  [DUPLICATE] Moved '{pdf.name}' -> Duplicates/{dest.name}")
            moved_count += 1
        else:
            seen_hashes[digest] = pdf
            unique_files.append(pdf)

    print(f"\nDeduplication done: {len(unique_files)} unique file(s), {moved_count} duplicate(s) moved.")
    return unique_files


def sort_and_rename_by_kabala(unique_files: list[Path], folder: Path) -> None:
    """
    Extract 'קבלה' dates, sort ascending, rename with zero-padded numeric prefix.
    Files without a date go to the end.
    """
    print("\nExtracting 'קבלה' dates ...")

    file_info: list[tuple[Path, tuple | None]] = []

    for pdf in unique_files:
        # First: try extracting from PDF content
        text = extract_text_from_pdf_file(pdf)
        dates = extract_date_fields(text)
        kabala_date = dates["קבלה"]

        # Fallback: try extracting from the filename itself
        if kabala_date is None:
            dates_from_name = extract_date_fields_from_filename(pdf.name)
            kabala_date = dates_from_name["קבלה"]
            if kabala_date:
                source = "filename"
            else:
                source = None
        else:
            source = "content"

        if kabala_date:
            print(f"  {pdf.name}: קבלה = {kabala_date[2]:02d}/{kabala_date[1]:02d}/{kabala_date[0]}  (from {source})")
        else:
            print(f"  {pdf.name}: קבלה = (not found)")
        file_info.append((pdf, kabala_date))

    def sort_key(item):
        _, date = item
        return (1, (9999, 99, 99)) if date is None else (0, date)

    file_info.sort(key=sort_key)

    total = len(file_info)
    width = len(str(total))

    print("\nRenaming files ...")

    # Pass 1: rename to temp names to avoid collisions
    temp_renames: list[tuple[Path, Path]] = []
    for idx, (pdf, _) in enumerate(file_info, start=1):
        temp_name = folder / f"__tmp_{idx:0{width}d}_{pdf.name}"
        pdf.rename(temp_name)
        temp_renames.append((temp_name, pdf))

    # Pass 2: rename to final names
    for idx, (temp_path, original_pdf) in enumerate(temp_renames, start=1):
        clean_stem = re.sub(r"^\d+_", "", original_pdf.stem)
        final_name = folder / f"{idx:0{width}d}_{clean_stem}{original_pdf.suffix}"
        temp_path.rename(final_name)
        print(f"  {original_pdf.name}  ->  {final_name.name}")

    print("\nAll files renamed and sorted by 'קבלה' date.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def process_folder(folder: Path) -> None:
    """Deduplicate and sort PDFs inside a single folder (no recursion)."""
    print(f"\n{'='*60}")
    print(f"Processing folder: {folder}")
    print(f"{'='*60}")

    print("\n--- Detecting and removing duplicates ---")
    unique_files = deduplicate_folder(folder)

    if not unique_files:
        print("No files to sort in this folder.")
        return

    print("\n--- Sorting by 'קבלה' date ---")
    sort_and_rename_by_kabala(unique_files, folder)


def has_subfolders(folder: Path) -> bool:
    """Return True if folder contains at least one sub-directory."""
    return any(p.is_dir() for p in folder.iterdir())


def process(input_path: Path) -> None:
    """Run the full pipeline on a folder or a single large PDF."""

    if input_path.is_file():
        if input_path.suffix.lower() != ".pdf":
            print(f"Error: '{input_path}' is not a PDF file.", file=sys.stderr)
            sys.exit(1)

        print(f"Input is a single PDF file: {input_path}")
        output_folder = input_path.parent / input_path.stem
        if output_folder.exists():
            print(f"Output folder already exists: {output_folder}")
            print("Continuing — files already split will be re-processed.\n")
        else:
            output_folder.mkdir(parents=True)

        print(f"Output folder: {output_folder}\n")

        print("=== Step 1: Splitting large PDF into individual documents ===")
        split_files = split_large_pdf(input_path, output_folder)

        if not split_files:
            print("No documents extracted. Exiting.")
            sys.exit(0)

        folder = output_folder

    elif input_path.is_dir():
        print(f"Input is a folder: {input_path}")

        if has_subfolders(input_path):
            print("\n=== Step 1: Merging files from all sub-folders into 'merger' ===")
            folder = merge_folder(input_path)
            print(f"\nMerger folder ready: {folder}")
        else:
            print("No sub-folders found — skipping merge step.")
            folder = input_path

    else:
        print(f"Error: '{input_path}' does not exist.", file=sys.stderr)
        sys.exit(1)

    process_folder(folder)

    print(f"\n{'='*60}")
    print("Done.")
    print(f"{'='*60}")


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Organize PDF files by deduplicating and sorting by 'קבלה' date. "
            "Accepts a folder of PDFs or a single large PDF to split first."
        )
    )
    parser.add_argument(
        "path",
        nargs="?",
        help="Path to a folder containing PDFs, or a single large PDF file.",
    )
    args = parser.parse_args()

    if args.path:
        input_path = Path(args.path).resolve()
    else:
        raw = input("Enter the folder or PDF file path: ").strip().strip('"').strip("'")
        input_path = Path(raw).resolve()

    process(input_path)


if __name__ == "__main__":
    main()
