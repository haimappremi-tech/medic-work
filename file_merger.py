#!/usr/bin/env python3
"""
PDF Merger Script
-----------------
Takes a folder as input and merges every PDF file from the input folder
and all its sub-folders into a single file called "finishedPdf.pdf",
created next to the input folder.

Non-PDF files are skipped.

Usage:
    python3 pdf_merger.py <folder_path>
    python3 pdf_merger.py          # prompts for path interactively

Requires:
    pip install PyPDF2
"""

import argparse
import sys
from pathlib import Path
from PyPDF2 import PdfMerger


def merge_pdfs(input_folder: Path) -> Path:
    output_file = input_folder.parent / "finishedPdf.pdf"

    all_pdfs = sorted(
        p for p in input_folder.rglob("*")
        if p.is_file() and p.suffix.lower() == ".pdf"
    )

    if not all_pdfs:
        print("No PDF files found in the input folder.")
        sys.exit(1)

    print(f"Found {len(all_pdfs)} PDF(s). Merging...\n")

    merger = PdfMerger()
    for pdf in all_pdfs:
        print(f"  Adding: {pdf.relative_to(input_folder)}")
        merger.append(str(pdf))

    merger.write(str(output_file))
    merger.close()

    print(f"\nDone. Merged into: {output_file}")
    return output_file


def main():
    parser = argparse.ArgumentParser(
        description="Merge all PDFs in a folder into a single 'finishedPdf.pdf'."
    )
    parser.add_argument("path", nargs="?", help="Path to the input folder.")
    args = parser.parse_args()

    if args.path:
        input_path = Path(args.path).resolve()
    else:
        raw = input("Enter the folder path: ").strip().strip('"').strip("'")
        input_path = Path(raw).resolve()

    if not input_path.exists():
        print(f"Error: '{input_path}' does not exist.", file=sys.stderr)
        sys.exit(1)

    if not input_path.is_dir():
        print(f"Error: '{input_path}' is not a folder.", file=sys.stderr)
        sys.exit(1)

    print(f"Input folder: {input_path}\n")
    merge_pdfs(input_path)


if __name__ == "__main__":
    main()

