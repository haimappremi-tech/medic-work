#!/usr/bin/env python3
"""
PDF Splitter Script
-------------------
Takes a folder as input and creates a new folder called "separated pdfs"
(next to the input folder) containing every PDF split into single pages.

PDFs that are already one page long are copied as-is.
Multi-page PDFs are split so each page becomes its own file, named:
    <original_name>_page_01.pdf, <original_name>_page_02.pdf, ...

Usage:
    python3 split_pdf.py <folder_path>
    python3 split_pdf.py          # prompts for path interactively

Requirements:
    pip install PyMuPDF
"""

import argparse
import sys
from pathlib import Path

import fitz  # PyMuPDF


def split_folder(input_folder: Path) -> Path:
    """
    Scan input_folder for PDF files, split any multi-page PDFs into
    single-page files, and write all results to a 'separated pdfs' folder
    created next to input_folder.  Returns the output folder path.
    """
    output_folder = input_folder.parent / "separated pdfs"
    output_folder.mkdir(exist_ok=True)

    pdf_files = sorted(input_folder.glob("*.pdf"))

    if not pdf_files:
        print("No PDF files found in the input folder.")
        return output_folder

    print(f"Found {len(pdf_files)} PDF file(s). Output -> {output_folder}\n")

    total_output = 0

    for pdf_path in pdf_files:
        doc = fitz.open(str(pdf_path))
        num_pages = len(doc)

        if num_pages == 1:
            # Already a single page — copy as-is
            dest = output_folder / pdf_path.name
            # Avoid collision
            counter = 1
            while dest.exists():
                dest = output_folder / f"{pdf_path.stem}_{counter}{pdf_path.suffix}"
                counter += 1
            single = fitz.open(str(pdf_path))
            single.save(str(dest))
            single.close()
            print(f"  {pdf_path.name}  (1 page)  ->  {dest.name}")
            total_output += 1

        else:
            # Split into individual pages
            width = len(str(num_pages))
            print(f"  {pdf_path.name}  ({num_pages} pages)  ->  splitting ...")
            for i in range(num_pages):
                page_doc = fitz.open()
                page_doc.insert_pdf(doc, from_page=i, to_page=i)
                page_name = f"{pdf_path.stem}_page_{i + 1:0{width}d}.pdf"
                dest = output_folder / page_name
                # Avoid collision
                counter = 1
                base = dest
                while dest.exists():
                    dest = output_folder / f"{base.stem}_{counter}{base.suffix}"
                    counter += 1
                page_doc.save(str(dest))
                page_doc.close()
                print(f"    page {i + 1:0{width}d}  ->  {dest.name}")
                total_output += 1

        doc.close()

    print(f"\nDone. {total_output} file(s) written to: {output_folder}")
    return output_folder


def main():
    parser = argparse.ArgumentParser(
        description="Split all PDFs in a folder into single-page files."
    )
    parser.add_argument(
        "path",
        nargs="?",
        help="Path to the folder containing PDF files.",
    )
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
    split_folder(input_path)


if __name__ == "__main__":
    main()
