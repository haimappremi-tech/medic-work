#!/usr/bin/env python3
"""
Folder Merger Script
--------------------
Takes a folder as input and creates a new folder called "merger" (next to the
input folder) that contains every file from the input folder and all its
sub-folders, flattened into one place.

If two files from different sub-folders share the same name, the duplicate is
renamed by prepending its relative path segments so nothing is lost.

Usage:
    python3 pdf_merger.py <folder_path>
    python3 pdf_merger.py          # prompts for path interactively
"""

import argparse
import shutil
import sys
from pathlib import Path


def merge_folder(input_folder: Path) -> Path:
    """
    Copy every file inside input_folder (recursively) into a new 'merger'
    folder created next to input_folder.  Returns the path to the merger folder.
    """
    merger_folder = input_folder.parent / "merger"
    merger_folder.mkdir(exist_ok=True)

    # Collect all files recursively, skipping the merger folder itself
    all_files = [
        p for p in sorted(input_folder.rglob("*"))
        if p.is_file() and merger_folder not in p.parents and p != merger_folder
    ]

    if not all_files:
        print("No files found in the input folder.")
        return merger_folder

    print(f"Found {len(all_files)} file(s). Copying into: {merger_folder}\n")

    copied = 0
    renamed = 0

    for src in all_files:
        dest = merger_folder / src.name

        if dest.exists():
            # Resolve collision by prepending the relative sub-path
            rel_parts = src.relative_to(input_folder).parts[:-1]  # parent dirs only
            prefix = "_".join(rel_parts)
            dest = merger_folder / f"{prefix}_{src.name}"
            # If still colliding, add a counter
            counter = 1
            base_dest = dest
            while dest.exists():
                dest = base_dest.with_stem(f"{base_dest.stem}_{counter}")
                counter += 1
            shutil.copy2(str(src), str(dest))
            print(f"  [RENAMED] {src.relative_to(input_folder)}  ->  {dest.name}")
            renamed += 1
        else:
            shutil.copy2(str(src), str(dest))
            print(f"  {src.relative_to(input_folder)}  ->  {dest.name}")
            copied += 1

    print(f"\nDone. {copied} file(s) copied, {renamed} file(s) renamed to avoid collisions.")
    return merger_folder


def main():
    parser = argparse.ArgumentParser(
        description="Flatten a folder and all its sub-folders into a single 'merger' folder."
    )
    parser.add_argument(
        "path",
        nargs="?",
        help="Path to the input folder.",
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
    merger_folder = merge_folder(input_path)
    print(f"\nMerger folder: {merger_folder}")


if __name__ == "__main__":
    main()
