#!/usr/bin/python3

import argparse
import re
import subprocess
import sys

# Letter size in points
LETTER_WIDTH = 612
LETTER_HEIGHT = 792
TOLERANCE = 5  # small variation allowed


def is_letter_size(width, height):
    """Check if width/height matches letter dimensions with tolerance."""
    w, h = sorted([float(width), float(height)])
    return (
        abs(w - LETTER_WIDTH) <= TOLERANCE
        and abs(h - LETTER_HEIGHT) <= TOLERANCE
    )


def get_num_pages(pdf_path):
    """Return the number of pages in the PDF."""
    try:
        output = subprocess.check_output(["pdfinfo", pdf_path], text=True)
    except subprocess.CalledProcessError as e:
        print(f"Error reading {pdf_path}: {e}", file=sys.stderr)
        return 0

    match = re.search(r"Pages:\s*(\d+)", output)
    if match:
        return int(match.group(1))
    return 0


def get_page_sizes(pdf_path):
    """Return list of (width, height) for all pages using pdfinfo -f 1 -l N."""
    num_pages = get_num_pages(pdf_path)
    if num_pages == 0:
        return []

    try:
        output = subprocess.check_output(
            ["pdfinfo", "-f", "1", "-l", str(num_pages), pdf_path], text=True
        )
    except subprocess.CalledProcessError as e:
        print(f"Error reading {pdf_path}: {e}", file=sys.stderr)
        return []

    sizes = []
    # Lines like: "Page    1 size: 612 x 792 pts"
    for line in output.splitlines():
        match = re.match(r"Page\s+\d+\s+size:\s*([\d.]+)\s+x\s+([\d.]+)", line)
        if match:
            sizes.append(match.groups())
    return sizes


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Determine if PDFs are letter sized: "
            f"{LETTER_WIDTH} x {LETTER_HEIGHT} pts"
        )
    )
    parser.add_argument("pdf_file", nargs="+", help="PDF file to check")
    parser.add_argument(
        "-p",
        "--partial",
        action="store_true",
        help="Show pdfs that have some pages that are letter sized.",
    )
    args = parser.parse_args()

    for pdf in args.pdf_file:
        sizes = get_page_sizes(pdf)
        if not sizes:
            print(f"WARNING {pdf} is empty", file=sys.stderr)
            continue

        # Report PDFs where all pages are letter-sized
        if all(is_letter_size(w, h) for w, h in sizes):
            print(f"{pdf} is entirely letter sized")
        # Optional: report PDFs with at least one letter-sized page
        elif args.partial and any(is_letter_size(w, h) for w, h in sizes):
            print(f"{pdf} contains some letter-sized pages")


if __name__ == "__main__":
    main()
