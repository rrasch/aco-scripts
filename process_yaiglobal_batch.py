#!/usr/bin/env python3
"""
YaiGlobal Retrieval & Processing Flow Automation
================================================

Implements the workflow described in
"YaiGlobalRetrievalProcessingFlow.txt".

--------------------------------------------------------------------
Workflow Steps
--------------------------------------------------------------------

1. Create a batch directory that follows this template:
   /path/to/dropbox/outbox/batch<NNNN>
   where NNNN is the 4-digit zero-padded batch number.

2. Use AWS CLI to sync the batch ZIP files from:
   s3://<some-url>/outbox/batch<NNNN>/

3. Retrieve the corresponding batch CSV file:
   s3://<some-url>/batches/batch<NNNN>.csv

4. Confirm that the number of ZIP files matches the number of books in
   the batch CSV.

5. Create a batch directory under:
   /path/to/dropbox/processing/batch<NNNN>

6. Create subdirectories under 'processing' for each digitization ID.

7. Unzip each ZIP file into its respective processing directory.

8. Confirm that the number of .html and .txt files match per book.

9. Confirm that the number of .html files matches the number of dmaker
   *_d.tif files in R*.

10. Rename YaiGlobal .txt and .html files to match dmaker filenames:
      - Convert ".html" → "_ocr.hocr"
      - Convert ".txt"  → "_ocr.txt"

11. Generate searchable PDFs for each book:
      - Delete low-quality PDF received from YaiGlobal.
      - Generate high- and low-quality PDFs using OCR data and dmaker
        images.
      - Save resulting PDFs in the same processing directory.

--------------------------------------------------------------------
Configuration
--------------------------------------------------------------------
A config file (default: ./yaiglobal_config.ini) defines:

    [paths]
    root = /path/to/dropbox
    s3_bucket = s3://<some-url>

--------------------------------------------------------------------
Usage
--------------------------------------------------------------------
  ./process_yaiglobal_batch.py 0007
  ./process_yaiglobal_batch.py 0007 --verbose
  ./process_yaiglobal_batch.py 0007 --config /path/to/custom.ini
"""

from pathlib import Path
import argparse
import configparser
import csv
import hashlib
import logging
import re
import rename_yaiglobal_ocr as ryo
import shutil
import subprocess
import sys
import util
import zipfile


class ValidationError(Exception):
    pass


# -------------------------------------------------------------------
# Configuration
# -------------------------------------------------------------------
def load_config(config_path: Path):
    """Load config file and return key paths."""
    if not config_path.exists():
        logging.error("Config file not found: %s", config_path)
        sys.exit(1)

    config = configparser.ConfigParser()
    config.read(config_path)

    try:
        root = Path(config["paths"]["root"])
        s3_bucket = config["paths"]["s3_bucket"]
        output_dir = Path(config["paths"]["output_dir"])
    except KeyError as e:
        logging.error("Missing required config key: %s", e)
        sys.exit(1)

    return root, s3_bucket, output_dir


# -------------------------------------------------------------------
# Logging setup
# -------------------------------------------------------------------
def setup_logging(verbose: bool):
    """Configure logging output and level."""
    log_format = "%(asctime)s [%(levelname)s] %(message)s"
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format=log_format,
        datefmt="%H:%M:%S",
    )


# -------------------------------------------------------------------
# Utility functions
# -------------------------------------------------------------------
def run(args):
    """Run external command safely (no shell=True)."""
    logging.debug("Running: %s", " ".join(args))
    result = subprocess.run(args, capture_output=True, text=True)
    if result.returncode != 0:
        logging.error("Command failed: %s", " ".join(args))
        logging.error(result.stderr.strip())
        sys.exit(result.returncode)
    return result.stdout.strip()


def verify_tools():
    """
    Verify that required command-line tools are installed:
        - aws
        - exiftool
        - qpdf
        - ImageMagick (either 'convert' or 'magick')

    Uses logging for status messages:
        [OK]  indicates a tool was found
        [X]   indicates a tool is missing

    Raises:
        SystemExit: if one or more required tools are missing.
    """
    tools = {
        "aws": "aws",
        "exiftool": "exiftool",
        "qpdf": "qpdf",
    }

    results = {
        name: shutil.which(cmd) is not None for name, cmd in tools.items()
    }

    # Handle ImageMagick (either 'magick' or 'convert')
    imagemagick_found = shutil.which("magick") or shutil.which("convert")
    results["imagemagick"] = imagemagick_found is not None

    # Log tool status
    for tool, ok in results.items():
        mark = "[OK]" if ok else "[X]"
        if ok:
            logging.info("%s  %-12s found", mark, tool)
        else:
            logging.error("%s  %-12s missing", mark, tool)

    missing = [t for t, ok in results.items() if not ok]
    if missing:
        logging.critical("Missing required tools: %s", ", ".join(missing))
        sys.exit(1)

    logging.info("All required tools are installed.")


def create_batch_dirs(root: Path, batch_id: str):
    """Create outbox and processing directories for a batch."""
    outbox = root / "outbox" / f"batch{batch_id}"
    processing = root / "processing" / f"batch{batch_id}"
    outbox.mkdir(parents=True, exist_ok=True)
    processing.mkdir(parents=True, exist_ok=True)
    logging.debug("Created directories:\n  %s\n  %s", outbox, processing)
    return outbox, processing


def sync_s3_batch(s3_bucket: str, batch_id: str, outbox: Path):
    """Pull .zip files for this batch from S3."""
    run([
        "aws",
        "s3",
        "sync",
        f"{s3_bucket}/outbox/batch{batch_id}/",
        str(outbox),
        "--profile",
        "yaiglobal",
    ])
    logging.info("S3 sync complete for batch%s", batch_id)


def get_batch_csv(s3_bucket: str, batch_id: str, outbox: Path):
    """Download the batch CSV file from S3."""
    csv_path = outbox / f"batch{batch_id}.csv"
    run([
        "aws",
        "s3",
        "cp",
        f"{s3_bucket}/batches/batch{batch_id}.csv",
        str(csv_path),
        "--profile",
        "yaiglobal",
    ])
    logging.info("Batch CSV downloaded: %s", csv_path)
    return csv_path


def create_directory_checksum(dirpath: Path, hash_algo: str = "sha256"):
    """
    Create checksum and metadata files for all files in dirpath.

    Produces two files:

      CHECKSUMS.txt   - POSIX-compatible hash list
      CACHEINFO.txt   - Optional metadata: file size + mtime

    Args:
        dirpath (Path): Directory containing batch files.
        hash_algo (str): Hash algorithm to use (default: sha256).
    """
    checksum_file = dirpath / "CHECKSUMS.txt"
    info_file = dirpath / "CACHEINFO.txt"
    hasher_ctor = getattr(hashlib, hash_algo)

    logging.info("Creating checksum files for %s...", dirpath)

    with checksum_file.open("w", encoding="utf-8") as cksum, \
             info_file.open("w", encoding="utf-8") as info:
        for file in sorted(dirpath.glob("*")):
            if not file.is_file():
                continue
            if file.name in ("CHECKSUMS.txt", "CACHEINFO.txt"):
                continue

            hasher = hasher_ctor()
            with file.open("rb") as fh:
                for chunk in iter(lambda: fh.read(8192), b""):
                    hasher.update(chunk)

            digest = hasher.hexdigest()
            cksum.write(f"{digest}  {file.name}\n")

            stat = file.stat()
            info.write(
                f"{file.name}  size={stat.st_size}  mtime={stat.st_mtime}\n"
            )

    logging.info("✅ Wrote CHECKSUMS.txt and CACHEINFO.txt in %s", dirpath)
    return checksum_file


def verify_directory_checksum(dirpath: Path, hash_algo: str = "sha256"):
    """
    Verify integrity of dirpath using CHECKSUMS.txt and optional
    CACHEINFO.txt.

    Args:
        dirpath (Path): Directory to verify.
        hash_algo (str): Hash algorithm used at checksum creation.

    Raises:
        ValidationError: On missing files, bad checksums or size
                         mismatches.
    """
    checksum_file = dirpath / "CHECKSUMS.txt"
    info_file = dirpath / "CACHEINFO.txt"
    hasher_ctor = getattr(hashlib, hash_algo)

    if not checksum_file.exists():
        raise ValidationError(f"Checksum file missing in {dirpath}")

    mismatches = []
    sizes_expected = {}

    if info_file.exists():
        logging.debug("Using CACHEINFO.txt for size verification.")
        with info_file.open("r", encoding="utf-8") as f:
            for line in f:
                m = re.match(r"^(\S+)\s+size=(\d+)", line.strip())
                if m:
                    fname, fsize = m.groups()
                    sizes_expected[fname] = int(fsize)

    with checksum_file.open("r", encoding="utf-8") as f:
        for line in f:
            parts = line.strip().split(maxsplit=1)
            if len(parts) != 2:
                continue
            expected, filename = parts
            file_path = dirpath / filename

            if not file_path.exists():
                mismatches.append(f"Missing file: {filename}")
                continue

            hasher = hasher_ctor()
            with file_path.open("rb") as fh:
                for chunk in iter(lambda: fh.read(8192), b""):
                    hasher.update(chunk)
            actual = hasher.hexdigest()

            if actual != expected:
                mismatches.append(f"Checksum mismatch: {filename}")

            if filename in sizes_expected:
                actual_size = file_path.stat().st_size
                expected_size = sizes_expected[filename]
                if actual_size != expected_size:
                    mismatches.append(
                        f"Size mismatch: {filename} "
                        f"(expected {expected_size}, got {actual_size})"
                    )

    if mismatches:
        for msg in mismatches:
            logging.error(msg)
        raise ValidationError(
            f"Integrity check failed for {dirpath}:\n" + "\n".join(mismatches)
        )

    logging.info("✅ Directory integrity verified: %s", dirpath)


def fetch_batch_files(
    s3_bucket: str, batch_id: str, outbox: Path, cache_root: Path
):
    """
    Fetch batch files into cache, verify them, then copy to outbox and
    verify again.

    Ensures outbox contains only complete and validated data.
    """
    batch_name = f"batch{batch_id}"
    cache_dir = cache_root / batch_name
    tmp_dir = cache_root / f"{batch_name}.tmp"

    outbox.mkdir(parents=True, exist_ok=True)
    cache_root.mkdir(parents=True, exist_ok=True)

    if tmp_dir.exists():
        logging.warning("Removing stale cache temp: %s", tmp_dir)
        shutil.rmtree(tmp_dir, ignore_errors=True)

    zip_files = list(cache_dir.glob("*.zip"))
    cache_ready = len(zip_files) > 0

    if cache_ready:
        logging.info("Cache found for %s — verifying...", batch_name)
        verify_directory_checksum(cache_dir)
    else:
        logging.info("Building cache for %s from S3...", batch_name)
        tmp_dir.mkdir(parents=True, exist_ok=True)

        run([
            "aws",
            "s3",
            "sync",
            f"{s3_bucket}/outbox/{batch_name}/",
            str(tmp_dir),
            "--profile",
            "yaiglobal",
        ])

        remove_pattern(tmp_dir, "_lo")

        tmp_dir.rename(cache_dir)
        logging.info("✅ Cache built: %s", cache_dir)

        create_directory_checksum(cache_dir)

    for file in cache_dir.glob("*"):
        shutil.copy2(file, outbox)

    logging.info("Verifying outbox integrity: %s", outbox)
    verify_directory_checksum(outbox)

    logging.info("✅ Outbox ready: %s", outbox)


def confirm_zip_count(outbox: Path, csv_path: Path):
    """
    Step 4: Confirm that the number of ZIP files in the outbox matches
    the number of entries (books) listed in the batch CSV, and that
    each <bookid>.zip corresponds to a CSV entry.

    Args:
        outbox (Path): Directory containing downloaded .zip files.
        csv_path (Path): Path to the batch CSV file.

    Returns:
        list[str]: Sorted list of verified bookids (without ".zip").

    Raises:
        ValidationError: If any mismatch, encoding error, or
                         structural CSV issue is detected.
    """
    id_col = "identifier"

    # Collect ZIP files and their base names
    zip_files = sorted(outbox.glob("*.zip"))
    zip_bookids = {z.stem for z in zip_files}

    # --- Read CSV file robustly using DictReader ---
    try_encodings = ["utf-8", "utf-8-sig", "cp1252", "latin-1"]
    csv_bookids = None
    used_encoding = None

    for enc in try_encodings:
        try:
            with csv_path.open(newline="", encoding=enc) as f:
                reader = csv.DictReader(f)
                if not reader.fieldnames:
                    raise ValidationError(f"CSV missing headers in {csv_path}.")
                if id_col not in reader.fieldnames:
                    raise ValidationError(
                        f"CSV missing required '{id_col}' column in {csv_path}."
                    )

                csv_bookids = {
                    row[id_col].strip()
                    for row in reader
                    if row.get(id_col) and row[id_col].strip()
                }

            used_encoding = enc
            logging.debug("Parsed CSV using encoding: %s", enc)
            break

        except UnicodeDecodeError as e:
            logging.debug("Failed to parse %s with %s: %s", csv_path, enc, e)
            continue  # Try the next encoding

    else:
        raise ValidationError(
            f"Unable to parse {csv_path} using common encodings"
        )

    # --- Compare ZIPs vs CSV entries ---
    zip_count = len(zip_bookids)
    csv_count = len(csv_bookids)
    logging.info("Found %d ZIP files and %d CSV entries.", zip_count, csv_count)

    missing_in_outbox = csv_bookids - zip_bookids
    extra_in_outbox = zip_bookids - csv_bookids

    if missing_in_outbox or extra_in_outbox or zip_count != csv_count:
        msg_lines = []
        if missing_in_outbox:
            msg_lines.append(
                f"Missing ZIPs for {len(missing_in_outbox)} book(s):"
            )
            for bookid in sorted(missing_in_outbox):
                msg_lines.append(f"  {bookid}.zip")
        if extra_in_outbox:
            msg_lines.append(
                f"Extra ZIPs not listed in CSV ({len(extra_in_outbox)}):"
            )
            for bookid in sorted(extra_in_outbox):
                msg_lines.append(f"  {bookid}.zip")
        if zip_count != csv_count:
            msg_lines.append(
                f"Count mismatch: {zip_count} ZIPs vs {csv_count} CSV entries."
            )
        message = "\n".join(msg_lines)
        logging.error(message)
        raise ValidationError(message)

    logging.info(
        "✅ ZIP files and CSV entries match exactly (encoding: %s).",
        used_encoding,
    )
    return sorted(csv_bookids)


def unzip_to_processing(outbox: Path, processing: Path):
    """Unzip each digitization ID zip into processing directory."""
    for zipfile_path in sorted(outbox.glob("*.zip")):
        digitization_id = zipfile_path.stem
        target_dir = processing / digitization_id
        target_dir.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(zipfile_path, "r") as z:
            z.extractall(target_dir)
        remove_pattern(target_dir, "_lo")
        logging.debug("Unzipped %s → %s", zipfile_path, target_dir)
    logging.info("All zip files extracted into processing directory.")


def validate_file_counts(processing: Path):
    """
    Ensure HTML and TXT file counts match for each digitization ID.

    Raises:
        ValidationError: If any directory contains mismatched counts.
    """
    mismatches = []

    for d in sorted(processing.iterdir()):
        if d.is_dir():
            htmls = list(d.glob("*.html"))
            txts = list(d.glob("*.txt"))
            if len(htmls) != len(txts):
                msg = (
                    f"Count mismatch in {d.name}: "
                    f"{len(htmls)} html vs {len(txts)} txt"
                )
                logging.error(msg)
                mismatches.append(msg)
            else:
                logging.debug("Counts OK for %s: %d each", d.name, len(htmls))

    if mismatches:
        raise ValidationError(
            "File count validation failed:\n" + "\n".join(mismatches)
        )

    logging.info("✅ File count validation passed for %s", processing)


def rename_files(digitization_dir: Path, dmaker_files):
    """Rename YaiGlobal OCR files to match dmaker pattern."""
    htmls = sorted(digitization_dir.glob("*.html"))
    txts = sorted(digitization_dir.glob("*.txt"))
    dmaker_bases = [Path(f).stem.replace("_d", "") for f in dmaker_files]

    for old_html, old_txt, dmaker_base in zip(htmls, txts, dmaker_bases):
        new_html = digitization_dir / f"{dmaker_base}_ocr.hocr"
        new_txt = digitization_dir / f"{dmaker_base}_ocr.txt"
        old_html.rename(new_html)
        old_txt.rename(new_txt)
        logging.debug("Renamed %s → %s", old_html.name, new_html.name)
        logging.debug("Renamed %s → %s", old_txt.name, new_txt.name)

    logging.info("Renaming complete for %s", digitization_dir.name)


def generate_pdfs(digitization_dir: Path, dmaker_files):
    """Placeholder for searchable PDF generation."""
    high_pdf = digitization_dir / f"{digitization_dir.name}_high.pdf"
    low_pdf = digitization_dir / f"{digitization_dir.name}_low.pdf"
    logging.info("(Placeholder) Would generate %s and %s", high_pdf, low_pdf)


def remove_pattern(root: Path, pattern: str) -> None:
    """
    Recursively rename all files and directories under `root` by removing
    occurrences of `pattern` from their names.

    Parameters
    ----------
    root : Path
        The directory whose contents will be renamed.

    pattern : str
        The substring to remove from filenames and directory names.

    Notes
    -----
    - Walks bottom-up to safely rename directories after their content.
    - Only renames when the name actually contains the pattern.
    - Raises exceptions normally if rename fails.
    """
    if not root.is_dir():
        raise ValueError(f"{root} is not a directory")

    if not pattern:
        raise ValueError("Pattern must be a non-empty string")

    for path in sorted(
        root.rglob("*"), key=lambda p: len(p.parts), reverse=True
    ):
        name = path.name
        new_name = name.replace(pattern, "")
        if new_name != name:
            new_path = path.with_name(new_name)
            path.rename(new_path)


# -------------------------------------------------------------------
# Main pipeline
# -------------------------------------------------------------------
def process_batch(root: Path, s3_bucket: str, output_dir: Path, batch_id: str):
    """Main workflow for one YaiGlobal batch."""
    logging.info("Starting YaiGlobal batch processing: %s", batch_id)

    outbox, processing = create_batch_dirs(root, batch_id)

    cache_dir = Path.home() / ".cache" / "yaiglobal"
    fetch_batch_files(s3_bucket, batch_id, outbox, cache_dir)

    unzip_to_processing(outbox, processing)
    validate_file_counts(processing)

    for d in sorted(processing.iterdir()):
        if not d.is_dir():
            continue
        partner = d.name.split("_")[0]
        dmaker_path = Path(
            f"/content/prod/rstar/content/{partner}/aco/wip/se/{d.name}/aux"
        )
        dmaker_imgs, hocr_files = ryo.rename_files(dmaker_path, d)
        util.generate_pdfs(dmaker_imgs, hocr_files, output_dir / d.name)

    logging.info("✅ Batch %s processing complete.", batch_id)


# -------------------------------------------------------------------
# Entry point with argparse
# -------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Automate the YaiGlobal Retrieval & Processing workflow."
    )
    parser.add_argument(
        "batch_id", help="Batch identifier (e.g., 0007 for batch0007)"
    )
    parser.add_argument(
        "-c",
        "--config",
        type=Path,
        default=Path("./yaiglobal_config.ini"),
        help="Path to configuration file (default: ./yaiglobal_config.ini)",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable detailed debug output.",
    )

    args = parser.parse_args()
    setup_logging(args.verbose)

    verify_tools()

    root, s3_bucket, output_dir = load_config(args.config)
    process_batch(root, s3_bucket, output_dir, args.batch_id)


if __name__ == "__main__":
    main()
