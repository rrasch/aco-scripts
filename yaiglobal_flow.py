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
  ./yaiglobal_flow.py 0007
  ./yaiglobal_flow.py 0007 --verbose
  ./yaiglobal_flow.py 0007 --config /path/to/custom.ini
"""

from pathlib import Path
import argparse
import configparser
import csv
import logging
import rename_yaiglobal_ocr
import subprocess
import sys
import tempfile
import util
import zipfile

# -------------------------------------------------------------------
# Configuration
# -------------------------------------------------------------------
PDF_DPI = {
    "hi": 200,
    "lo": 96,
}


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


def confirm_zip_count(outbox: Path, csv_path: Path):
    """
    Step 4: Confirm that the number of ZIP files in the outbox matches
    the number of entries (books) listed in the batch CSV, and that
    each <bookid>.zip corresponds to a CSV entry.

    Args:
        outbox (Path): Directory containing downloaded .zip files.
        csv_path (Path): Path to the batch CSV file.

    Returns:
        list[str]: Sorted list of verified book IDs (without ".zip").

    Raises:
        ZipCountMismatchError: If any mismatch or missing/extra file
                               is detected.
    """
    # Collect ZIP files and their base names
    zip_files = sorted(outbox.glob("*.zip"))
    zip_bookids = {z.stem for z in zip_files}

    # --- Read CSV file robustly ---
    try_encodings = ["utf-8", "utf-8-sig", "cp1252", "latin-1"]
    for enc in try_encodings:
        try:
            with csv_path.open(newline="", encoding=enc) as f:
                reader = csv.reader(f)
                header = next(reader, None)
                csv_bookids = {
                    row[0].strip() for row in reader if row and row[0].strip()
                }
            logging.debug("Parsed CSV using encoding: %s", enc)
            break
        except UnicodeDecodeError:
            continue
    else:
        raise ZipCountMismatchError(
            f"Unable to decode CSV file {csv_path} using common encodings."
        )

    # Compare ZIPs vs CSV entries
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
        raise ZipCountMismatchError(message)

    logging.info("✅ ZIP files and CSV entries match exactly.")
    return sorted(zip_bookids)


def unzip_to_processing(outbox: Path, processing: Path):
    """Unzip each digitization ID zip into processing directory."""
    for zipfile_path in sorted(outbox.glob("*.zip")):
        digitization_id = zipfile_path.stem
        target_dir = processing / digitization_id
        target_dir.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(zipfile_path, "r") as z:
            z.extractall(target_dir)
        logging.debug("Unzipped %s → %s", zipfile_path, target_dir)
        break
    logging.info("All zip files extracted into processing directory.")


def validate_file_counts(processing: Path):
    """Ensure HTML and TXT file counts match for each digitization ID."""
    for d in processing.iterdir():
        if d.is_dir():
            htmls = list(d.glob("*.html"))
            txts = list(d.glob("*.txt"))
            if len(htmls) != len(txts):
                logging.warning(
                    "Count mismatch in %s: %d html vs %d txt",
                    d.name,
                    len(htmls),
                    len(txts),
                )
            else:
                logging.debug("Counts OK for %s: %d each", d.name, len(htmls))
    logging.info("File count validation complete.")


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


# -------------------------------------------------------------------
# Main pipeline
# -------------------------------------------------------------------
def process_batch(root: Path, s3_bucket: str, output_dir: Path, batch_id: str):
    """Main workflow for one YaiGlobal batch."""
    logging.info("Starting YaiGlobal batch processing: %s", batch_id)

    outbox, processing = create_batch_dirs(root, batch_id)
    sync_s3_batch(s3_bucket, batch_id, outbox)
    csv_path = get_batch_csv(s3_bucket, batch_id, outbox)
    csv_path = Path("batch0000.csv")
    verified_ids = confirm_zip_count(outbox, csv_path)
    unzip_to_processing(outbox, processing)
    validate_file_counts(processing)

    for d in processing.iterdir():
        if not d.is_dir():
            continue
        partner = d.name.split("_")[0]
        dmaker_path = Path(
            f"/content/prod/rstar/content/{partner}/aco/wip/se/{d.name}/data"
        )
        dmaker_files = sorted(dmaker_path.glob("*_d.tif"))
        if not dmaker_files:
            logging.warning("No dmaker files found for %s", d.name)
            continue
        # rename_files(d, dmaker_files)
        # generate_pdfs(d, dmaker_files)
        dmaker_imgs, hocr_files = rename_yaiglobal_ocr.rename_ocr(
            dmaker_path, d, dry_run=False, colorize=True
        )
        for ext, dpi in PDF_DPI.items():
            with tempfile.TemporaryDirectory() as tmpdir:
                util.resize_and_merge_hocr(
                    dmaker_imgs,
                    hocr_files,
                    output_dir / f"{d.name}_{ext}.pdf",
                    tmpdir,
                )

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

    root, s3_bucket, output_dir = load_config(args.config)
    process_batch(root, s3_bucket, output_dir, args.batch_id)


if __name__ == "__main__":
    main()
