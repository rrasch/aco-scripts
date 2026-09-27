#!/usr/bin/env python3

import logging
import shutil
import sys
from pathlib import Path

import yaiglobal.helpers as yh

logger = logging.getLogger(__name__)


def copy_hocr_files(
    src_dir: Path, dest_dir: Path, *, dry_run: bool = False
) -> None:
    """Copy all .hocr files from src_dir to dest_dir.

    When dry_run is True, log what would be copied without making changes.
    """
    if not src_dir.is_dir():
        raise FileNotFoundError(f"Source directory does not exist: {src_dir}")

    if not dest_dir.is_dir():
        raise FileNotFoundError(
            f"Destination directory does not exist: {dest_dir}"
        )

    files = list(src_dir.glob("*.hocr"))
    if not files:
        logger.warning("No .hocr files found in %s", src_dir)
        return

    copied_count = 0
    skipped_count = 0

    for hocr_file in files:
        target = dest_dir / hocr_file.name

        if target.exists():
            skipped_count += 1
            logger.warning("[SKIP] %s already exists", target)
            continue

        copied_count += 1
        if dry_run:
            logger.info("[DRY-RUN] Would copy %s -> %s", hocr_file, target)
        else:
            logger.debug("Copying %s -> %s", hocr_file, target)
            shutil.copy2(hocr_file, target)

    action = "Would copy" if dry_run else "Copied"
    logger.info(
        "%s %d files, skipped %d files, to %s",
        action,
        copied_count,
        skipped_count,
        dest_dir,
    )


def main():
    parser = yh.setup_args(
        "Copy hocr files to aux directory for each book "
        "in yaiglobal processing directory",
        parse=False,
    )
    parser.add_argument(
        "-n",
        "--dry-run",
        action="store_true",
        help="Show what would be done without copying files.",
    )
    args = parser.parse_args()

    yh.setup_logging(args.verbose)

    if args.root:
        root = args.root
    else:
        root, _, _ = yh.load_config(args.config)

    if not root.is_dir():
        sys.exit(f"Root directory not found: '{root}'")

    processing = root / "processing" / f"batch{args.batch_id}"
    logger.debug("Processing dir: %s", processing)

    for d in sorted(processing.iterdir()):
        if not d.is_dir():
            continue
        book_paths = yh.get_book_paths(d)
        copy_hocr_files(d, book_paths.aux, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
