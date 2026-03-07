#!/usr/bin/env python3

import logging
import subprocess
import sys
import yaiglobal.helpers as yh

sys.path.append("/usr/local/dlib/task-queue")
import tqcommon


def add_job(mqhost, processing_subdir, *, dry_run=False):
    book_id = processing_subdir.name
    book_paths = yh.get_book_paths(processing_subdir)

    cmd = [
        "add-mb-job",
        "-m",
        mqhost,
        "-s",
        "book_publisher:hocr2pdf",
        "-r",
        str(book_paths.coll),
        "-e",
        "-f",
        book_id,
    ]

    logging.debug("Command (%s): %s", book_id, " ".join(cmd))

    if not dry_run:
        try:
            subprocess.run(cmd, check=True)
        except subprocess.CalledProcessError as e:
            logging.error(f"Command failed for {book_id}: {e}")


def main():
    parser = yh.setup_args(
        "Submit book_publisher:hocr2pdf jobs for books in Yaiglobal batch",
        parse=False,
    )
    parser.add_argument(
        "-n",
        "--dry-run",
        action="store_true",
        help=(
            "Show add-mb-job commands that will be run without executing them."
            " (Implies verbose mode)"
        ),
    )
    args = parser.parse_args()

    yh.setup_logging(args.verbose or args.dry_run)

    sysconfig = tqcommon.get_sysconfig()
    if "mqhost" not in sysconfig:
        sys.exit("RabbitMQ host not set in sysconfig.")

    if args.root:
        root = args.root
    else:
        root, _, _ = yh.load_config(args.config)

    if not root.is_dir():
        sys.exit(f"Root directory not found: '{root}'")

    processing = root / "processing" / f"batch{args.batch_id}"
    logging.debug("Processing dir: %s", processing)

    for d in sorted(processing.iterdir()):
        if not d.is_dir():
            continue
        add_job(sysconfig["mqhost"], d, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
