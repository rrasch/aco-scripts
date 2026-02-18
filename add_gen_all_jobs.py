#!/usr/bin/python3

import argparse
import logging
import subprocess
import sys

from book_paths import get_book_dirs, BookDirError

sys.path.append("/usr/local/dlib/task-queue")
import tqcommon


def main():
    parser = argparse.ArgumentParser(
        description="Submit book_publisher:gen_all jobs for book IDs."
    )
    parser.add_argument("book_ids", nargs="+", help="One or more book IDs")
    parser.add_argument(
        "--dry-run", action="store_true", help="Show commands without executing"
    )

    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    sysconfig = tqcommon.get_sysconfig()
    if "mqhost" not in sysconfig:
        sys.exit("RabbitMQ host not set in sysconfig.")

    try:
        meta = get_book_dirs(args.book_ids)
    except BookDirError as e:
        logging.error(e)
        return

    for book_id, info in meta.items():
        rstar_dir = info["rstar_dir"]

        cmd = [
            "add-mb-job",
            "-m",
            sysconfig["mqhost"],
            "-s",
            "book_publisher:gen_all",
            "-r",
            str(rstar_dir),
            "-e",
            "-f",
            book_id,
        ]

        logging.info(f"Command: {' '.join(cmd)}")

        if not args.dry_run:
            try:
                subprocess.run(cmd, check=True)
            except subprocess.CalledProcessError as e:
                logging.error(f"Command failed for {book_id}: {e}")


if __name__ == "__main__":
    main()
