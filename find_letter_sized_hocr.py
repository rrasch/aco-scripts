#!/usr/bin/env python3

import logging

import pdf_utils as util
import yaiglobal.helpers as yh

logger = logging.getLogger(__name__)


def main():
    args = yh.setup_args("Find books with letter size hocr")
    yh.setup_logging(args.verbose)

    for d in yh.get_processing_dirs(args):
        if not d.is_dir():
            continue

        hocr_files = sorted(d.rglob("*.hocr"))
        bbox = util.get_first_valid_bbox(hocr_files)
        logger.debug("bbox: %s", bbox)
        if bbox and bbox["width"] == 612 and bbox["height"] == 792:
            print(d.name)


if __name__ == "__main__":
    main()
