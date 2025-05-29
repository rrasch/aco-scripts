#!/usr/bin/python3

from PIL import Image
from lxml import etree as ET
import argparse
import logging
import os
import tempfile
import util


def get_page_val(page):
    return int(page.get("value"))


def get_num_pages(meta_file):
    tree = ET.parse(meta_file)
    pages = tree.xpath("//OBJECT/PARAM[@name='PAGE']")
    max_page = max(pages, key=get_page_val)
    return get_page_val(max_page)


def merge_hocr(img_files, hocr_files, output_file, workdir, scale):
    new_dpi = 200
    with Image.open(img_files[0]) as img:
        scale = new_dpi / img.info["dpi"][0]

    for i, (img, hocr) in enumerate(zip(img_files, hocr_files)):
        root = os.path.join(workdir, f"{i:06}")
        output = util.run_command(
            ["magick", img, "-resample", str(new_dpi), root + ".jpg"]
        )
        logging.debug("magick output: %s", output)
        os.symlink(hocr, root + ".hocr")

    output = util.run_command([
        "hocr-pdf",
        "--scale-hocr",
        f"{scale:.3f}",
        "--reverse",
        "--savefile",
        output_file,
        workdir,
    ])
    logging.debug("hocr-pdf output: %s", output)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("book_dir", type=util.validate_dirpath)
    parser.add_argument("output_file")
    parser.add_argument("-d", "--debug", action="store_true")
    args = parser.parse_args()

    level = logging.DEBUG if args.debug else logging.WARN
    logging.basicConfig(level=level)

    book_id = os.path.basename(args.book_dir)

    meta_file = os.path.join(args.book_dir, "DJVUXML.xml")
    num_pages = get_num_pages(meta_file)
    logging.debug("Num pages: %s", num_pages)

    img_files = []
    hocr_files = []

    with tempfile.TemporaryDirectory() as tmpdir:
        for i in range(1, num_pages + 1):
            basename = os.path.join(args.book_dir, f"{book_id}_n{i:06}")
            img_files.append(os.path.join(basename, "JPG.jpg"))
            hocr_files.append(os.path.join(basename, "HOCR.html"))
        merge_hocr(img_files, hocr_files, args.output_file, tmpdir, "1.0")


if __name__ == "__main__":
    main()
