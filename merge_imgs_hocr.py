#!/usr/bin/python3

from PIL import Image
from lxml import etree as ET
import argparse
import glob
import logging
import os
import subprocess
import sys
import tempfile
import util


def get_page_val(page):
    return int(page.get("value"))


def get_num_pages(meta_file):
    try:
        tree = ET.parse(meta_file)
        pages = tree.xpath("//OBJECT/PARAM[@name='PAGE']")
        max_page = max(pages, key=get_page_val)
        return get_page_val(max_page)
    except ET.XMLSyntaxError as e:
        logging.warning(
            "Problem parsing file '%s' - %s: %s", meta_file, type(e).__name__, e
        )
        return None


def merge_hocr(img_files, hocr_files, output_file, workdir):
    new_dpi = 200
    with Image.open(img_files[0]) as img:
        scale = new_dpi / img.info["dpi"][0]

    magick = util.get_magick_cmd()
    for i, (img, hocr) in enumerate(zip(img_files, hocr_files)):
        root = os.path.join(workdir, f"{i:06}")
        output = util.run_command(
            [magick, img, "-resample", str(new_dpi), root + ".jpg"]
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


def remove_blank_lines(text):
    return "\n".join(line for line in text.splitlines() if line.strip())


# def remove_blank_lines(text):
#     return re.sub(r'(?m)^\s*\n', '', text)


def get_all_text(root):
    return remove_blank_lines("".join(root.itertext()))


def validate_pdf(pdf_file):
    result = subprocess.run(
        ["jhove", "-m", "PDF-hul", "-h", "XML", pdf_file],
        stdout=subprocess.PIPE,
        check=True,
    )

    logging.debug("jhove xml output:\n%s", result.stdout.decode())

    root = ET.fromstring(result.stdout)
    logging.debug("jhove text output:\n%s", get_all_text(root))

    logging.debug("Namespaces: %s", root.nsmap)
    nsmap = {"j": root.nsmap[None]}
    xpath = "/j:jhove/j:repInfo/j:status"
    status = root.xpath(xpath, namespaces=nsmap)[0].text
    logging.debug("jhove status: %s", status)

    if "well-formed and valid" not in status.lower():
        sys.exit(f"PDF {pdf_file} fails JHOVE validation.")


def do_cmd(command):
    logging.debug("Running command: %s", command)
    subprocess.run(command, check=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("book_dir", type=util.validate_dirpath)
    parser.add_argument("output_file")
    parser.add_argument("-m", "--max-pages", type=util.is_pos_int)
    parser.add_argument("-s", "--skip-meta", action="store_true")
    parser.add_argument("-d", "--debug", action="store_true")
    args = parser.parse_args()

    level = logging.DEBUG if args.debug else logging.WARN
    logging.basicConfig(format="%(levelname)s: %(message)s", level=level)

    book_id = os.path.basename(args.book_dir)

    img_files = []
    hocr_files = []
    img_glob = os.path.join(args.book_dir, f"{book_id}_*", "JPG.jpg")
    for filepath in sorted(glob.glob(img_glob)):
        img_files.append(filepath)
        hocr_files.append(os.path.join(os.path.dirname(filepath), "HOCR.html"))

    if not args.skip_meta:
        meta_file = os.path.join(args.book_dir, "DJVUXML.xml")
        num_pages = get_num_pages(meta_file)
        logging.debug("Num pages: %s", num_pages)

        if not num_pages:
            sys.exit(f"Can't find number of pages for {book_id}")

        if num_pages != len(img_files):
            sys.exit(
                f"Page count [{num_pages}] in metadata file '{meta_file}' !="
                f" number of images [{len(img_files)}] in image directory"
                f" '{args.book_dir}'"
            )

    if args.max_pages:
        img_files = img_files[: args.max_pages]
        hocr_files = hocr_files[: args.max_pages]

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_pdf_file = os.path.join(tmpdir, "tmp.pdf")
        merge_hocr(img_files, hocr_files, tmp_pdf_file, tmpdir)
        do_cmd(["exiftool", "-q", "-all:all=", tmp_pdf_file])
        do_cmd(["qpdf", "--linearize", tmp_pdf_file, args.output_file])

    validate_pdf(args.output_file)


if __name__ == "__main__":
    main()
