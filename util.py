from glob import glob
from lxml import etree
from pathlib import Path
import PIL.Image
import argparse
import logging
import os
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import zipfile

PDF_DPI = {
    "hi": 200,
    "lo": 96,
}


def sglob(pattern):
    return sorted(glob(pattern))


def run_command(command, stderr=subprocess.STDOUT, **kwargs):
    """Run a shell command and return its output."""
    logging.debug("Running command: %s", shlex_join(command))

    try:
        result = subprocess.run(
            command,
            stdout=subprocess.PIPE,
            stderr=stderr,
            check=True,
            universal_newlines=True,
            **kwargs,
        )
    except subprocess.CalledProcessError as e:
        output = "\n".join(out.strip() for out in (e.stdout, e.stderr) if out)
        logging.error("%s - %s", e, output)
        sys.exit(1)
    return result.stdout.strip()


def extract_images(pdf_file, dirpath, book_id):
    root_path = os.path.join(dirpath, book_id)
    run_command(["pdfimages", "-all", pdf_file, root_path])
    img_files = []
    for i, img_file in enumerate(sglob(f"{root_path}*.jpg")):
        new_img_file = os.path.join(dirpath, f"{book_id}_{i+1:06}.jpg")
        os.rename(img_file, new_img_file)
        img_files.append(new_img_file)
        with PIL.Image.open(new_img_file) as img:
            logging.debug(
                "image name: %s, dpi: %s, size: %s",
                os.path.basename(new_img_file),
                img.info.get("dpi", ("unknown",))[0],
                " x ".join(map(str, img.size)),
            )
    return img_files


def hocr2pdf(hocr_file, img_file, out_file):
    with open(hocr_file, "rb") as f:
        run_command(
            ["hocr2pdf", "-i", img_file, "-o", out_file, "-n", "-r", "400"],
            stdin=f,
            stderr=subprocess.STDOUT,
        )


def img_size(img_file):
    with PIL.Image.open(img_file) as img:
        return img.size


def get_page_bbox(hocr_path):
    """
    Extract the page-level bounding box from a single hOCR file using
    lxml.etree.

    Args:
        hocr_path (str): Path to the hOCR (.html or .hocr) file.

    Returns:
        tuple: (x1, y1, x2, y2) bounding box coordinates, or None if not
        found.
    """
    parser = etree.HTMLParser()
    tree = etree.parse(hocr_path, parser)

    # Match div elements with class="ocr_page"
    page_divs = tree.xpath('//div[@class="ocr_page"]')
    if not page_divs:
        return None

    title_attr = page_divs[0].get("title", "")
    if "bbox" not in title_attr:
        return None

    bbox_str = title_attr.split("bbox")[1].split(";")[0].strip()
    return tuple(map(int, bbox_str.split()))


def get_first_valid_bbox(hocr_files):
    """
    Iterate through a list of hOCR files and return the first valid page
    bounding box found.

    Args:
        hocr_files (list[str]): List of file paths to .hocr or .html
        files.

    Returns:
        dict: A dictionary with keys:
            'bbox'   (tuple): (x1, y1, x2, y2)
            'width'  (int): Page width in pixels
            'height' (int): Page height in pixels
            'index'  (int): Index of the file in the input list
            'path'   (str): Path of the file where the bbox was found

        Returns None if no valid bbox is found.
    """
    for idx, path in enumerate(hocr_files):
        bbox = get_page_bbox(path)
        if bbox:
            x1, y1, x2, y2 = bbox
            return {
                "bbox": bbox,
                "width": x2 - x1,
                "height": y2 - y1,
                "index": idx,
                "path": path,
            }
    return None


def merge_hocr(img_files, hocr_files, output_file, workdir, scale):
    for i, (img, hocr) in enumerate(zip(img_files, hocr_files)):
        root = os.path.join(workdir, f"{i:06}")
        os.symlink(img, root + ".jpg")
        os.symlink(hocr, root + ".hocr")

    output = run_command([
        "hocr-pdf",
        "--scale-hocr",
        scale,
        "--reverse",
        "--savefile",
        output_file,
        workdir,
    ])
    logging.debug("hocr-pdf output: %s", output)


def generate_pdf(img_files, hocr_files, output_file, workdir, dpi=200):
    """
    Generate a searchable PDF from image and hOCR files using hocr-pdf.

    This function takes lists of image and corresponding hOCR files, rescales
    the images to match the text bounding boxes in the hOCR data, and uses
    ImageMagick and hocr-pdf to assemble them into a single searchable PDF.

    The function determines the scaling factor by comparing the dimensions of
    the first valid hOCR bounding box with the corresponding image. It then
    rescales and strips each image, links the matching hOCR files, and calls
    hocr-pdf to generate the output file.

    Args:
        img_files (list[str]): List of paths to image files (one per page).
        hocr_files (list[str]): List of paths to hOCR files corresponding to
            each image file.
        output_file (str): Path where the final PDF will be saved.
        workdir (str): Working directory used for intermediate files.
        dpi (int, optional): Target DPI for image resampling. Defaults to 200.

    Raises:
        RuntimeError: If external commands (ImageMagick or hocr-pdf) fail.
        ValueError: If no valid bounding box can be extracted from hOCR files.

    Returns:
        None
    """
    bbox = get_first_valid_bbox(hocr_files)
    logging.debug("bbox: %s", bbox)

    with PIL.Image.open(img_files[bbox["index"]]) as img:
        img_width, img_height = img.size
        logging.debug("img.size: %s", img.size)
        scale = (img_width / bbox["width"]) * (dpi / img.info["dpi"][0])
        logging.debug("scale: %s", scale)

    magick = get_magick_cmd()
    for i, (img, hocr) in enumerate(zip(img_files, hocr_files)):
        root = os.path.join(workdir, f"{i:06}")
        output = run_command(
            [magick, img, "-resample", str(dpi), "-strip", root + ".jpg"]
        )
        logging.debug("magick output: %s", output)
        os.symlink(hocr, root + ".hocr")

    output = run_command([
        "hocr-pdf",
        "--scale-hocr",
        f"{scale:.3f}",
        "--reverse",
        "--savefile",
        output_file,
        workdir,
    ])
    logging.debug("hocr-pdf output: %s", output)


def generate_pdfs(img_files, hocr_files, output_base):
    """
    Generate multiple PDF variants from image and hOCR files.

    This function serves as a wrapper around `generate_pdf()`. It iterates
    over the DPI settings defined in the global `PDF_DPI` mapping and
    produces a separate PDF for each setting. Each generated PDF is named
    using the `output_base` plus a variant suffix derived from the `PDF_DPI`
    key (e.g., "low", "high", "print").

    Temporary directories are used for each conversion to ensure isolation
    and automatic cleanup of intermediate files.

    Args:
        img_files (list[str] or list[pathlib.Path]): Paths to image files
            (one per page).
        hocr_files (list[str] or list[pathlib.Path]): Paths to hOCR files
            corresponding to each image file.
        output_base (str or pathlib.Path): Base path (without extension) for
            the output PDF files.

    Returns:
        list[pathlib.Path]: Paths to the generated PDF files.

    Raises:
        RuntimeError: If PDF generation fails for any DPI setting.
    """
    pdfs = []

    for ext, dpi in PDF_DPI.items():
        outfile = Path(f"{output_base}_{ext}.pdf")
        logging.debug("Generating PDF: %s (DPI=%s)", outfile, dpi)

        with tempfile.TemporaryDirectory() as tmpdir:
            generate_pdf(
                img_files,
                hocr_files,
                outfile,
                tmpdir,
                dpi=dpi,
            )
        pdfs.append(outfile)

    return pdfs


def extract_zip(zip_path, dirpath):
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(dirpath)


def get_magick_cmd():
    magick_cmd = None
    for cmd in ("magick", "convert"):
        if shutil.which(cmd):
            magick_cmd = cmd
            break
    if not magick_cmd:
        sys.exit("ImageMagick is not installed.")
    return magick_cmd


def shlex_join(split_command):
    """Return a shell-escaped string from *split_command*."""
    return " ".join(shlex.quote(str(arg)) for arg in split_command)


class PDFInfo:
    def __init__(self, pdf_file):
        self.path = pdf_file
        self.info = PDFInfo.pdf_info(pdf_file)

    @staticmethod
    def pdf_info(pdf_file):
        info = {}
        output = run_command(["pdfinfo", pdf_file], stderr=subprocess.PIPE)
        for line in output.splitlines():
            key, val = line.split(":", maxsplit=1)
            info[key] = val.strip()
        info["Page size"] = PDFInfo.parse_size(info["Page size"])
        return info

    @staticmethod
    def parse_size(size_str):
        match = re.search(r"^(\d+(?:\.\d+)?) x (\d+(?:\.\d+)?) pts", size_str)
        if not match:
            raise ValueError(f"Can't parse page size {size_str}")
        return tuple(map(float, match.groups()))


def validate_dirpath(dirpath):
    """Validates a dirpath and returns it if valid."""
    if not os.path.isdir(dirpath):
        raise argparse.ArgumentTypeError(f"Directory not found: '{dirpath}'")
    return os.path.realpath(dirpath)


def validate_filepath(filepath):
    if not os.path.exists(filepath):
        raise argparse.ArgumentTypeError(f"File '{filepath}' does not exist.")
    return os.path.realpath(filepath)


def is_pos_int(val):
    int_val = None
    try:
        int_val = int(val)
    except ValueError:
        pass
    if int_val is None or int_val < 1:
        raise argparse.ArgumentTypeError(f"'{val}' is not a positive integer")
    return int_val
