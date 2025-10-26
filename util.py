from glob import glob
from lxml import etree
from pathlib import Path
import PIL.Image
import argparse
import hocrdoc
import logging
import os
import re
import shlex
import shutil
import socket
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


def calc_scale(img_path, bbox_width, target_dpi):
    with PIL.Image.open(img_path) as img:
        img_width, img_height = img.size
        img_dpi = img.info["dpi"][0]
    logging.debug("img size: %s x %s", img_width, img_height)
    scale = (img_width / bbox_width) * (target_dpi / img_dpi)
    logging.debug("scale: %s", scale)
    return scale


def _generate_pdf(img_files, hocr_files, output_file, dpi=200):
    """
    Generate a searchable PDF from image and hOCR files using hocr-pdf.

    This function combines a set of image files and their corresponding hOCR
    files into a single searchable PDF. Each image is resampled to a target
    DPI, stripped of metadata, and paired with its matching hOCR file before
    being passed to `hocr-pdf` for conversion.

    The function determines the correct scaling factor to align hOCR text
    coordinates with the resampled image dimensions. It does this by comparing
    the bounding box dimensions from the first valid hOCR entry against the
    corresponding image size and DPI. The computed scaling factor is passed to
    `hocr-pdf` via the `--scale-hocr` option.

    All intermediate files are created in a temporary working directory to
    ensure isolation and automatic cleanup after processing. The resulting
    PDF is linearized and stripped of all metadata.

    Args:
        img_files (list[str]): Paths to image files (one per page).
        hocr_files (list[str]): Paths to hOCR files corresponding to each image.
        output_file (str): Path where the final searchable PDF will be written.
        dpi (int, optional): Target DPI for image resampling. Defaults to 200.

    Raises:
        ValueError: If the number of image and hOCR files differ, or if no valid
            bounding box can be extracted from the hOCR files.
        RuntimeError: If any external command (ImageMagick, hocr-pdf, exiftool,
            or qpdf) fails during execution.

    Returns:
        None
    """
    if os.path.isfile(output_file):
        logging.warning("File %s already exists", output_file)
        return

    if len(img_files) != len(hocr_files):
        raise ValueError(
            f"Number of image files {len(img_files)} != "
            f"Number of hOCR files {len(hocr_files)}"
        )

    bbox = get_first_valid_bbox(hocr_files)
    logging.debug("bbox: %s", bbox)

    if bbox is None:
        scale = 1
    else:
        with PIL.Image.open(img_files[bbox["index"]]) as img:
            img_width, img_height = img.size
            logging.debug("img.size: %s", img.size)
            scale = (img_width / bbox["width"]) * (dpi / img.info["dpi"][0])
            logging.debug("scale: %s", scale)

    magick = get_magick_cmd()

    with tempfile.TemporaryDirectory() as workdir:
        for i, (img, hocr) in enumerate(zip(img_files, hocr_files)):
            root = os.path.join(workdir, f"{i:06}")
            output = run_command([
                magick,
                img + "[0]",
                "-resample",
                str(dpi),
                "-strip",
                root + ".jpg",
            ])
            logging.debug("magick output: %s", output)
            os.symlink(hocr, root + ".hocr")

        tmp_pdf_file = os.path.join(workdir, "tmp.pdf")

        output = run_command([
            "hocr-pdf",
            "--scale-hocr",
            f"{scale:.3f}",
            "--reverse",
            "--savefile",
            tmp_pdf_file,
            workdir,
        ])
        logging.debug("hocr-pdf output: %s", output)

        # Remove metadata from pdf
        output = run_command(["exiftool", "-q", "-all:all=", tmp_pdf_file])
        logging.debug("exiftool output: %s", output)

        # make exiftool changes irreversible
        output = run_command(["qpdf", "--linearize", tmp_pdf_file, output_file])
        logging.debug("qpdf output: %s", output)


def generate_pdf(img_files, hocr_files, output_file, dpi=200):
    """
    Generate a searchable PDF from image and hOCR files using hocr-pdf.

    This function combines a set of image files and their corresponding hOCR
    files into a single searchable PDF. Each image is resampled to a target
    DPI, stripped of metadata, and paired with its matching hOCR file before
    being passed to `hocr-pdf` for conversion.

    The function determines the correct scaling factor to align hOCR text
    coordinates with the resampled image dimensions. It does this by comparing
    the bounding box dimensions from the first valid hOCR entry against the
    corresponding image size and DPI. The computed scaling factor is passed to
    `hocr-pdf` via the `--scale-hocr` option.

    All intermediate files are created in a temporary working directory to
    ensure isolation and automatic cleanup after processing. The resulting
    PDF is linearized and stripped of all metadata.

    Args:
        img_files (list[str]): Paths to image files (one per page).
        hocr_files (list[str]): Paths to hOCR files corresponding to each image.
        output_file (str): Path where the final searchable PDF will be written.
        dpi (int, optional): Target DPI for image resampling. Defaults to 200.

    Raises:
        ValueError: If the number of image and hOCR files differ, or if no valid
            bounding box can be extracted from the hOCR files.
        RuntimeError: If any external command (ImageMagick, hocr-pdf, exiftool,
            or qpdf) fails during execution.

    Returns:
        None
    """
    if os.path.isfile(output_file):
        logging.warning("File %s already exists", output_file)
        return

    if len(img_files) != len(hocr_files):
        raise ValueError(
            f"Number of image files {len(img_files)} != "
            f"Number of hOCR files {len(hocr_files)}"
        )

    bbox = get_first_valid_bbox(hocr_files)
    logging.debug("bbox: %s", bbox)

    if bbox is None:
        scale = 1
    else:
        with PIL.Image.open(img_files[bbox["index"]]) as img:
            img_width, img_height = img.size
            logging.debug("img.size: %s", img.size)
            scale = (img_width / bbox["width"]) * (dpi / img.info["dpi"][0])
            logging.debug("scale: %s", scale)

    magick = get_magick_cmd()

    with tempfile.TemporaryDirectory() as tmpdir:
        workdir = Path(tmpdir)
        page_pdf_files = []
        for i, (src_img, src_hocr) in enumerate(zip(img_files, hocr_files)):
            doc = hocrdoc.HocrDocument(src_hocr)
            logging.debug("hocr doc: %s", repr(doc))

            if doc.bbox:
                scale = calc_scale(src_img, doc.bbox["width"], dpi)
            else:
                scale = 1.0

            page_num = f"{i:06}"
            page_dir = workdir / page_num
            page_dir.mkdir()
            dst_img, dst_hocr, page_pdf = (
                page_dir / f"{page_num}.{ext}" for ext in ("jpg", "hocr", "pdf")
            )
            output = run_command([
                magick,
                src_img + "[0]",
                "-resample",
                str(dpi),
                "-strip",
                dst_img,
            ])
            logging.debug("magick output: %s", output)
            os.symlink(src_hocr, dst_hocr)

            if doc.lang is None or doc.lang == "ar":
                extra_args = ["--reverse"]
            else:
                extra_args = []

            output = run_command([
                "hocr-pdf",
                "--scale-hocr",
                f"{scale:.3f}",
                "--savefile",
                page_pdf,
                *extra_args,
                page_dir,
            ])
            logging.debug("hocr-pdf output: %s", output)
            page_pdf_files.append(page_pdf)

        tmp_pdf_file = workdir / "tmp.pdf"

        merge_pdfs(page_pdf_files, tmp_pdf_file, tmpdir=workdir)

        # Remove metadata from pdf
        output = run_command(["exiftool", "-q", "-all:all=", tmp_pdf_file])
        logging.debug("exiftool output: %s", output)

        # make exiftool changes irreversible
        output = run_command(["qpdf", "--linearize", tmp_pdf_file, output_file])
        logging.debug("qpdf output: %s", output)


def generate_pdfs(img_files, hocr_files, output_base):
    """
    Generate multiple PDF variants from image and hOCR files.

    This function serves as a wrapper around `generate_pdf()`. It iterates
    over the DPI settings defined in the global `PDF_DPI` mapping and
    produces a separate PDF for each setting. Each generated PDF is named
    using the `output_base` plus a variant suffix derived from the `PDF_DPI`
    key (e.g., "low", "high", "print").

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
        generate_pdf(img_files, hocr_files, outfile, dpi=dpi)
        pdfs.append(outfile)
    return pdfs


def merge_pdfs(input_files, output_file, tmpdir=None, keep_sources=True):
    """
    Merge multiple PDF files into a single output PDF.

    Uses `pdftk` if available, otherwise falls back to Apache PDFBox via Java.
    If neither is available, raises a RuntimeError. After merging, moves the
    temporary file to the final destination and optionally deletes the source PDFs.

    Args:
        input_files (list[str]): List of input PDF file paths.
        output_file (str): Path to the final merged PDF file.
        tmpdir (str | None): Temporary directory to use. If None, a temporary
            directory is created and automatically cleaned up.
        keep_sources (bool): If True, retain the input PDFs after merging (default: True).

    Returns:
        str: The path to the merged output file.

    Raises:
        RuntimeError: If no PDF merge tool is available or the merge fails.
    """
    # Use provided tmpdir or create an automatic temporary directory
    if tmpdir is None:
        with tempfile.TemporaryDirectory() as tmpdir_path:
            _do_merge(input_files, output_file, tmpdir_path, keep_sources)
    else:
        Path(tmpdir).mkdir(parents=True, exist_ok=True)
        _do_merge(input_files, output_file, tmpdir, keep_sources)

    return output_file


def _do_merge(input_files, output_file, tmpdir, keep_sources):
    """Internal helper to perform the merge logic."""
    tmp_file = Path(tmpdir) / Path(output_file).name
    host = socket.gethostname()

    pdftk = shutil.which("pdftk")
    java_bin = shutil.which("java")

    jar_names = ["pdfbox", "pdfbox-tools", "commons-logging"]
    pdfbox_jars = [Path(f"/usr/share/java/{name}.jar") for name in jar_names]

    # Verify tool availability
    if not pdftk:
        if not java_bin:
            logging.error(
                "Neither 'pdftk' nor 'java' is available on this system."
            )
            raise RuntimeError(
                "No PDF merge tool found (pdftk or Java PDFBox required)."
            )
        if not all(jar.exists() for jar in pdfbox_jars):
            missing = [jar.name for jar in pdfbox_jars if not jar.exists()]
            logging.error(
                "Missing required PDFBox jars: %s", ", ".join(missing)
            )
            raise RuntimeError(f"Missing PDFBox jars: {', '.join(missing)}")

    try:
        if pdftk:
            cmd = [pdftk, *input_files, "cat", "output", str(tmp_file)]
        else:
            classpath = ":".join(str(jar) for jar in pdfbox_jars)
            cmd = [
                java_bin,
                "-Xms512m",
                "-Xmx512m",
                "-cp",
                classpath,
                "org.apache.pdfbox.tools.PDFMerger",
                *input_files,
                str(tmp_file),
            ]

        logging.debug("Running command: %s", " ".join(cmd))
        subprocess.run(cmd, check=True)

        logging.debug("Moving %s to %s:%s", tmp_file, host, output_file)
        shutil.move(str(tmp_file), output_file)

        if not keep_sources:
            for file in input_files:
                try:
                    os.unlink(file)
                    logging.debug("Deleted intermediate file: %s", file)
                except OSError as e:
                    logging.error("Can't unlink %s: %s", file, e)
                    raise RuntimeError(f"Can't unlink {file}: {e}")
        else:
            logging.debug("Keeping source files: %s", ", ".join(input_files))

    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"PDF merge failed: {e}")
    except Exception as e:
        raise RuntimeError(f"Error during PDF merge: {e}")


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
