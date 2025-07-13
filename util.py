from glob import glob
import PIL.Image
import argparse
import logging
import os
import re
import shlex
import shutil
import subprocess
import sys
import zipfile


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
    return " ".join(shlex.quote(arg) for arg in split_command)


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
