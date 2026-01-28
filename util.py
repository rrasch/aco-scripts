from glob import glob
from lxml import etree
from pathlib import Path
import PIL.Image
import argparse
import concurrent.futures
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


try:
    from tqdm import tqdm

    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False


PDF_DPI = {
    "hi": 200,
    "lo": 96,
}


def sglob(pattern):
    return sorted(glob(pattern))


def run_command(
    command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, **kwargs
):
    """Run a shell command and return its output."""
    logging.debug("Running command: %s", shlex_join(command))

    try:
        result = subprocess.run(
            command,
            stdout=stdout,
            stderr=stderr,
            check=True,
            universal_newlines=True,
            **kwargs,
        )
    except subprocess.CalledProcessError as e:
        output = "\n".join(out.strip() for out in (e.stdout, e.stderr) if out)
        logging.error("%s - %s", e, output)
        sys.exit(1)
    return (result.stdout or "").strip()


def extract_images(pdf_file, dirpath, book_id):
    root_path = os.path.join(dirpath, book_id)
    run_command(["pdfimages", "-all", pdf_file, root_path])
    img_files = []
    for i, img_file in enumerate(sglob(f"{root_path}*.jpg"), start=1):
        new_img_file = os.path.join(dirpath, f"{book_id}_{i:06}.jpg")
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
        "--reverse",
        "auto",
        "--savefile",
        output_file,
        workdir,
    ])
    logging.debug("hocr-pdf output: %s", output)


def calc_scale(img_path, bbox_width, target_dpi):
    pil_logger = logging.getLogger("PIL")
    orig_level = pil_logger.level
    pil_logger.setLevel(logging.WARN)
    with PIL.Image.open(img_path) as img:
        img_width, img_height = img.size
        img_dpi = img.info["dpi"][0]
    pil_logger.setLevel(orig_level)
    logging.debug("img size: %s x %s", img_width, img_height)
    scale = (img_width / bbox_width) * (target_dpi / img_dpi)
    logging.debug("scale: %s", scale)
    return scale


def get_max_workers():
    """
    Determine a reasonable number of worker processes for parallel tasks.

    This returns the number of CPU cores minus one, with a minimum of one
    worker. This helps avoid saturating the system while still providing
    parallelism.

    Returns:
        int: Recommended number of worker processes.
    """
    max_workers = max((os.cpu_count() or 1) - 1, 1)
    return max_workers


def process_page(src_img, src_hocr, page_num, workdir, magick, dpi):
    """Process a single page: resample image and symlink hOCR."""
    dst_img, dst_hocr = (
        Path(workdir) / f"{page_num}.{ext}" for ext in ("jpg", "hocr")
    )

    output = run_command([
        magick,
        str(src_img) + "[0]",
        "-resample",
        str(dpi),
        "-strip",
        str(dst_img),
    ])
    logging.debug("ImageMagick output: %s", output)

    logging.debug("Creating symlink: %s -> %s", src_hocr, dst_hocr)
    os.symlink(src_hocr, dst_hocr)

    return src_hocr, dst_hocr


def generate_pdf(
    img_files,
    hocr_files,
    output_file,
    dpi=200,
    max_workers=None,
    use_processes=False,
    overwrite=False,
):
    """
    Generate a searchable PDF from image and hOCR files using hocr-pdf.

    This function combines a set of image files and their corresponding hOCR
    files into a single searchable PDF. Each image is resampled to a target
    DPI, stripped of metadata, and paired with its matching hOCR file before
    being passed to `hocr-pdf` for conversion.

    All intermediate files are created in a temporary working directory to
    ensure isolation and automatic cleanup after processing. The resulting
    PDF is linearized and stripped of all metadata.

    Args:
        img_files (list[str] or list[pathlib.Path]): Paths to image files
            (one per page).
        hocr_files (list[str] or list[pathlib.Path]): Paths to hOCR files
            corresponding to each image.
        output_file (str or pathlib.Path): Path where the final searchable
            PDF will be written.
        dpi (int, optional): Target DPI for image resampling. Defaults to 200.
        max_workers (int, optional): Maximum number of threads or
            processes to use. Defaults to CPU count - 1.
        use_processes (bool, optional): If True, uses
            ProcessPoolExecutor for CPU-heavy Python work; otherwise
            ThreadPoolExecutor (default, ideal for subprocess-heavy
            tasks).
        overwrite (bool, optional): Allow output_file to be overwritten if True.
            Defaukts to False.

    Raises:
        ValueError: If the number of image and hOCR files differ, or if no
            image or hOCR files found.
        RuntimeError: If any external command (ImageMagick, hocr-pdf, exiftool,
            or qpdf) fails during execution.

    Returns:
        None
    """
    output_path = Path(output_file).resolve()
    if not overwrite and output_path.is_file():
        logging.warning("File %s already exists", output_path)
        return

    if len(img_files) != len(hocr_files):
        raise ValueError(
            f"Number of image files {len(img_files)} != "
            f"Number of hOCR files {len(hocr_files)}"
        )

    if not img_files:
        raise ValueError("No image or hocr files")

    magick = get_magick_cmd()

    if max_workers is None:
        max_workers = get_max_workers()

    ExecutorClass = (
        concurrent.futures.ProcessPoolExecutor
        if use_processes
        else concurrent.futures.ThreadPoolExecutor
    )

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        with ExecutorClass(max_workers=max_workers) as executor:
            futures = [
                executor.submit(
                    process_page,
                    src_img,
                    src_hocr,
                    f"{i:06}",
                    tmpdir,
                    magick,
                    dpi,
                )
                for i, (src_img, src_hocr) in enumerate(
                    zip(img_files, hocr_files), start=1
                )
            ]

            if TQDM_AVAILABLE:
                futures_iter = tqdm(
                    concurrent.futures.as_completed(futures),
                    total=len(futures),
                    desc="Processing pages",
                )
            else:
                futures_iter = concurrent.futures.as_completed(futures)

            for future in futures_iter:
                result = future.result()
                logging.debug("future result: %s", result)

        basename = output_path.stem
        tmp_file_orig = tmp_path / f"{basename}_orig.pdf"
        tmp_file_exif = tmp_path / f"{basename}_exif.pdf"
        tmp_file_qpdf = tmp_path / f"{basename}_qpdf.pdf"

        if logging.getLogger().isEnabledFor(logging.DEBUG):
            extra_args = ["--debug"]
        else:
            extra_args = []

        output = run_command(
            [
                "hocr-pdf",
                "--reverse",
                "auto",
                "--savefile",
                tmp_file_orig,
                *extra_args,
                tmpdir,
            ],
            stdout=None,
            stderr=None,
        )
        logging.debug("hocr-pdf output: %s", output)

        host = socket.gethostname()

        # Remove metadata from pdf
        output = run_command([
            "exiftool",
            "-q",
            "-all:all=",
            "-o",
            tmp_file_exif,
            tmp_file_orig,
        ])
        logging.debug("exiftool output: %s", output)

        # make exiftool changes irreversible
        output = run_command([
            "qpdf",
            "--linearize",
            tmp_file_exif,
            tmp_file_qpdf,
        ])
        logging.debug("qpdf output: %s", output)

        logging.debug("Moving %s to %s:%s", tmp_file_qpdf, host, output_path)
        shutil.move(tmp_file_qpdf, output_file)


def _generate_pdf(img_files, hocr_files, output_file, dpi=200):
    """
    Generate a searchable PDF from image and hOCR files using hocr-pdf.

    This function combines a set of image files and their corresponding hOCR
    files into a single searchable PDF. Each image is resampled to a target
    DPI, stripped of metadata, and paired with its matching hOCR file before
    being passed to `hocr-pdf` for conversion.

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

    if not img_files:
        raise ValueError("No image or hocr files")

    magick = get_magick_cmd()

    with tempfile.TemporaryDirectory() as tmpdir:
        workdir = Path(tmpdir)
        page_pdf_files = []
        for i, (src_img, src_hocr) in enumerate(
            zip(img_files, hocr_files), start=1
        ):
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

            output = run_command([
                "hocr-pdf",
                "--reverse",
                "auto",
                "--savefile",
                page_pdf,
                page_dir,
            ])
            logging.debug("hocr-pdf output: %s", output)
            page_pdf_files.append(page_pdf)

        merge_pdfs(page_pdf_files, output_file, tmpdir=tmpdir)


def generate_pdf_parallel(
    img_files,
    hocr_files,
    output_file,
    dpi=200,
    max_workers=None,
    use_processes=False,
):
    """
    Generate a searchable PDF from image and hOCR files using hocr-pdf,
    with concurrency.

    This function combines a set of image files and their corresponding
    hOCR files into a single searchable PDF. Each image is resampled to
    a target DPI, stripped of metadata, and paired with its matching
    hOCR file before being passed to `hocr-pdf` for conversion.

    All intermediate files are created in a temporary working directory
    to ensure isolation and automatic cleanup after processing. The
    resulting PDF is linearized and stripped of all metadata.

    Args:
        img_files (list[str]): Paths to image files (one per page).
        hocr_files (list[str]): Paths to hOCR files corresponding to
            each image.
        output_file (str): Path where the final searchable PDF will be
            written.
        dpi (int, optional): Target DPI for image resampling.
            Defaults to 200.
        max_workers (int, optional): Maximum number of threads or
            processes to use. Defaults to CPU count × 2 (capped at 32).
        use_processes (bool, optional): If True, uses
            ProcessPoolExecutor for CPU-heavy Python work; otherwise
            ThreadPoolExecutor (default, ideal for subprocess-heavy
            tasks).

    Raises:
        ValueError: If the number of image and hOCR files differ, or if
            no valid bounding box can be extracted from the hOCR files.
        RuntimeError: If any external command (ImageMagick, hocr-pdf,
            etc.) fails.

    Returns:
        None
    """
    output_path = Path(output_file).resolve()
    if output_path.is_file():
        logging.warning("File %s already exists", output_path)
        return

    if len(img_files) != len(hocr_files):
        raise ValueError("Number of image files != Number of hOCR files")

    if not img_files:
        raise ValueError("No image or hOCR files provided")

    magick = get_magick_cmd()

    if max_workers is None:
        max_workers = get_max_workers()

    ExecutorClass = (
        concurrent.futures.ProcessPoolExecutor
        if use_processes
        else concurrent.futures.ThreadPoolExecutor
    )

    with tempfile.TemporaryDirectory() as tmpdir:
        workdir = Path(tmpdir)
        page_pdf_files = []

        with ExecutorClass(max_workers=max_workers) as executor:
            futures = [
                executor.submit(
                    process_page,
                    src_img,
                    src_hocr,
                    f"{i:06}",
                    workdir,
                    magick,
                    dpi,
                )
                for i, (src_img, src_hocr) in enumerate(
                    zip(img_files, hocr_files), start=1
                )
            ]

            if TQDM_AVAILABLE:
                futures_iter = tqdm(
                    concurrent.futures.as_completed(futures),
                    total=len(futures),
                    desc="Processing pages",
                )
            else:
                futures_iter = concurrent.futures.as_completed(futures)

            for future in futures_iter:
                page_pdf = future.result()
                page_pdf_files.append(page_pdf)

        page_pdf_files.sort()
        merge_pdfs(page_pdf_files, output_file, tmpdir=tmpdir)


def generate_pdfs(
    img_files,
    hocr_files,
    output_base,
    max_workers=None,
    overwrite=False,
):
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
        max_workers (int, optional): Maximum number of threads to use.
            Defaults to CPU count - 1.
        overwrite (bool, optional): Allow output files to be overwritten
            if True. Defaults to False.

    Returns:
        list[pathlib.Path]: Paths to the generated PDF files.

    Raises:
        RuntimeError: If PDF generation fails for any DPI setting.
    """
    pdfs = []
    for ext, dpi in PDF_DPI.items():
        outfile = Path(f"{output_base}_{ext}.pdf")
        logging.debug("Generating PDF: %s (DPI=%s)", outfile, dpi)
        generate_pdf(
            img_files,
            hocr_files,
            outfile,
            dpi=dpi,
            max_workers=max_workers,
            overwrite=overwrite,
        )
        pdfs.append(outfile)
    return pdfs


def merge_pdfs(input_files, output_file, tmpdir=None, keep_sources=True):
    """
    Merge multiple PDF files into a single output PDF.

    The function automatically detects and uses the first available merge tool
    in this order of preference:
        1. qpdf
        2. pdftk
        3. Apache PDFBox (via Java)

    If none of these tools are available, a RuntimeError is raised.

    A temporary file is created in a working directory during the merge. If
    `tmpdir` is not provided, a temporary directory is automatically created and
    cleaned up when the function exits. The merged PDF is moved to `output_file`
    after successful completion.

    Args:
        input_files (list[str] | list[pathlib.Path]):
            A list of input PDF file paths to merge, in order.
        output_file (str | pathlib.Path):
            Path to the final merged PDF file.
        tmpdir (str | pathlib.Path | None, optional):
            Directory to use for temporary files. If None, a temporary directory
            is created and automatically deleted when done.
        keep_sources (bool, optional):
            If True (default), keep the original PDF files after merging.
            If False, the input files are deleted after a successful merge.

    Returns:
        str: The path to the merged output PDF.

    Raises:
        RuntimeError:
            - If no suitable PDF merge tool is found (`qpdf`, `pdftk`, or `pdfbox`).
            - If deleting files fails.
        SystemExit:
            If the underlying merge command returns a non-zero exit code.

    Notes:
        - Uses the command-line tools `qpdf`, `pdftk`, or Java’s PDFBox library.
        - The function logs which tool is used and key steps like file movement
          and cleanup using Python’s standard `logging` module.
        - The hostname of the current system is logged for traceability.
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
    """Helper for merge_pdfs().

    Detects available tool (qpdf/pdftk/pdfbox), runs merge, and cleans up.
    """
    tmp_path = Path(tmpdir)
    basename = Path(output_file).stem
    tmp_file_merged = tmp_path / f"{basename}_merged.pdf"
    tmp_file_opt = tmp_path / f"{basename}_optimized.pdf"
    host = socket.gethostname()

    qpdf = shutil.which("qpdf")
    pdftk = shutil.which("pdftk")
    java_bin = shutil.which("java")

    jar_names = ["pdfbox", "pdfbox-tools", "commons-logging"]
    pdfbox_jars = [Path(f"/usr/share/java/{name}.jar") for name in jar_names]

    if qpdf:
        logging.debug("Using qpdf for merge")
        cmd = [qpdf, "--empty", "--pages", *input_files, "--", tmp_file_merged]

    elif pdftk:
        logging.debug("Using pdftk for merge")
        cmd = [pdftk, *input_files, "cat", "output", tmp_file_merged]

    elif java_bin and all(jar.exists() for jar in pdfbox_jars):
        logging.debug("Using PDFBox for merge")
        classpath = ":".join(str(jar) for jar in pdfbox_jars)
        cmd = [
            java_bin,
            "-Xms512m",
            "-Xmx512m",
            "-cp",
            classpath,
            "org.apache.pdfbox.tools.PDFMerger",
            *input_files,
            tmp_file_merged,
        ]

    else:
        msg = "No available PDF merge tool (qpdf, pdftk, or PDFBox)."
        logging.error(msg)
        raise RuntimeError(msg)

    output = run_command(cmd)
    logging.debug("%s output: %s", Path(cmd[0]).name, output)

    # Remove metadata from pdf
    output = run_command(["exiftool", "-q", "-all:all=", tmp_file_merged])
    logging.debug("exiftool output: %s", output)

    # make exiftool changes irreversible
    output = run_command([qpdf, "--linearize", tmp_file_merged, tmp_file_opt])
    logging.debug("qpdf output: %s", output)

    logging.debug("Moving %s to %s:%s", tmp_file_opt, host, output_file)
    shutil.move(tmp_file_opt, output_file)

    if not keep_sources:
        for file in input_files:
            try:
                os.unlink(file)
                logging.debug("Deleted intermediate file: %s", file)
            except OSError as e:
                raise RuntimeError(f"Can't unlink {file}: {e}")


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


def validate_dirpath_path(dirpath):
    """Validates a dirpath and returns it as a Path object."""
    path = Path(dirpath)
    if not path.is_dir():
        raise argparse.ArgumentTypeError(f"Directory not found: '{dirpath}'")
    return path.resolve()


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
