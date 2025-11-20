#!/usr/bin/python3

"""
rename_yaiglobal_ocr -- rename YaiGlobal OCR files to match dmaker filenames
                        in corresponding rstar book directory

For example, the YaiGlobal files for book columbia_aco001224 should be renamed:

    columbia_aco001224_000001.html --> columbia_aco001224_afr01_ocr.hocr
    columbia_aco001224_000001.txt  --> columbia_aco001224_afr01_ocr.txt
    columbia_aco001224_000002.html --> columbia_aco001224_afr02_ocr.hocr
    columbia_aco001224_000002.txt  --> columbia_aco001224_afr02_ocr.txt
    columbia_aco001224_000003.html --> columbia_aco001224_afr03_ocr.hocr
    columbia_aco001224_000003.txt  --> columbia_aco001224_afr03_ocr.txt
    columbia_aco001224_000004.html --> columbia_aco001224_afr04_ocr.hocr
    columbia_aco001224_000004.txt  --> columbia_aco001224_afr04_ocr.txt
    columbia_aco001224_000005.html --> columbia_aco001224_n000001_ocr.hocr
    columbia_aco001224_000005.txt  --> columbia_aco001224_n000001_ocr.txt
    columbia_aco001224_000006.html --> columbia_aco001224_n000002_ocr.hocr
    columbia_aco001224_000006.txt  --> columbia_aco001224_n000002_ocr.txt
    columbia_aco001224_000007.html --> columbia_aco001224_n000003_ocr.hocr
    columbia_aco001224_000007.txt  --> columbia_aco001224_n000003_ocr.txt
    columbia_aco001224_000008.html --> columbia_aco001224_n000004_ocr.hocr
    columbia_aco001224_000008.txt  --> columbia_aco001224_n000004_ocr.txt
    columbia_aco001224_000009.html --> columbia_aco001224_n000005_ocr.hocr
    columbia_aco001224_000009.txt  --> columbia_aco001224_n000005_ocr.txt
    columbia_aco001224_000010.html --> columbia_aco001224_n000006_ocr.hocr
    columbia_aco001224_000010.txt  --> columbia_aco001224_n000006_ocr.txt
    ...

"""

from pprint import pformat
from typing import Callable, List
import argparse
import grp
import logging
import os
import pwd
import re
import stat
import sys
import time

DMAKER_SUFFIX = "_d.tif"


class FileRenameError(Exception):
    """Raised when there is a problem with the file renaming logic."""

    pass


class ColorFormatter:
    """Class for colorizing text

    Attributes:
        ColorFormatter.color_codes (dict[str]):  dict of ANSI color codes

    """

    color_codes = {
        "red": "\033[31m",
        "green": "\033[32m",
        "yellow": "\033[33m",
        "blue": "\033[34m",
        "magenta": "\033[35m",
        "cyan": "\033[36m",
        "reset": "\033[0m",
    }

    def __init__(self, use_color=True):
        """
        Initializes the ColorFormatter.
        :param use_color: Boolean that determines whether to format text
            in color. This value is overwridden to False if stdout is not
            connected to a tty. (default is True)
        """
        self.use_color = use_color and sys.stdout.isatty()

    def format(self, text: str, color: str) -> str:
        """
        Return the given text in color if use_color is True.
        :param text: The text to format.
        :param color: The color to use.
        """
        return (
            f"{self.color_codes[color]}{text}{self.color_codes['reset']}"
            if self.use_color
            else text
        )


def format_permissions(path: str) -> str:
    """Return string representing permissions for file or directory
    similar to output of "ls -l" command, e.g. "-rw-r--r--"

    Args:
        path (str): path to file or directory
    """
    file_stat = os.stat(path)

    # Get the file's permissions
    permissions = stat.S_IMODE(file_stat.st_mode)
    permissions_str = "".join([
        "r" if permissions & stat.S_IRUSR else "-",
        "w" if permissions & stat.S_IWUSR else "-",
        "x" if permissions & stat.S_IXUSR else "-",
        "r" if permissions & stat.S_IRGRP else "-",
        "w" if permissions & stat.S_IWGRP else "-",
        "x" if permissions & stat.S_IXGRP else "-",
        "r" if permissions & stat.S_IROTH else "-",
        "w" if permissions & stat.S_IWOTH else "-",
        "x" if permissions & stat.S_IXOTH else "-",
    ])

    # Get the file type (regular file or directory)
    file_type = "d" if stat.S_ISDIR(file_stat.st_mode) else "-"

    # Get the file's owner and group name
    owner = pwd.getpwuid(file_stat.st_uid).pw_name
    group = grp.getgrgid(file_stat.st_gid).gr_name

    # Get the file size
    file_size = file_stat.st_size

    # Get the last modification time
    last_modified = time.strftime(
        "%b %d %Y", time.localtime(file_stat.st_mtime)
    )

    # Return the information similar to 'ls -l' output
    return (
        f"{file_type}{permissions_str} {file_stat.st_nlink} "
        f"{owner} {group} {file_size} {last_modified} {path}"
    )


def is_writable_and_executable(path: str) -> bool:
    """Checks if a file or directory is writable and executable

    Args:
        path (str): path to file or directory

    Returns:
        True if path is writable and executable, False otherwise
    """
    return os.access(path, os.W_OK | os.X_OK)


def is_dmaker(entry: os.DirEntry) -> bool:
    """Checks if entry is derivative maker image

    Args:
        enrtry (os.DirEntry): os.DirEntry object to check

    Returns:
        True if entry is a derivative maker, False otherwise
    """
    return entry.name.endswith(DMAKER_SUFFIX) and entry.is_file()


def is_nyu_format(bookid: str, ext: str) -> Callable[[os.DirEntry], bool]:
    """Return a function that checks if entry's path name conforms to
    naming format <bookid>_<afr|zbk><2-digit number>_ocr.<ext> or
    <bookid>_n<6-digit number>_ocr.<ext>

    Args:
        bookid (str): book object id
        ext (str): file extension

    Returns:
        function that takes an os.DirEntry as input and returns a bool
    """

    def _is_nyu_format(entry: os.DirEntry) -> bool:
        """Return True if entry's path is in nyu naming format and
        entry is a file, False otherwise.

        Args::
            entry (os.DirEntry): os.DirEntry object to check
        """
        return (
            re.search(
                rf"^{bookid}_((afr|zbk)\d{{2}}|n\d{{6}})_ocr\.{ext}",
                clean_text(entry.name),
            )
            is not None
            and entry.is_file()
        )

    return _is_nyu_format


def is_yai_format(bookid: str, ext: str) -> Callable[[os.DirEntry], bool]:
    """Return a function that checks if entry's path name conforms to
    naming format <book_id>_<6-digit number>.<ext>

    Args:
        bookid (str): book object id
        ext (str): file extension

    Returns:
        function that takes an os.DirEntry as input and returns a bool
    """

    def _is_yai_format(entry: os.DirEntry) -> bool:
        """Return True if entry's path is in yaiglobal naming format and
        entry is a file, False otherwise.

        Args::
            entry (os.DirEntry): os.DirEntry object to check
        """
        return (
            re.search(rf"^{bookid}_\d{{6}}\.{ext}$", clean_text(entry.name))
            is not None
            and entry.is_file()
        )

    return _is_yai_format


def clean_text(text: str) -> str:
    """Return text with byte order marker removed from start of string"""
    return text.lstrip("\ufeff")


def path_grep(
    dirpath: str, cond_func: Callable[[os.DirEntry], bool]
) -> List[str]:
    """Gets list of directory entries that match a condition

    Args:
        dirpath (str): directory path to search
        cond_func (callable): function providing condition to match

    Returns:
        list of os.DirEntry file objects that match condition
    """
    return [
        entry.path
        for entry in sorted(
            os.scandir(dirpath), key=lambda e: clean_text(e.name)
        )
        if cond_func(entry)
    ]


def validate_dirpath(dirpath: str) -> str:
    """Validates a dirpath and returns it if valid."""
    if not os.path.isdir(dirpath):
        raise argparse.ArgumentTypeError(f"Directory not found: '{dirpath}'")
    return os.path.realpath(dirpath)


def get_bookid(se_dir):
    """Return bookid from source entity data directory"""
    return os.path.basename(os.path.dirname(se_dir))


def rename_files(
    se_dir, yai_dir, dry_run=False, colorize=False, check_perms=True
):
    bookid = get_bookid(se_dir)
    logging.debug("Book ID: %s", bookid)

    renamed_files = path_grep(yai_dir, is_nyu_format(bookid, "html"))
    renamed_files.extend(path_grep(yai_dir, is_nyu_format(bookid, "txt")))
    logging.debug("Renamed files: %s", pformat(renamed_files))

    if renamed_files:
        raise FileRenameError(
            "Found following files already renamed in "
            f"{yai_dir}: {pformat(renamed_files)}"
        )

    src_txt = path_grep(yai_dir, is_yai_format(bookid, "txt"))
    logging.debug("src_txt: %s", pformat(src_txt))

    src_html = path_grep(yai_dir, is_yai_format(bookid, "html"))
    logging.debug("src_html: %s", pformat(src_html))

    len_suffix = len(DMAKER_SUFFIX)
    dmaker_imgs = path_grep(se_dir, is_dmaker)
    logging.debug("dmaker imgs: %s", pformat(dmaker_imgs))

    if not dmaker_imgs:
        raise FileRenameError(f"Couldn't find dmaker images in '{se_dir}'")

    dst_txt = []
    dst_html = []
    for img in dmaker_imgs:
        basename = (
            os.path.join(yai_dir, os.path.basename(img[:-len_suffix])) + "_ocr"
        )
        dst_txt.append(basename + ".txt")
        dst_html.append(basename + ".hocr")

    logging.debug("dst_txt: %s", pformat(dst_txt))
    logging.debug("dst_html: %s", pformat(dst_html))

    if not src_txt:
        raise FileRenameError(
            f"Can't find YaiGlobal OCR text files '{yai_dir}'"
        )

    if not src_html:
        raise FileRenameError(
            f"Can't find YaiGlobal hOCR html files '{yai_dir}'"
        )

    if len(src_txt) != len(dst_txt):
        raise FileRenameError(
            f"Number of dmaker files in the R* directory '{se_dir}'"
            f" ({len(dst_txt)}) doesn't match the number of YaiGlobal OCR "
            f" text files in '{yai_dir}' ({len(src_txt)})"
        )

    if len(src_html) != len(dst_html):
        raise FileRenameError(
            f"Number of dmaker files in the R* directory '{se_dir}'"
            f" ({len(dst_html)}) doesn't match the number of YaiGlobal hOCR"
            f" html files in '{yai_dir}' ({len(src_html)})"
        )

    if check_perms and not is_writable_and_executable(yai_dir):
        raise FileRenameError(
            "You don't have write and execute permissions on YaiGlobal"
            f" directory '{yai_dir}' perm: {format_permissions(yai_dir)}"
        )

    cf = ColorFormatter(use_color=colorize)
    src_list = src_txt + src_html
    dst_list = dst_txt + dst_html
    for src, dst in zip(src_list, dst_list):
        logging.debug(
            "Renaming '%s' to '%s'",
            cf.format(src, "red"),
            cf.format(dst, "green"),
        )
        if not dry_run:
            os.rename(src, dst)

    return dmaker_imgs, dst_html


def main():
    parser = argparse.ArgumentParser(
        description="Rename YaiGlobal OCR files to match R* source filenames"
    )
    parser.add_argument(
        "se_dir",
        type=validate_dirpath,
        help="Path to book SE directory in R* that contains dmaker files",
    )
    parser.add_argument(
        "yai_dir",
        type=validate_dirpath,
        help="YaiGlobal directory containig files to be renamed",
    )
    parser.add_argument(
        "-n",
        "--dry-run",
        action="store_true",
        help="Don't actually rename anything, implies --debug",
    )
    parser.add_argument(
        "-c",
        "--colorize",
        action="store_true",
        help="Colorize logging messages",
    )
    parser.add_argument(
        "-d", "--debug", action="store_true", help="Enable debugging"
    )
    parser.add_argument(
        "-p",
        "--no-check-permissions",
        dest="check_perms",
        action="store_false",
        help=argparse.SUPPRESS,
    )
    args = parser.parse_args()

    if args.dry_run:
        args.debug = True

    level = logging.DEBUG if args.debug else logging.WARNING
    logging.basicConfig(format="%(levelname)s: %(message)s", level=level)

    rename_files(
        args.se_dir, args.yai_dir, args.dry_run, args.colorize, args.check_perms
    )


if __name__ == "__main__":
    main()
