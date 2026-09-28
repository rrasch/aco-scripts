import re
import sys
from pathlib import Path

sys.path.append("/usr/local/dlib/task-queue")
from tqcommon import get_rstar_dir


class BookDirError(Exception):
    """Raised when required directories for a book ID do not exist."""

    __module__ = "builtins"


def parse_book_id(book_id):
    """
    Parse a book ID of the form <partner>_<collection><6 digits>.
    Returns (partner, collection) or raises ValueError if invalid.
    """
    book_id = book_id.strip()
    m = re.match(r"^([^_]+)_([A-Za-z]+)\d{6}$", book_id)
    if not m:
        raise ValueError(f"Invalid book id format: {book_id}")
    return m.group(1), m.group(2)


def get_book_dirs(book_ids):
    """
    Given a list of book IDs, return a dictionary keyed by book_id:

        {
            book_id: {
                "partner": <partner>,
                "collection": <collection>,
                "coll_dir": Path(...),
                "book_dir": Path(...),
                "data_dir": Path(...),
                "aux_dir": Path(...),
            },
            ...
        }

    Raises BookDirError if required directories are missing.
    """
    results = {}

    for book_id in book_ids:
        partner, collection = parse_book_id(book_id)

        coll_dir = Path(get_rstar_dir()) / "content" / partner / collection

        if not coll_dir.exists():
            raise BookDirError(f"Missing coll_dir: {coll_dir}")

        book_dir = coll_dir / "wip" / "se" / book_id
        if not book_dir.exists():
            raise BookDirError(f"Missing book_dir: {book_dir}")

        data_dir = book_dir / "data"
        if not data_dir.exists():
            raise BookDirError(f"Missing data_dir: {data_dir}")

        aux_dir = book_dir / "aux"
        if not aux_dir.exists():
            raise BookDirError(f"Missing aux_dir: {aux_dir}")

        results[book_id] = {
            "partner": partner,
            "collection": collection,
            "coll_dir": coll_dir,
            "book_dir": book_dir,
            "data_dir": data_dir,
            "aux_dir": aux_dir,
        }

    return results


def get_dmaker_images(data_dir):
    """
    Return a sorted list of all dmaker images in the data_dir.
    Dmaker images are files ending with '_d.tif'.
    """
    return sorted(data_dir.glob("*_d.tif"))
