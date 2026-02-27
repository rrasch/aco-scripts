import re
from pathlib import Path


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
                "rstar_dir": Path(...),
                "wip_dir": Path(...),
            },
            ...
        }

    Raises BookDirError if required directories are missing.
    """
    results = {}

    for book_id in book_ids:
        partner, collection = parse_book_id(book_id)

        rstar_dir = Path(f"/content/prod/rstar/content/{partner}/{collection}")
        if not rstar_dir.exists():
            raise BookDirError(f"Missing rstar_dir: {rstar_dir}")

        wip_dir = rstar_dir / "wip" / "se" / book_id
        if not wip_dir.exists():
            raise BookDirError(f"Missing wip_dir: {wip_dir}")

        data_dir = wip_dir / "data"
        if not data_dir.exists():
            raise BookDirError(f"Missing data_dir: {data_dir}")

        aux_dir = wip_dir / "aux"
        if not aux_dir.exists():
            raise BookDirError(f"Missing aux_dir: {aux_dir}")

        results[book_id] = {
            "partner": partner,
            "collection": collection,
            "rstar_dir": rstar_dir,
            "wip_dir": wip_dir,
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
