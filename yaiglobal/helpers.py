import argparse
import configparser
import logging
import sys
from dataclasses import dataclass
from pathlib import Path


# -------------------------------------------------------------------
# Models
# -------------------------------------------------------------------


@dataclass
class BookPaths:
    coll: Path
    book: Path
    data: Path
    aux: Path


# -------------------------------------------------------------------
# CLI Arguments
# -------------------------------------------------------------------


def setup_args(description: str, parse: bool = True):
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "batch_id", help="Batch identifier (e.g., 0007 for batch0007)"
    )
    parser.add_argument(
        "-c",
        "--config",
        type=Path,
        default=Path.home() / ".yaiglobal_config.ini",
        help="Path to configuration file (default: %(default)s)",
    )
    parser.add_argument(
        "-r",
        "--root",
        "--dropbox-root",
        type=Path,
        help="Root path of yaiglobal dropbox",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable detailed debug output.",
    )
    return parser.parse_args() if parse else parser


# -------------------------------------------------------------------
# Configuration
# -------------------------------------------------------------------


def load_config(config_path: Path):
    """Load config file and return key paths."""
    if not config_path.exists():
        logging.error("Config file not found: %s", config_path)
        sys.exit(1)

    config = configparser.ConfigParser()
    config.read(config_path)

    try:
        root = Path(config["paths"]["root"])
        s3_bucket = config["paths"]["s3_bucket"]
    except KeyError as e:
        logging.error("Missing required config key: %s", e)
        sys.exit(1)

    output_dir = config["paths"].get("output_dir")
    if output_dir:
        output_dir = Path(output_dir)
    else:
        output_dir = None

    return root, s3_bucket, output_dir


# -------------------------------------------------------------------
# Logging setup
# -------------------------------------------------------------------


def setup_logging(verbose: bool):
    """Configure logging output and level."""
    log_format = "%(asctime)s [%(levelname)s] %(message)s"
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format=log_format,
        datefmt="%H:%M:%S",
    )


# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------


def get_book_paths(processing_subdir: Path):
    """Derive standard collection, book, data, and aux paths from a
    processing subdirectory.
    """
    partner = processing_subdir.name.split("_")[0]

    coll_dir = Path(f"/content/prod/rstar/content/{partner}/aco")
    book_dir = coll_dir / "wip" / "se" / processing_subdir.name
    data_dir = book_dir / "data"
    aux_dir = book_dir / "aux"

    return BookPaths(
        coll=coll_dir,
        book=book_dir,
        data=data_dir,
        aux=aux_dir,
    )
