#!/usr/bin/env python3

import argparse
from pathlib import Path
from process_yaiglobal_batch import create_directory_checksum


def main():
    parser = argparse.ArgumentParser(
        description="Compute checksum of a directory"
    )
    parser.add_argument("directory", type=Path, help="Path to the directory")
    args = parser.parse_args()

    directory_path = args.directory
    if not directory_path.is_dir():
        print(f"Error: {directory_path} is not a valid directory")
        return

    create_directory_checksum(directory_path)


if __name__ == "__main__":
    main()
