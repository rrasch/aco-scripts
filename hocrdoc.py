from lxml import etree


class HocrDocument:
    """
    Represents an hOCR document and provides access to page-level metadata
    such as bounding boxes, language codes, and page title.
    """

    def __init__(self, path):
        """
        Parse the hOCR file and extract key metadata.

        Args:
            path (str): Path to the hOCR (.html or .hocr) file.
        """
        self.path = path
        parser = etree.HTMLParser()
        self.tree = etree.parse(path, parser)

        # Extract metadata once
        self.bbox = self._extract_bbox()
        self.langs = self._extract_langs()  # None if no languages
        self.lang = self.langs[0] if self.langs else None
        self.title = self._extract_title()  # None if no <title>

    def _extract_bbox(self):
        """Extract the page bounding box, width, and height."""
        page_divs = self.tree.xpath('//div[@class="ocr_page"]')
        if not page_divs:
            return None

        title_attr = page_divs[0].get("title", "")
        if "bbox" not in title_attr:
            return None

        bbox_str = title_attr.split("bbox")[1].split(";")[0].strip()
        x1, y1, x2, y2 = map(int, bbox_str.split())

        return {
            "bbox": (x1, y1, x2, y2),
            "width": x2 - x1,
            "height": y2 - y1,
        }

    def _extract_langs(self):
        """Extract all declared languages; returns None if none found."""
        langs = self.tree.xpath('//meta[@name="language"]/@content')
        return langs if langs else None

    def _extract_title(self):
        """Extract the document's <title> text, if available."""
        titles = self.tree.xpath("//title/text()")
        return titles[0].strip() if titles else None

    def __repr__(self):
        """Developer-friendly string representation."""
        bbox_str = (
            f"{self.bbox['bbox']} (w={self.bbox['width']},"
            f" h={self.bbox['height']})"
            if self.bbox
            else "None"
        )
        title = f"'{self.title}'" if self.title else "None"
        return (
            f"HocrDocument(path='{self.path}', title={title}, "
            f"bbox={bbox_str}, lang='{self.lang}', langs={self.langs})"
        )

    def __str__(self):
        """Concise, human-readable description."""
        lang = self.lang or "unknown"
        title = f" – {self.title}" if self.title else ""
        if self.bbox:
            w, h = self.bbox["width"], self.bbox["height"]
            return f"{self.path}{title}: {lang} [{w}×{h}]"
        return f"{self.path}{title}: {lang} [no bbox]"


def get_first_valid_bbox(hocr_files):
    """
    Return the first valid bbox and metadata from a list of hOCR files.

    Args:
        hocr_files (list[str]): List of paths to hOCR files.

    Returns:
        dict | None: Contains bbox info, dimensions, index, path, title,
        and language data, or None if no valid bbox found.
    """
    for idx, path in enumerate(hocr_files):
        doc = HocrDocument(path)
        if doc.bbox:
            return {
                "bbox": doc.bbox["bbox"],
                "width": doc.bbox["width"],
                "height": doc.bbox["height"],
                "index": idx,
                "path": path,
                "title": doc.title,
                "lang": doc.lang,
                "langs": doc.langs,
            }
    return None
