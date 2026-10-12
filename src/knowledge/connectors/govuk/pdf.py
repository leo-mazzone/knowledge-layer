"""Local PDF text extraction and page-by-page Tesseract fallback."""

import base64
import hashlib
import logging
import math
import subprocess
import tempfile
from contextlib import closing
from pathlib import Path

logger = logging.getLogger(__name__)

DEFAULT_LIMITS = {
    "max_bytes": 50 * 1024 * 1024,
    "max_pages": 1000,
    "max_pixels": 25_000_000,
    "ocr_timeout": 120,
    "ocr_language": "eng",
}


def pdf_payload(data, metadata, limits):
    import pypdfium2 as pdfium

    pages = []
    with closing(pdfium.PdfDocument(data)) as pdf:
        if len(pdf) > limits["max_pages"]:
            raise ValueError("PDF exceeds configured page limit")
        for index in range(len(pdf)):
            with closing(pdf[index]) as page:
                with closing(page.get_textpage()) as textpage:
                    text = textpage.get_text_bounded().strip()
                method = "text"
                if sum(char.isalnum() for char in text) < 40:
                    logger.info(
                        "OCR starting page=%d/%d language=%s",
                        index + 1,
                        len(pdf),
                        limits["ocr_language"],
                    )
                    method = "ocr"
                    width, height = page.get_size()
                    scale = 300 / 72
                    if math.ceil(width * scale) * math.ceil(height * scale) > limits["max_pixels"]:
                        raise ValueError(f"PDF page {index + 1} exceeds rendering pixel limit")
                    with tempfile.TemporaryDirectory(prefix="knowledge-ocr-") as directory:
                        path = Path(directory) / "page.png"
                        with closing(page.render(scale=scale)) as bitmap:
                            image = bitmap.to_pil()
                            try:
                                image.save(path)
                            finally:
                                image.close()
                        try:
                            result = subprocess.run(
                                ["tesseract", str(path), "stdout", "-l", limits["ocr_language"]],
                                capture_output=True,
                                timeout=limits["ocr_timeout"],
                                check=True,
                            )
                        except FileNotFoundError as exc:
                            raise RuntimeError(
                                "Local OCR requires the tesseract executable"
                            ) from exc
                        except subprocess.TimeoutExpired as exc:
                            raise RuntimeError(f"OCR timed out on PDF page {index + 1}") from exc
                        except subprocess.CalledProcessError as exc:
                            raise RuntimeError(
                                f"OCR failed on PDF page {index + 1}: "
                                + exc.stderr.decode("utf-8", errors="replace")[:500]
                            ) from exc
                        text = result.stdout.decode("utf-8", errors="replace").strip() or text
                        logger.info(
                            "OCR finished page=%d/%d characters=%d", index + 1, len(pdf), len(text)
                        )
                pages.append({"number": index + 1, "text": text, "method": method})
                if (index + 1) % 25 == 0 or index + 1 == len(pdf):
                    logger.info("PDF text extraction progress pages=%d/%d", index + 1, len(pdf))
    if not any(page["text"].strip() for page in pages):
        raise ValueError("PDF has no readable text after OCR")
    return {
        "representation": "pdf",
        "attachment": metadata,
        "sha256": hashlib.sha256(data).hexdigest(),
        "bytes_base64": base64.b64encode(data).decode("ascii"),
        "pages": pages,
    }
