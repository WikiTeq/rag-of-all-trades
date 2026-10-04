import importlib
import re
import unittest
from pathlib import Path

REQUIREMENTS = Path(__file__).resolve().parent.parent / "requirements.txt"

# LlamaIndex SimpleDirectoryReader needs these to parse .docx and .xlsx files.
# Without them the reader raises ImportError and the ingestion job skips the file.
OFFICE_PARSER_PACKAGES = ("docx2txt", "openpyxl")


class TestOfficeParserDependencies(unittest.TestCase):
    def test_parsers_are_importable(self):
        for package in OFFICE_PARSER_PACKAGES:
            with self.subTest(package=package):
                importlib.import_module(package)

    def test_parsers_are_declared_in_requirements(self):
        # A transitive dependency can mask a removed pin, so check the file itself.
        declared = {
            re.split(r"[=<>~!\[ ]", line.strip(), maxsplit=1)[0].lower()
            for line in REQUIREMENTS.read_text().splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        }
        for package in OFFICE_PARSER_PACKAGES:
            with self.subTest(package=package):
                self.assertIn(package, declared)


if __name__ == "__main__":
    unittest.main()
