from __future__ import annotations

import unittest
from pathlib import Path

import normalize_binance_klines as normalizer


PROJECT_ROOT = Path(__file__).resolve().parent.parent
CORPUS_ROOT = PROJECT_ROOT / "materials"


class KnowledgeBaseReferenceTests(unittest.TestCase):
    def test_reference_registry_is_present_and_nonempty(self) -> None:
        registry = normalizer.load_knowledge_base_references()

        self.assertEqual(registry["corpus_root"], "materials")
        self.assertGreater(len(registry["references"]), 0)
        self.assertTrue(all(reference.get("path") for reference in registry["references"]))

    def test_repository_reference_paths_exist(self) -> None:
        registry = normalizer.load_knowledge_base_references()

        for reference in registry["references"]:
            if reference["path"].startswith(f"{registry['corpus_root']}/"):
                continue
            self.assertTrue((PROJECT_ROOT / reference["path"]).exists(), reference["path"])

    @unittest.skipUnless(
        CORPUS_ROOT.is_dir(),
        "local materials corpus is not distributed with the repository",
    )
    def test_local_corpus_reference_paths_exist(self) -> None:
        registry = normalizer.load_knowledge_base_references()

        for reference in registry["references"]:
            if not reference["path"].startswith(f"{registry['corpus_root']}/"):
                continue
            self.assertTrue((PROJECT_ROOT / reference["path"]).exists(), reference["path"])


if __name__ == "__main__":
    unittest.main()
