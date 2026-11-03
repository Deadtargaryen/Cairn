from __future__ import annotations

import unittest
from pathlib import Path

import normalize_binance_klines as normalizer


class KnowledgeBaseReferenceTests(unittest.TestCase):
    def test_reference_registry_is_present_and_nonempty(self) -> None:
        registry = normalizer.load_knowledge_base_references()

        self.assertEqual(registry["corpus_root"], "/Users/mac/Desktop/crypto/materials")
        self.assertGreater(len(registry["references"]), 0)
        self.assertTrue(all(reference.get("path") for reference in registry["references"]))

    def test_all_local_reference_paths_exist(self) -> None:
        root = Path("/Users/mac/Desktop/crypto")
        registry = normalizer.load_knowledge_base_references()

        for reference in registry["references"]:
            self.assertTrue((root / reference["path"]).exists(), reference["path"])


if __name__ == "__main__":
    unittest.main()
