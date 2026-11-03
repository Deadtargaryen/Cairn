from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

import refresh_warehouse_metadata as refresher


class WarehouseMetadataRefreshTests(unittest.TestCase):
    def test_refresh_updates_registry_hashes_without_touching_rows(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            warehouse = root / "warehouse.sqlite"
            with sqlite3.connect(warehouse) as connection:
                connection.execute("CREATE TABLE klines (id INTEGER)")
                connection.execute("CREATE TABLE source_archives (id INTEGER)")
                connection.execute("CREATE TABLE warehouse_metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
                connection.execute("INSERT INTO klines VALUES (1)")
                connection.execute("INSERT INTO source_archives VALUES (1)")
                connection.commit()
            manifest = root / "manifest.json"
            manifest.write_text(json.dumps({"verification_status": "complete", "error_count": 0, "job_count": 1}))
            knowledge_base = root / "kb.json"
            knowledge_base.write_text(json.dumps({"references": [{"path": "x"}]}))
            assumptions = root / "assumptions.json"
            assumptions.write_text(json.dumps({"assumptions": [{"id": "x"}]}))

            result = refresher.refresh(warehouse, manifest, knowledge_base, assumptions)

            self.assertEqual(result["row_count"], 1)
            with sqlite3.connect(warehouse) as connection:
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM klines").fetchone()[0], 1)
                keys = {row[0] for row in connection.execute("SELECT key FROM warehouse_metadata")}
            self.assertIn("knowledge_base_sha256", keys)
            self.assertIn("research_assumptions_sha256", keys)


if __name__ == "__main__":
    unittest.main()
