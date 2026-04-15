from pathlib import Path
import tempfile
import unittest

from app.storage import Storage, parse_area_sqm, parse_rent_yen, parse_walk_minutes


class StorageHelpersTest(unittest.TestCase):
    def test_parse_rent_yen(self) -> None:
        self.assertEqual(parse_rent_yen("12.4万円"), 124000)

    def test_parse_area_sqm(self) -> None:
        self.assertEqual(parse_area_sqm("36.2m2"), 36.2)

    def test_parse_walk_minutes(self) -> None:
        self.assertEqual(parse_walk_minutes("浅間町駅 徒歩6分"), 6)

    def test_seeded_listings_can_be_filtered(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            storage = Storage(Path(temp_dir) / "test.sqlite3")
            try:
                storage.seed_demo_listings()
                listings = storage.get_recent_listings(max_walk_minutes=8)
            finally:
                storage.close()
        self.assertEqual(len(listings), 1)


if __name__ == "__main__":
    unittest.main()
