from datetime import datetime, timezone
import unittest

from app.normalizer import age_days, parse_source_date


class NormalizerTest(unittest.TestCase):
    def test_parse_relative_day(self) -> None:
        now = datetime(2026, 4, 15, 0, 0, tzinfo=timezone.utc)
        parsed = parse_source_date("3日前", now=now)
        self.assertEqual(parsed, datetime(2026, 4, 12, 0, 0, tzinfo=timezone.utc))

    def test_parse_same_year_date(self) -> None:
        now = datetime(2026, 4, 15, 0, 0, tzinfo=timezone.utc)
        parsed = parse_source_date("更新日 4/13", now=now)
        self.assertEqual(parsed, datetime(2026, 4, 13, 0, 0, tzinfo=timezone.utc))

    def test_age_days_is_never_negative(self) -> None:
        now = datetime(2026, 4, 15, 0, 0, tzinfo=timezone.utc)
        future = datetime(2026, 4, 16, 0, 0, tzinfo=timezone.utc)
        self.assertEqual(age_days(future, now), 0)


if __name__ == "__main__":
    unittest.main()
