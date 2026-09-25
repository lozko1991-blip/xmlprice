import unittest
from masterevaxml import normalize_sync_article

class TestArticleNormalization(unittest.TestCase):
    def test_normalize_sync_article(self):
        self.assertEqual(normalize_sync_article("58487/9"), "58487")
        self.assertEqual(normalize_sync_article("13079/00"), "13079")
        self.assertEqual(normalize_sync_article("61011/1"), "61011")
        self.assertEqual(normalize_sync_article("58487"), "58487")
        self.assertEqual(normalize_sync_article("  ABC-123/5  "), "ABC-123")
        self.assertEqual(normalize_sync_article(""), "")
        self.assertEqual(normalize_sync_article(None), "")
        self.assertEqual(normalize_sync_article(12345), "12345")

if __name__ == '__main__':
    unittest.main()
