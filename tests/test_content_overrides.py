import unittest
import lxml.etree as ET
from content_overrides import get_product_override, get_product_clones, PRODUCT_OVERRIDES, PRODUCT_CLONES

class TestContentOverridesAndClones(unittest.TestCase):
    def test_override_by_article(self):
        override = get_product_override(offer_id="2222_106619", raw_id="106619", article="sp10144")
        self.assertIsNotNone(override)
        self.assertIn("name_ua", override)
        self.assertEqual(
            override["name_ua"],
            "Магнітний павербанк 10000mAh MagSafe / Power Bank lightning / Повербанк магнітний бездротовий"
        )

    def test_override_case_insensitive_article(self):
        override = get_product_override(offer_id="2222_106619", raw_id="106619", article="SP10144")
        self.assertIsNotNone(override)
        self.assertEqual(
            override["name_ua"],
            "Магнітний павербанк 10000mAh MagSafe / Power Bank lightning / Повербанк магнітний бездротовий"
        )

    def test_get_product_clones(self):
        clones = get_product_clones(offer_id="2222_106619", raw_id="106619", article="sp10144")
        self.assertIsInstance(clones, list)
        self.assertEqual(len(clones), 1)
        clone = clones[0]
        self.assertEqual(clone["id_suffix"], "5000")
        self.assertEqual(clone["article"], "sp10144-5000")
        self.assertEqual(
            clone["name_ua"],
            "Магнітний павербанк 5000mAh MagSafe / Power Bank lightning / Повербанк магнітний бездротовий"
        )

    def test_clone_case_insensitive(self):
        clones = get_product_clones(offer_id="2222_106619", raw_id="106619", article="SP10144")
        self.assertEqual(len(clones), 1)
        self.assertEqual(clones[0]["id_suffix"], "5000")

    def test_unmatched_returns_none_and_empty(self):
        self.assertIsNone(get_product_override(offer_id="9999_999", raw_id="999", article="nonexistent"))
        self.assertEqual(get_product_clones(offer_id="9999_999", raw_id="999", article="nonexistent"), [])

if __name__ == '__main__':
    unittest.main()
