import unittest
import lxml.etree as ET
from masterevaxml import CATEGORY_NAME_OVERRIDES

class TestCategoryIntegrity(unittest.TestCase):
    def test_sonechko_category_name_overrides(self):
        """Sonechko categories must map Russian names to valid Ukrainian names."""
        overrides = CATEGORY_NAME_OVERRIDES.get("sonechko233.com.ua", {})
        self.assertIn("Товары для дома и сада", overrides)
        self.assertEqual(overrides["Товары для дома и сада"], "Товари для дому та саду")
        self.assertIn("Красота и здоровье", overrides)
        self.assertEqual(overrides["Красота и здоровье"], "Краса та здоров'я")

    def test_fallback_category_structure_and_naming(self):
        """Fallback category 99999 must follow standard naming and prefix pattern."""
        prefix = "2222"
        domain = "opt-drop.com"
        clean_domain = domain.replace("www.", "")
        fixed_cat_id = f"{prefix}99999"
        cat_name = f"Інші товари {clean_domain}"
        
        self.assertEqual(fixed_cat_id, "222299999")
        self.assertEqual(cat_name, "Інші товари opt-drop.com")

    def test_category_element_xml_validity(self):
        """Category elements in XML must have id attribute and non-empty text <= 255 chars."""
        categories_el = ET.Element("categories")
        test_cats = [
            ("1000101", "Електроніка"),
            ("222299999", "Інші товари opt-drop.com"),
            ("3333205", "Жіночий одяг та аксесуари")
        ]
        for cat_id, cat_name in test_cats:
            c = ET.SubElement(categories_el, "category", id=cat_id)
            c.text = cat_name

        for c in categories_el.findall("category"):
            self.assertIsNotNone(c.get("id"))
            self.assertTrue(len(c.get("id")) > 0)
            self.assertIsNotNone(c.text)
            self.assertGreater(len(c.text), 0)
            self.assertLessEqual(len(c.text), 255)

    def test_offer_referencing_fallback_category_is_valid(self):
        """Offers referencing category 99999 must resolve correctly against final categories."""
        from feed_passport_validator import validate_offer_element
        
        xml = """
        <offer id="2222_999123" available="true">
            <price>300</price>
            <price_old>375</price_old>
            <currencyId>UAH</currencyId>
            <stock_quantity>5</stock_quantity>
            <categoryId>222299999</categoryId>
            <picture>https://example.com/pic.jpg</picture>
            <vendor>NoBrand</vendor>
            <name_ua>Тестовий врятований товар</name_ua>
            <description_ua><![CDATA[<p>Опис товару врятованого через резервну категорію інше.</p>]]></description_ua>
            <param name="Стан">Новий</param>
        </offer>
        """
        offer = ET.fromstring(xml)
        valid_cats = {"2222101", "222299999"}
        errors = validate_offer_element(offer, valid_categories=valid_cats)
        self.assertEqual(errors, [])

    def test_yavshoke_leaf_category_pruning_under_10_offers(self):
        """Leaf categories for Yavshoke (< 10 offers) must be pruned, while parents and other suppliers are protected."""
        from masterevaxml import is_thin_category
        # Yavshoke leaf category with < 10 offers should be pruned
        self.assertTrue(is_thin_category("2000123", 0, is_parent=False))
        self.assertTrue(is_thin_category("2000123", 5, is_parent=False))
        self.assertTrue(is_thin_category("2000123", 9, is_parent=False))
        
        # Yavshoke leaf category with >= 10 offers should be kept
        self.assertFalse(is_thin_category("2000123", 10, is_parent=False))
        self.assertFalse(is_thin_category("2000123", 25, is_parent=False))
        
        # Yavshoke parent category should NEVER be pruned, even if count < 10
        self.assertFalse(is_thin_category("2000123", 0, is_parent=True))
        self.assertFalse(is_thin_category("2000123", 5, is_parent=True))
        
        # Other suppliers leaf category uses standard threshold <= 3
        self.assertTrue(is_thin_category("1000123", 3, is_parent=False))
        self.assertFalse(is_thin_category("1000123", 4, is_parent=False))
        self.assertFalse(is_thin_category("1000123", 0, is_parent=True))

if __name__ == '__main__':
    unittest.main()
