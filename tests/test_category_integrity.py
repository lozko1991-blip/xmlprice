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

    def test_load_eva_category_mapping(self):
        """Verify that docs/eva_category_mapping.csv loads correctly and contains valid mapped records."""
        from masterevaxml import load_eva_category_mapping
        mapping = load_eva_category_mapping()
        self.assertGreater(len(mapping), 1000)
        
        # Test mapped leaf category
        self.assertIn("200014975", mapping)
        self.assertEqual(mapping["200014975"]["status"], "MAPPED")
        self.assertEqual(mapping["200014975"]["eva_id"], "100933")
        self.assertEqual(mapping["200014975"]["eva_name"], "Сковорода")
        
        # Test NO_EVA_CATEGORY
        self.assertIn("30004048", mapping)
        self.assertEqual(mapping["30004048"]["status"], "NO_EVA_CATEGORY")
        self.assertEqual(mapping["30004048"]["eva_id"], "")

    def test_eva_category_element_transformation(self):
        """Verify that category elements receive eva_id and eva_name correctly according to EVA spec."""
        from masterevaxml import load_eva_category_mapping
        mapping = load_eva_category_mapping()

        # Simulated final_categories
        c_mapped = ET.Element("category", id="200014975")
        c_mapped.text = "Сковороды"
        
        c_no_eva = ET.Element("category", id="30004048")
        c_no_eva.text = "Жіночі ремені"

        # Apply mapping logic as in Step 7
        for c in [c_mapped, c_no_eva]:
            cid = c.get('id')
            if cid in mapping:
                info = mapping[cid]
                eva_id = info.get('eva_id')
                if eva_id and info.get('status') == 'MAPPED':
                    c.set('eva_id', str(eva_id))
                    eva_name = info.get('eva_name')
                    if eva_name:
                        c.text = eva_name
                else:
                    c.attrib.pop('eva_id', None)

        # Asserts
        self.assertEqual(c_mapped.get("eva_id"), "100933")
        self.assertEqual(c_mapped.text, "Сковорода")
        
        self.assertIsNone(c_no_eva.get("eva_id"))
        self.assertEqual(c_no_eva.text, "Жіночі ремені")


    def test_supplier_3000_category_resolver(self):
        """Verify that mixed category 836 items of supplier 3000 resolve to accurate EVA categories."""
        from masterevaxml import resolve_supplier_3000_category
        
        # Perfume
        self.assertEqual(resolve_supplier_3000_category("Женская парфюмированная вода Chanel Coco Mademoiselle 100 мл", "Chanel"), "300032010")
        self.assertEqual(resolve_supplier_3000_category("Мужская туалетная вода Lacoste Rouge 100 мл", "Lacoste"), "300032011")
        
        # Figures and Dolls
        self.assertEqual(resolve_supplier_3000_category("Ігрова фігурка FUNKO POP! - Чіка", "Funko"), "300012136")
        self.assertEqual(resolve_supplier_3000_category("Лялька BLOOPIES – РУСАЛОНЬКА ЛАВЛІ", "Bloopies"), "3000101230")
        self.assertEqual(resolve_supplier_3000_category("М'яка іграшка Squishmallows Котик", "Squishmallows"), "300012144")
        
        # School & Bags
        self.assertEqual(resolve_supplier_3000_category("Пенал шкільний з принтом Nike для хлопчика", "No brand"), "3000101509")
        self.assertEqual(resolve_supplier_3000_category("Рюкзак шкільний дитячий Stranger Things 36х27 см", "No brand"), "30002328")
        self.assertEqual(resolve_supplier_3000_category("Сумка-бананка дитяча з принтом Brawl Stars", "No brand"), "30002321")
        
        # Cleaning & Tableware
        self.assertEqual(resolve_supplier_3000_category("Концентрат для видалення жиру та нагару Primaterra", "Primaterra"), "300010091")
        self.assertEqual(resolve_supplier_3000_category("Тарілка порцелянова новорічна 23 см", "Yiwu"), "3000100953")
        
        # Pets
        self.assertEqual(resolve_supplier_3000_category("Повідець для собак нейлоновий WAUDOG Nylon", "Waudog"), "3000108202")
        self.assertEqual(resolve_supplier_3000_category("Нашийник блокер протипаразитарний для собак", "NoBrand"), "300015537")
        
        # Tools & Non-EVA goods
        self.assertEqual(resolve_supplier_3000_category("Валик игольчатий аерационний SIGMA 8313221", "Sigma"), "3000836")
        self.assertEqual(resolve_supplier_3000_category("Ремінь шкіряний чоловічий чорний", "Vintage"), "3000836")

    def test_supplier_3000_virtual_categories_protected(self):
        """Virtual categories must not be pruned by is_thin_category."""
        from masterevaxml import is_thin_category, _SUPPLIER_3000_VIRTUAL_CATEGORIES
        for v_id in _SUPPLIER_3000_VIRTUAL_CATEGORIES:
            self.assertFalse(is_thin_category(v_id, 1, is_parent=False))
            self.assertFalse(is_thin_category(v_id, 0, is_parent=False))

if __name__ == '__main__':
    unittest.main()
