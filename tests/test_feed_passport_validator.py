import unittest
import lxml.etree as ET

class TestFeedPassportValidator(unittest.TestCase):
    def setUp(self):
        # We import the validator inside the test to test its module API
        from feed_passport_validator import (
            validate_offer_element,
            validate_price_safety,
            clean_ascii_control_chars,
            has_ascii_control_chars,
            validate_feed_structure,
        )
        self.validate_offer_element = validate_offer_element
        self.validate_price_safety = validate_price_safety
        self.clean_ascii_control_chars = clean_ascii_control_chars
        self.has_ascii_control_chars = has_ascii_control_chars
        self.validate_feed_structure = validate_feed_structure

    def test_ascii_control_chars_detection_and_cleaning(self):
        clean_text = "Привіт! Це нормальний текст з табуляцією\tта новим рядком\n."
        dirty_text = "Текст з нуль-байтом\x00 та белл-символом\x07."
        
        self.assertFalse(self.has_ascii_control_chars(clean_text))
        self.assertTrue(self.has_ascii_control_chars(dirty_text))
        
        cleaned = self.clean_ascii_control_chars(dirty_text)
        self.assertFalse(self.has_ascii_control_chars(cleaned))
        self.assertEqual(cleaned, "Текст з нуль-байтом та белл-символом.")

    def test_price_safety_validation(self):
        # Valid case: our price > original, old price > our price
        errors = self.validate_price_safety(original_price_uah=100.0, our_price=150.0, old_price=187.0)
        self.assertEqual(errors, [])

        # Invalid case 1: price below cost
        errors = self.validate_price_safety(original_price_uah=200.0, our_price=180.0, old_price=220.0)
        self.assertTrue(any("ERR_PRICE_BELOW_COST" in e for e in errors))

        # Invalid case 2: old price <= our price (EVA rule violation)
        errors = self.validate_price_safety(original_price_uah=100.0, our_price=150.0, old_price=150.0)
        self.assertTrue(any("ERR_OLD_PRICE_LE_PRICE" in e for e in errors))

        # Invalid case 3: negative or zero price
        errors = self.validate_price_safety(original_price_uah=0.0, our_price=-10.0)
        self.assertTrue(any("ERR_PRICE_INVALID" in e for e in errors))

    def test_valid_offer_element_passes_validation(self):
        xml = """
        <offer id="2222_106619" available="true">
            <price>553</price>
            <price_old>691</price_old>
            <currencyId>UAH</currencyId>
            <stock_quantity>10</stock_quantity>
            <categoryId>2222101</categoryId>
            <picture>https://example.com/images/powerbank.jpg</picture>
            <vendor>Domotec</vendor>
            <article>sp10144</article>
            <name_ua>Магнітний павербанк 10000mAh MagSafe / Power Bank lightning</name_ua>
            <description_ua><![CDATA[<p>Якісний магнітний павербанк з підтримкою бездротової зарядки MagSafe для iPhone та Android пристроїв.</p>]]></description_ua>
            <param name="Стан">Новий</param>
            <param name="Колір">Чорний</param>
        </offer>
        """
        offer = ET.fromstring(xml)
        valid_cats = {"2222101", "222299999"}
        errors = self.validate_offer_element(offer, valid_categories=valid_cats)
        self.assertEqual(errors, [])

    def test_missing_mandatory_tags_fails(self):
        # Missing name_ua, vendor, description_ua, stock_quantity, currencyId
        xml = """
        <offer id="2222_999999" available="true">
            <price>500</price>
            <categoryId>101</categoryId>
            <picture>https://example.com/pic.jpg</picture>
        </offer>
        """
        offer = ET.fromstring(xml)
        errors = self.validate_offer_element(offer)
        self.assertTrue(any("ERR_NAME_UA_MISSING" in e for e in errors))
        self.assertTrue(any("ERR_VENDOR_MISSING" in e for e in errors))
        self.assertTrue(any("ERR_DESC_UA_MISSING" in e for e in errors))
        self.assertTrue(any("ERR_STOCK_QUANTITY_MISSING" in e for e in errors))
        self.assertTrue(any("ERR_CURRENCY_INVALID" in e for e in errors))

    def test_short_description_fails(self):
        xml = """
        <offer id="1" available="true">
            <price>100</price>
            <currencyId>UAH</currencyId>
            <stock_quantity>5</stock_quantity>
            <categoryId>101</categoryId>
            <picture>https://example.com/pic.jpg</picture>
            <vendor>Samsung</vendor>
            <name_ua>Навушники бездротові</name_ua>
            <description_ua>Коротко</description_ua>
            <param name="Стан">Новий</param>
        </offer>
        """
        offer = ET.fromstring(xml)
        errors = self.validate_offer_element(offer)
        self.assertTrue(any("ERR_DESC_UA_TOO_SHORT" in e for e in errors))

    def test_unresolved_category_fails(self):
        xml = """
        <offer id="1" available="true">
            <price>100</price>
            <currencyId>UAH</currencyId>
            <stock_quantity>5</stock_quantity>
            <categoryId>999888</categoryId>
            <picture>https://example.com/pic.jpg</picture>
            <vendor>Samsung</vendor>
            <name_ua>Навушники бездротові</name_ua>
            <description_ua><![CDATA[<p>Чудові бездротові навушники з відмінним звуком та мікрофоном.</p>]]></description_ua>
            <param name="Стан">Новий</param>
        </offer>
        """
        offer = ET.fromstring(xml)
        valid_cats = {"101", "102"}
        errors = self.validate_offer_element(offer, valid_categories=valid_cats)
        self.assertTrue(any("ERR_CATEGORY_UNRESOLVED" in e for e in errors))

    def test_invalid_photo_url_fails(self):
        xml = """
        <offer id="1" available="true">
            <price>100</price>
            <currencyId>UAH</currencyId>
            <stock_quantity>5</stock_quantity>
            <categoryId>101</categoryId>
            <picture>https://example.com/зображення з пробілом.jpg</picture>
            <vendor>Samsung</vendor>
            <name_ua>Навушники бездротові</name_ua>
            <description_ua><![CDATA[<p>Чудові бездротові навушники з відмінним звуком та мікрофоном.</p>]]></description_ua>
            <param name="Стан">Новий</param>
        </offer>
        """
        offer = ET.fromstring(xml)
        errors = self.validate_offer_element(offer)
        self.assertTrue(any("ERR_PICTURE_URL_INVALID" in e for e in errors))

    def test_validate_feed_structure(self):
        good_feed_xml = """
        <yml_catalog date="2026-10-13 16:00">
            <shop>
                <name>EVA Shop</name>
                <company>EVA</company>
                <url>https://eva.ua</url>
                <currencies>
                    <currency id="UAH" rate="1"/>
                </currencies>
                <categories>
                    <category id="101">Електроніка</category>
                </categories>
                <offers>
                </offers>
            </shop>
        </yml_catalog>
        """
        root = ET.fromstring(good_feed_xml)
        errors = self.validate_feed_structure(root)
        self.assertEqual(errors, [])

        bad_feed_xml = "<unknown_root></unknown_root>"
        bad_root = ET.fromstring(bad_feed_xml)
        errors = self.validate_feed_structure(bad_root)
        self.assertTrue(len(errors) > 0)

if __name__ == '__main__':
    unittest.main()
