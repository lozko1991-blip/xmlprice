import unittest
import lxml.etree as ET
from masterevaxml import clean_description, fix_text, ru_to_ua
from feed_passport_validator import has_ascii_control_chars

class TestXmlSanitizationAndCdata(unittest.TestCase):
    def test_ascii_control_chars_removed_from_descriptions(self):
        """ASCII control characters 0-31 must be eliminated from descriptions."""
        dirty_input = "<p>Опис товару\x00 з нуль-байтами\x07 та белл-символами\x1b.</p>"
        cleaned = clean_description(dirty_input, "Тестовий товар", "ТестБренд")
        self.assertFalse(has_ascii_control_chars(cleaned))
        self.assertIn("Опис товару з нуль-байтами та белл-символами.", cleaned)

    def test_cdata_closing_tag_is_escaped(self):
        """Descriptions must never contain raw ']]>' which would corrupt XML CDATA blocks."""
        dangerous_input = "<p>Текст який містить ]]> заборонену послідовність.</p>"
        cleaned = clean_description(dangerous_input, "Тестовий товар", "ТестБренд")
        self.assertNotIn("]]>", cleaned)
        self.assertIn("]] >", cleaned)

    def test_external_links_and_images_are_stripped(self):
        """EVA forbids images and external URLs inside descriptions."""
        input_html = (
            "<p>Купуйте на нашому сайті <a href='https://bad-site.com'>bad-site.com</a> "
            "<img src='https://bad-site.com/pic.jpg'/> якісний товар для дому.</p>"
        )
        cleaned = clean_description(input_html, "Тестовий товар", "ТестБренд")
        self.assertNotIn("https://bad-site.com", cleaned)
        self.assertNotIn("<img", cleaned)

    def test_short_description_fallback_enforces_eva_30_char_rule(self):
        """Descriptions under 30 characters of plain text must trigger a safe EVA fallback."""
        short_input = "<p>Коротко</p>"
        name_ua = "Супер Павербанк 10000mAh"
        vendor = "Baseus"
        cleaned = clean_description(short_input, name_ua, vendor)
        
        # Must be >= 30 chars
        plain_len = len(cleaned.replace("<p>", "").replace("</p>", "").strip())
        self.assertGreaterEqual(plain_len, 30)
        self.assertIn(name_ua, cleaned)
        self.assertIn(vendor, cleaned)

    def test_fix_text_cleans_whitespace_and_control_chars(self):
        """fix_text helper must remove non-printable characters and clean spaces."""
        raw = "  Товар \x01 з \t пробілами  та \n новими рядками \x1f "
        fixed = fix_text(raw)
        self.assertFalse(has_ascii_control_chars(fixed))
        self.assertNotIn("\x01", fixed)
        self.assertNotIn("\x1f", fixed)

    def test_ru_to_ua_transliteration(self):
        """Basic Russian letters (ы, э, ъ, ё) must be mapped to Ukrainian equivalents."""
        self.assertEqual(ru_to_ua("быстро"), "бистро")
        self.assertEqual(ru_to_ua("экран"), "екран")
        self.assertEqual(ru_to_ua("съезд"), "сезд")
        self.assertEqual(ru_to_ua("ёлка"), "елка")

if __name__ == '__main__':
    unittest.main()
