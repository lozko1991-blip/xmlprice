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

    def test_description_truncated_at_sentence_boundary_under_700_chars(self):
        """Long description must be truncated to <= 700 chars ending with a complete sentence period."""
        sentences = [
            "Це перше чудове розгорнуте речення про наш товар для дому та всі його переваги.",
            "Це друге речення, яке описує унікальні характеристики виробу та матеріали преміальної якості.",
            "Третє речення містить детальну інформацію про надійність, довговічність та тривалий термін використання.",
            "Четверте речення розповідає про надзвичайну легкість у догляді та максимальну практичність у повсякденному побуті.",
            "П'яте речення деталізує точні габарити, якість складання, ергономічність та комплектацію заводської поставки.",
            "Шосте речення надає професійні рекомендації експертів щодо оптимальних умов щоденної експлуатації виробу.",
            "Сьоме речення повідомляє про офіційні гарантійні зобов'язання виробника товару для кожного покупця.",
            "Восьме речення підсумовує всі ключові переваги даного виробу для найвибагливішого українського споживача.",
            "Дев'яте речення додає фінальний штрих до загального гармонійного опису представленого товару на сайті."
        ]
        long_html = "<p>" + " ".join(sentences) + "</p>"
        self.assertGreater(len(long_html), 700)
        
        cleaned = clean_description(long_html, "Тестовий товар", "Brand")
        self.assertLessEqual(len(cleaned), 700)
        
        # Must end cleanly with a closing tag, and the text before closing tag must end with a period
        plain_text = cleaned.replace("</p>", "").replace("<p>", "").strip()
        self.assertTrue(plain_text.endswith("."), f"Description should end with a period, got: {plain_text[-30:]}")
        self.assertTrue(cleaned.endswith("</p>"))

if __name__ == '__main__':
    unittest.main()
