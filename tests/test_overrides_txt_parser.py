import unittest
import os
import tempfile

class TestOverridesTxtParser(unittest.TestCase):
    def test_parse_simple_override(self):
        content = """
# Коментар
[ТОВАР sp10144]
Назва: Магнітний павербанк 10000mAh MagSafe
Опис:
Перший рядок опису.
Другий рядок опису з "лапками" та 10" екраном.
        """
        from content_overrides import parse_overrides_text
        overrides, clones = parse_overrides_text(content)
        
        self.assertIn("SP10144", overrides)
        self.assertEqual(overrides["SP10144"]["name_ua"], "Магнітний павербанк 10000mAh MagSafe")
        self.assertIn('Другий рядок опису з "лапками"', overrides["SP10144"]["description_ua"])

    def test_parse_clone(self):
        content = """
[ДУБЛЬ sp10144]
Код: 5000
Артикул: sp10144-5000
Назва: Магнітний павербанк 5000mAh MagSafe
Ємність акумулятора: 5000 мА*год
        """
        from content_overrides import parse_overrides_text
        overrides, clones = parse_overrides_text(content)
        
        self.assertEqual(len(clones), 1)
        clone = clones[0]
        self.assertEqual(clone["source"], "SP10144")
        self.assertEqual(clone["id_suffix"], "5000")
        self.assertEqual(clone["article"], "sp10144-5000")
        self.assertEqual(clone["name_ua"], "Магнітний павербанк 5000mAh MagSafe")
        self.assertEqual(clone["params"].get("Ємність акумулятора"), "5000 мА*год")

    def test_various_header_formats(self):
        content = """
[ТОВАР: 12345]
Назва: Товар 1

[ARTICLE ABC]
Назва: Товар 2

[999]
Назва: Товар 3

[ДУБЛЬ: ABC]
Код: dupl1
Назва: Дубль АБЦ
        """
        from content_overrides import parse_overrides_text
        overrides, clones = parse_overrides_text(content)
        
        self.assertIn("12345", overrides)
        self.assertIn("ABC", overrides)
        self.assertIn("999", overrides)
        self.assertEqual(len(clones), 1)
        self.assertEqual(clones[0]["source"], "ABC")

if __name__ == '__main__':
    unittest.main()
