import unittest
import lxml.etree as ET
from masterevaxml import build_dropt_catalog

class TestDroptCatalogIndexing(unittest.TestCase):
    def test_build_dropt_catalog(self):
        xml = """
        <yml_catalog>
          <shop>
            <offers>
              <offer id="18556" available="true">
                <article>58487</article>
                <price>293.48</price>
                <currencyId>UAH</currencyId>
                <quantity>5</quantity>
              </offer>
              <offer id="99999" available="false">
                <article>12345/1</article>
                <price>100</price>
                <currencyId>UAH</currencyId>
                <quantity>0</quantity>
              </offer>
            </offers>
          </shop>
        </yml_catalog>
        """
        root = ET.fromstring(xml)
        catalog = build_dropt_catalog(root, {"UAH": 1.0}, "dropt.in.ua")
        
        self.assertIn("58487", catalog)
        self.assertTrue(catalog["58487"]["available"])
        self.assertEqual(catalog["58487"]["price_uah"], 293.48)
        self.assertEqual(catalog["58487"]["qty"], 5)
        self.assertEqual(catalog["58487"]["id"], "18556")
        self.assertIn("price", catalog["58487"])
        self.assertIn("old_price", catalog["58487"])
        
        self.assertIn("12345", catalog)
        self.assertFalse(catalog["12345"]["available"])

if __name__ == '__main__':
    unittest.main()
