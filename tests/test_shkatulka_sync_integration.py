import unittest
import lxml.etree as ET
from masterevaxml import build_dropt_catalog, normalize_sync_article, get_article

class TestShkatulkaSyncIntegration(unittest.TestCase):
    def setUp(self):
        dropt_xml = """
        <yml_catalog>
          <shop>
            <offers>
              <offer id="18556" available="true">
                <article>58487</article>
                <price>293.48</price>
                <currencyId>UAH</currencyId>
                <quantity>10</quantity>
              </offer>
              <offer id="11111" available="false">
                <article>99999</article>
                <price>200.00</price>
                <currencyId>UAH</currencyId>
                <quantity>0</quantity>
              </offer>
            </offers>
          </shop>
        </yml_catalog>
        """
        dropt_root = ET.fromstring(dropt_xml)
        self.dropt_catalog = build_dropt_catalog(dropt_root, {"UAH": 1.0}, "dropt.in.ua")

    def test_matched_available_shkatulka_offer(self):
        shk_xml = """
        <offer id="19022" available="true">
          <article>58487/9</article>
          <price>168</price>
          <currencyId>UAH</currencyId>
        </offer>
        """
        offer = ET.fromstring(shk_xml)
        art = get_article(offer)
        base_art = normalize_sync_article(art)
        
        self.assertIn(base_art, self.dropt_catalog)
        d_item = self.dropt_catalog[base_art]
        self.assertTrue(d_item["available"])
        self.assertEqual(d_item["price_uah"], 293.48)
        self.assertEqual(d_item["qty"], 10)
        # Verify calculated price matches Dropt's formula (293.48 * 1.20 + 50 = 402)
        self.assertEqual(d_item["price"], 402)
        self.assertEqual(d_item["old_price"], 502)

    def test_matched_unavailable_shkatulka_offer(self):
        shk_xml = """
        <offer id="20000" available="true">
          <article>99999/1</article>
          <price>150</price>
        </offer>
        """
        offer = ET.fromstring(shk_xml)
        art = get_article(offer)
        base_art = normalize_sync_article(art)
        
        self.assertIn(base_art, self.dropt_catalog)
        d_item = self.dropt_catalog[base_art]
        # In Shkatulka it says available=true, but in Dropt it is available=false!
        self.assertFalse(d_item["available"])

    def test_unmatched_shkatulka_offer(self):
        shk_xml = """
        <offer id="30000" available="true">
          <article>UNKNOWN-ART/9</article>
          <price>100</price>
        </offer>
        """
        offer = ET.fromstring(shk_xml)
        art = get_article(offer)
        base_art = normalize_sync_article(art)
        
        self.assertNotIn(base_art, self.dropt_catalog)

if __name__ == '__main__':
    unittest.main()
