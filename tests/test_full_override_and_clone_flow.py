import unittest
import lxml.etree as ET
from content_overrides import get_product_override, get_product_clones
from copy import deepcopy

class TestFullOverrideAndCloneFlow(unittest.TestCase):
    def test_flow_for_sp10144(self):
        # 1. Base offer from Opt-drop
        raw_xml = """
        <offer id="106619" available="true">
            <price>371</price>
            <vendorCode>sp10144</vendorCode>
            <vendor>Domotec</vendor>
            <name>Магнітний павербанк 10000mAh MagSafe / Power Bank lightning / Повербанк магнітний бездротовий 22,5W Domotec</name>
            <categoryId>202</categoryId>
            <picture>https://example.com/pic1.jpg</picture>
            <param name="Ємність акумулятора">10000 мА*год</param>
        </offer>
        """
        offer = ET.fromstring(raw_xml)
        raw_id = "106619"
        id_prefix = "2222"
        offer_id = f"{id_prefix}_{raw_id}"
        article = offer.findtext('vendorCode')
        name_ua = offer.findtext('name')
        price = 541
        old_price = 676
        
        # Override application
        override_data = get_product_override(offer_id=offer_id, raw_id=raw_id, article=article)
        self.assertIsNotNone(override_data)
        if override_data.get("name_ua"):
            name_ua = override_data["name_ua"]
            
        self.assertEqual(
            name_ua,
            "Магнітний павербанк 10000mAh MagSafe / Power Bank lightning / Повербанк магнітний бездротовий"
        )
        
        # Build new_off
        new_off = ET.Element("offer", id=offer_id, available="true")
        ET.SubElement(new_off, "price").text = str(price)
        ET.SubElement(new_off, "price_old").text = str(old_price)
        ET.SubElement(new_off, "article").text = article
        ET.SubElement(new_off, "name_ua").text = name_ua
        param_el = ET.SubElement(new_off, "param", name="Ємність акумулятора")
        param_el.text = "10000 мА*год"
        
        processed = [new_off]
        
        # Clone generation
        clones = get_product_clones(offer_id=offer_id, raw_id=raw_id, article=article)
        self.assertEqual(len(clones), 1)
        for clone_cfg in clones:
            clone_off = deepcopy(new_off)
            clone_id = f"{offer_id}_{clone_cfg.get('id_suffix')}"
            clone_off.set("id", clone_id)
            if clone_cfg.get("article"):
                clone_off.find("article").text = clone_cfg["article"]
            if clone_cfg.get("name_ua"):
                clone_off.find("name_ua").text = clone_cfg["name_ua"]
            if clone_cfg.get("params"):
                for p in clone_off.findall("param"):
                    if p.get("name") in clone_cfg["params"]:
                        p.text = clone_cfg["params"][p.get("name")]
            processed.append(clone_off)
            
        self.assertEqual(len(processed), 2)
        
        # Check original
        self.assertEqual(processed[0].get("id"), "2222_106619")
        self.assertEqual(processed[0].findtext("article"), "sp10144")
        self.assertEqual(processed[0].findtext("name_ua"), "Магнітний павербанк 10000mAh MagSafe / Power Bank lightning / Повербанк магнітний бездротовий")
        self.assertEqual(processed[0].findtext("price"), "541")
        self.assertEqual(processed[0].find("param").text, "10000 мА*год")
        
        # Check clone
        self.assertEqual(processed[1].get("id"), "2222_106619_5000")
        self.assertEqual(processed[1].findtext("article"), "sp10144-5000")
        self.assertEqual(processed[1].findtext("name_ua"), "Магнітний павербанк 5000mAh MagSafe / Power Bank lightning / Повербанк магнітний бездротовий")
        self.assertEqual(processed[1].findtext("price"), "541")
        self.assertEqual(processed[1].find("param").text, "5000 мА*год")

    def test_clone_creates_new_param_when_missing(self):
        # Base element without any params
        base = ET.Element("offer", id="2222_106619")
        clones = get_product_clones(article="sp10144")
        self.assertTrue(len(clones) > 0)
        clone_cfg = clones[0]
        clone_off = deepcopy(base)
        override_params = clone_cfg.get("params")
        if override_params and isinstance(override_params, dict):
            existing_nodes = {p.get("name"): p for p in clone_off.findall("param")}
            for p_name, p_val in override_params.items():
                if p_name in existing_nodes:
                    existing_nodes[p_name].text = str(p_val)
                else:
                    ET.SubElement(clone_off, "param", name=p_name).text = str(p_val)
        
        param_elem = clone_off.find("param")
        self.assertIsNotNone(param_elem)
        self.assertEqual(param_elem.get("name"), "Ємність акумулятора")
        self.assertEqual(param_elem.text, "5000 мА*год")

if __name__ == '__main__':
    unittest.main()
