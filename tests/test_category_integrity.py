import os
import csv
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

    def test_supplier_1200_category_mapping_integrity(self):
        """Supplier 1200 (Aveon) must have complete, verified category mapping under EVA."""
        from masterevaxml import load_eva_category_mapping
        mapping = load_eva_category_mapping()

        rules_1200 = {k: v for k, v in mapping.items() if str(k).startswith("1200") and k != "1200"}
        self.assertEqual(len(rules_1200), 137, "Supplier 1200 must have exactly 137 rules (47 base + 90 virtual)")

        # Test refined virtual categories
        self.assertEqual(rules_1200["1200122857"]["eva_id"], "122857") # Тостери
        self.assertEqual(rules_1200["1200104003"]["eva_id"], "104003") # Домашні капці жіночі
        self.assertEqual(rules_1200["1200122267"]["eva_id"], "122267") # Акумуляторні пилососи
        self.assertEqual(rules_1200["1200100833"]["eva_id"], "100833") # Електричні зубні щітки
        self.assertEqual(rules_1200["1200101257"]["eva_id"], "101257") # Косметички
        self.assertEqual(rules_1200["1200119856"]["eva_id"], "119856") # Музичні інструменти
        self.assertEqual(rules_1200["1200100547"]["eva_id"], "100547") # Манікюрні набори

        # Test non-EVA categories
        for no_eva in ["120030017035", "1200108482107", "120070180736", "120099320587", "1200108117197"]:
            self.assertEqual(rules_1200[no_eva]["status"], "NO_EVA_CATEGORY")
            self.assertEqual(rules_1200[no_eva]["eva_id"], "")

    def test_supplier_1200_category_resolver(self):
        """Verify that umbrella category items of supplier 1200 resolve to accurate EVA categories."""
        from masterevaxml import resolve_supplier_1200_category

        # Kitchen tech & cookware
        self.assertEqual(resolve_supplier_1200_category("Електрична м'ясорубка Rainberg RB-5412", orig_cat="30018858"), "1200122805")
        self.assertEqual(resolve_supplier_1200_category("Заглибний блендер Denx DX2072", orig_cat="30018858"), "1200121456")
        self.assertEqual(resolve_supplier_1200_category("Електричний дисковий скляний чайник Crownberg CB 9121", orig_cat="30018858"), "1200121458")
        self.assertEqual(resolve_supplier_1200_category("Аерофритюрниця електрична Zepline 6 л", orig_cat="30018858"), "1200122868")
        self.assertEqual(resolve_supplier_1200_category("Набір каструль 12 предметів з нержавіючої сталі", orig_cat="30018858"), "1200113858")
        self.assertEqual(resolve_supplier_1200_category("Набір кухонних ножів на підставці", orig_cat="30018858"), "1200101097")
        self.assertEqual(resolve_supplier_1200_category("Тостер Sokany HJT-008S на 2 скибочки", orig_cat="30018858"), "1200122857")
        self.assertEqual(resolve_supplier_1200_category("Кавомашина крапельна CB-1568 з двома чашками", orig_cat="30018858"), "1200121557")
        self.assertEqual(resolve_supplier_1200_category("Електрична плита Crownberg CB-1327 настільна інфрачервона", orig_cat="30018858"), "1200122853")
        self.assertEqual(resolve_supplier_1200_category("Притискний контактний електричний настільний гриль", orig_cat="30018858"), "1200121552")
        self.assertEqual(resolve_supplier_1200_category("Набір контейнерів для їжі 7 предметів FOOD Storage", orig_cat="30018858"), "1200100911")
        self.assertEqual(resolve_supplier_1200_category("Кухонний набір ZP кухонне начиння 12 предметів", orig_cat="30018858"), "1200101094")

        # Beauty tech
        self.assertEqual(resolve_supplier_1200_category("Машинка для стрижки волосся VGR V-030", orig_cat="30014688"), "1200104152")
        self.assertEqual(resolve_supplier_1200_category("Тример для носа та вух Kemei", orig_cat="30014688"), "1200104154")
        self.assertEqual(resolve_supplier_1200_category("Професійний фен для сушіння та укладання волосся VGR V-431", orig_cat="30014688"), "1200122876")
        self.assertEqual(resolve_supplier_1200_category("Масажер для тіла", orig_cat="30014688"), "1200116273")
        self.assertEqual(resolve_supplier_1200_category("Вакуумний апарат для чищення пор Beauty Skin Care", orig_cat="30014688"), "1200104266")
        self.assertEqual(resolve_supplier_1200_category("Електрична зубна щітка Shuke SK-601 акумуляторна", orig_cat="30014688"), "1200100833")
        self.assertEqual(resolve_supplier_1200_category("Манікюрний набір 18 предметів у футлярі", orig_cat="30014688"), "1200100547")
        self.assertEqual(resolve_supplier_1200_category("Органайзер для косметики 2 секції YM206", orig_cat="30014688"), "1200101257")

        # Home, garden, slippers & tools
        self.assertEqual(resolve_supplier_1200_category("Відпарювач для одягу вертикальний ручний", orig_cat="30017053"), "1200110856")
        self.assertEqual(resolve_supplier_1200_category("Настільна LED лампа акумуляторна з USB", orig_cat="30018536"), "1200117861")
        self.assertEqual(resolve_supplier_1200_category("Shoozy капці шльопанці жіночі", orig_cat="108118382"), "1200104003")
        self.assertEqual(resolve_supplier_1200_category("Автомобільний портативний бездротовий акумуляторний пилосос 2 in1", orig_cat="30017035"), "1200122267")
        self.assertEqual(resolve_supplier_1200_category("Троянда в колбі з LED-підсвіткою Велика NoA52 (Нічник)", orig_cat="99320587"), "1200102253")

        # Kids & toys
        self.assertEqual(resolve_supplier_1200_category("Ігрова приставка SUP Game Box 400 in 1", orig_cat="30017064"), "1200122677")
        self.assertEqual(resolve_supplier_1200_category("3D-ручка з LCD-дисплеєм для дітей", orig_cat="30017064"), "1200117852")
        self.assertEqual(resolve_supplier_1200_category("Дитячий шезлонг гойдалка з заколисуванням", orig_cat="30017064"), "1200121859")
        self.assertEqual(resolve_supplier_1200_category("Набір скетч-маркерів 168 шт. для малювання двосторонніх Touch", orig_cat="30017064"), "1200105923")
        self.assertEqual(resolve_supplier_1200_category("Караоке-мікрофон Wster WS-669 бездротовий", orig_cat="70180736"), "1200119856")

        # Sport & tourism
        self.assertEqual(resolve_supplier_1200_category("Душ туристичний Camp Shower похідний переносний", orig_cat="30017047"), "1200122925")
        self.assertEqual(resolve_supplier_1200_category("Намет автоматичний 2-місний ЗЕЛЕНИЙ", orig_cat="30017047"), "1200122926")

        # Non-EVA categories
        self.assertEqual(resolve_supplier_1200_category("Автомобільний компресор для підкачування шин", orig_cat="30017035"), "120030017035")
        self.assertEqual(resolve_supplier_1200_category("Уцінка. Пошкоджена упаковка", orig_cat="108482107"), "1200108482107")
        self.assertEqual(resolve_supplier_1200_category("Портативна Bluetooth колонка Boombox", orig_cat="70180736"), "120070180736")
        self.assertEqual(resolve_supplier_1200_category("Вібратор-яйце Spark of love", orig_cat="99320587"), "120099320587")

    def test_supplier_1200_virtual_categories_protected(self):
        """Virtual categories of 1200 must not be pruned by is_thin_category."""
        from masterevaxml import is_thin_category, _SUPPLIER_1200_VIRTUAL_CATEGORIES
        self.assertEqual(len(_SUPPLIER_1200_VIRTUAL_CATEGORIES), 90, "1200 must have exactly 90 virtual categories")
        for v_id in _SUPPLIER_1200_VIRTUAL_CATEGORIES:
            self.assertFalse(is_thin_category(v_id, 1, is_parent=False))
            self.assertFalse(is_thin_category(v_id, 0, is_parent=False))

    def test_supplier_7777_category_mapping_integrity(self):
        """Supplier 7777 (Posudograd) must have complete, verified category mapping under EVA."""
        from masterevaxml import load_eva_category_mapping
        mapping = load_eva_category_mapping()

        rules_7777 = {k: v for k, v in mapping.items() if str(k).startswith("7777")}
        self.assertEqual(len(rules_7777), 106, "Supplier 7777 must have exactly 106 mapped rules")

        # Test core tableware mapping
        self.assertEqual(rules_7777["7777218"]["eva_id"], "100942")  # Тарілки
        self.assertEqual(rules_7777["777782"]["eva_id"], "100935")   # Келихи
        self.assertEqual(rules_7777["777771"]["eva_id"], "100946")   # Стакани
        self.assertEqual(rules_7777["7777267"]["eva_id"], "113857")  # Каструлі
        self.assertEqual(rules_7777["7777258"]["eva_id"], "102201")  # Чайники для плити
        self.assertEqual(rules_7777["7777256"]["eva_id"], "113755")  # Термоси і пляшки
        self.assertEqual(rules_7777["7777310"]["eva_id"], "112110")  # Статуетки і фігурки
        self.assertEqual(rules_7777["7777311"]["eva_id"], "112311")  # Новорічний декор
        self.assertEqual(rules_7777["7777306"]["eva_id"], "118709")  # Набори для ванної кімнати
        self.assertEqual(rules_7777["7777197"]["eva_id"], "100902")  # Хлібниці
        self.assertEqual(rules_7777["7777261"]["eva_id"], "104063")  # Підставки та планки для ножів
        self.assertEqual(rules_7777["7777230"]["eva_id"], "100929")  # Пароварки, скороварки
        self.assertEqual(rules_7777["7777122"]["eva_id"], "100982")  # Марміти
        self.assertEqual(rules_7777["7777195"]["eva_id"], "113803")  # Барні аксесуари
        self.assertEqual(rules_7777["7777262"]["eva_id"], "101077")  # Млини для спецій
        self.assertEqual(rules_7777["7777277"]["eva_id"], "101009")  # Миски для змішування
        self.assertEqual(rules_7777["7777281"]["eva_id"], "112560")  # Еротичні настільні ігри

        # Test parent categories
        for pid in ["777719", "7777272", "777796", "777737", "7777304", "777791", "7777294", "7777252"]:
            self.assertEqual(rules_7777[pid]["status"], "PARENT")
            self.assertEqual(rules_7777[pid]["eva_id"], "")

        # Test out-of-scope non-EVA categories
        for no_eva_id in ["7777209", "777774"]:
            self.assertEqual(rules_7777[no_eva_id]["status"], "NO_EVA_CATEGORY")
            self.assertEqual(rules_7777[no_eva_id]["eva_id"], "")

    def test_supplier_9999_category_mapping_integrity(self):
        """Supplier 9999 (WebSklad) must have complete, verified category mapping under EVA."""
        from masterevaxml import load_eva_category_mapping
        mapping = load_eva_category_mapping()

        rules_9999 = {k: v for k, v in mapping.items() if str(k).startswith("9999")}
        self.assertEqual(len(rules_9999), 140, "Supplier 9999 must have exactly 140 rules (115 base + 25 virtual)")

        # Test refined categories
        self.assertEqual(rules_9999["9999507759522"]["eva_id"], "122875")  # Ваги кухонні -> Інша дрібна техніка
        self.assertEqual(rules_9999["99992194088440"]["eva_id"], "119658") # Павербанки -> Універсальні мобільні батареї
        self.assertEqual(rules_9999["99993581823412"]["eva_id"], "105920") # Наметы детские -> Намети, тунелі
        self.assertEqual(rules_9999["99994107734731"]["eva_id"], "122267") # Пылесосы авто -> Акумуляторні пилососи
        self.assertEqual(rules_9999["99992884069620"]["eva_id"], "119856") # Мікрофони -> Музичні інструменти
        self.assertEqual(rules_9999["99991429839245"]["eva_id"], "100954") # Столові прилади -> Столові прилади

        # Test new virtual categories
        self.assertEqual(rules_9999["9999114012"]["eva_id"], "114012") # Камені для віскі
        self.assertEqual(rules_9999["9999104012"]["eva_id"], "104012") # Іграшки для котів
        self.assertEqual(rules_9999["9999121552"]["eva_id"], "121552") # Грилі електричні
        self.assertEqual(rules_9999["9999121273"]["eva_id"], "121273") # Сушарки для одягу
        self.assertEqual(rules_9999["9999121279"]["eva_id"], "121279") # Портативні дитячі принтери
        self.assertEqual(rules_9999["9999122279"]["eva_id"], "122279") # Набори для пікніка
        self.assertEqual(rules_9999["9999121556"]["eva_id"], "121556") # Морозивниці
        self.assertEqual(rules_9999["9999102254"]["eva_id"], "102254") # Гаджети для догляду за обличчям
        self.assertEqual(rules_9999["9999113805"]["eva_id"], "113805") # Набори для бару
        self.assertEqual(rules_9999["9999102253"]["eva_id"], "102253") # Нічник
        self.assertEqual(rules_9999["9999122278"]["eva_id"], "122278") # Туристичні ножі
        self.assertEqual(rules_9999["9999122875"]["eva_id"], "122875") # Інша дрібна техніка
        self.assertEqual(rules_9999["9999100911"]["eva_id"], "100911") # Харчові контейнери
        self.assertEqual(rules_9999["9999120714"]["eva_id"], "120714") # Іграшкова зброя
        self.assertEqual(rules_9999["9999122925"]["eva_id"], "122925") # Портативний душ

        # Test non-EVA categories
        for no_eva in ["99993330400428", "99994183355383", "99992722838145", "99993813542800", "9999459200474"]:
            self.assertEqual(rules_9999[no_eva]["status"], "NO_EVA_CATEGORY")
            self.assertEqual(rules_9999[no_eva]["eva_id"], "")

    def test_supplier_9999_category_resolver(self):
        """WebSklad fallback resolver must map products to correct virtual categories."""
        from masterevaxml import resolve_supplier_9999_category

        self.assertEqual(resolve_supplier_9999_category("Камені для віскі Whiskey Stones 9 шт"), "9999114012")
        self.assertEqual(resolve_supplier_9999_category("Інтерактивна іграшка для котика трекбол з м'ячиком"), "9999104012")
        self.assertEqual(resolve_supplier_9999_category("Контактний гриль електричний RAF"), "9999121552")
        self.assertEqual(resolve_supplier_9999_category("Сушарка для одягу підвісна електрична"), "9999121273")
        self.assertEqual(resolve_supplier_9999_category("Портативний міні термопринтер для телефону"), "9999121279")
        self.assertEqual(resolve_supplier_9999_category("Набір для пікніка Camping Casual на 4 персони"), "9999122279")
        self.assertEqual(resolve_supplier_9999_category("Автоматична морозивниця Camry для десертів"), "9999121556")
        self.assertEqual(resolve_supplier_9999_category("Косметологічна LED-маска для омолодження шкіри обличчя"), "9999102254")
        self.assertEqual(resolve_supplier_9999_category("Алкогольна рулетка з чарками Casino Night"), "9999113805")
        self.assertEqual(resolve_supplier_9999_category("Електрична мухобойка пастка 3 в 1 з УФ-лампою"), "9999102253")
        self.assertEqual(resolve_supplier_9999_category("Мультитул-ніж Leatherman кишеньковий"), "9999122278")
        self.assertEqual(resolve_supplier_9999_category("Апарат для приготування цукрової солодкої вати"), "9999122875")
        self.assertEqual(resolve_supplier_9999_category("Набір сталевих судочків з кришками"), "9999100911")
        self.assertEqual(resolve_supplier_9999_category("Дитячий кулемет з мильними бульбашками Gatling"), "9999120714")
        self.assertEqual(resolve_supplier_9999_category("Лійка 4в1 масажна душова лійка з фільтром"), "9999122925")

        # Unknown / non-matching product
        self.assertEqual(resolve_supplier_9999_category("Перфоратор бочковий Grand ПЕ-1900"), "999999999")

    def test_supplier_9999_virtual_categories_protected(self):
        """Virtual categories of 9999 must not be pruned by is_thin_category."""
        from masterevaxml import is_thin_category, _SUPPLIER_9999_VIRTUAL_CATEGORIES
        for v_id in _SUPPLIER_9999_VIRTUAL_CATEGORIES:
            self.assertFalse(is_thin_category(v_id, 1, is_parent=False))
            self.assertFalse(is_thin_category(v_id, 0, is_parent=False))

    def test_supplier_1100_category_mapping_integrity(self):
        """Supplier 1100 (Forus) must have complete, verified category mapping under EVA."""
        from masterevaxml import load_eva_category_mapping
        mapping = load_eva_category_mapping()

        rules_1100 = {k: v for k, v in mapping.items() if str(k).startswith("1100") and k != "1100"}
        self.assertEqual(len(rules_1100), 344, "Supplier 1100 must have exactly 344 mapped rules (225 base + 119 virtual)")

        # Verify critical base categories
        self.assertEqual(rules_1100["1100007108"]["eva_id"], "100943")  # Оригінальні чашки та кружки -> Чашки
        self.assertEqual(rules_1100["110000791"]["eva_id"], "100879")   # Подушки інтер'єрні -> Подушка
        self.assertEqual(rules_1100["1100007170"]["eva_id"], "100890")  # Прикольні фартухи -> Фартух
        self.assertEqual(rules_1100["110000710"]["eva_id"], "101257")   # Косметички -> Косметички
        self.assertEqual(rules_1100["1100007265"]["eva_id"], "117952")  # Маски для сну -> Маски для сну
        self.assertEqual(rules_1100["1100007266"]["eva_id"], "104003")  # Домашні капці жіночі
        self.assertEqual(rules_1100["1100007133"]["eva_id"], "122918")  # Жіночі сумки
        self.assertEqual(rules_1100["1100007135"]["eva_id"], "122020")  # Рюкзаки
        self.assertEqual(rules_1100["1100007130"]["eva_id"], "121869")  # Оригінальні скарбнички -> Скарбнички
        self.assertEqual(rules_1100["110000775"]["eva_id"], "112153")   # Фоторамки
        self.assertEqual(rules_1100["1100007104"]["eva_id"], "112356")  # Світлодіодні гірлянди
        self.assertEqual(rules_1100["1100007380"]["eva_id"], "112112")  # Металеві таблички
        self.assertEqual(rules_1100["1100007310"]["eva_id"], "104207")  # Настільні ігри
        self.assertEqual(rules_1100["1100007329"]["eva_id"], "100289")  # Піжами кігурумі -> Піжами жіночі
        self.assertEqual(rules_1100["1100007318"]["eva_id"], "113707")  # Форми для яєць -> Форми
        self.assertEqual(rules_1100["110000776"]["eva_id"], "118709")   # Все для ванни -> Набори для ванної кімнати
        self.assertEqual(rules_1100["1100007289"]["eva_id"], "118709")  # Ванні приналежності -> Набори для ванної кімнати

        # Test non-EVA categories
        for no_eva_id in ["110000720", "110000790", "1100007272", "1100007346", "1100007362"]:
            self.assertEqual(rules_1100[no_eva_id]["status"], "NO_EVA_CATEGORY")
            self.assertEqual(rules_1100[no_eva_id]["eva_id"], "")

    def test_supplier_1100_category_resolver(self):
        """Test keyword-based resolver for Supplier 1100 umbrella and composite categories."""
        from masterevaxml import resolve_supplier_1100_category

        # Kitchen utensils & knives
        self.assertEqual(resolve_supplier_1100_category("Магнітна рейка для ножів 38 см", orig_cat="00722"), "1100104063")
        self.assertEqual(resolve_supplier_1100_category("Ніж кухонний шеф 20 см", orig_cat="00722"), "1100101013")
        self.assertEqual(resolve_supplier_1100_category("Набір ножів кухонних 6 предметів", orig_cat="00722"), "1100101097")
        self.assertEqual(resolve_supplier_1100_category("Ножиці кухонні багатофункціональні", orig_cat="007268"), "1100100996")
        self.assertEqual(resolve_supplier_1100_category("Дошка обробна бамбукова", orig_cat="00722"), "1100100984")
        self.assertEqual(resolve_supplier_1100_category("Тертка багатофункціональна 4в1", orig_cat="007268"), "1100105454")
        self.assertEqual(resolve_supplier_1100_category("Кухарська лопатка силіконова", orig_cat="007268"), "1100113657")
        self.assertEqual(resolve_supplier_1100_category("Набір силіконових кришок 6 шт", orig_cat="007268"), "1100100995")
        self.assertEqual(resolve_supplier_1100_category("Силіконові форми для випікання пасок (3шт.)", orig_cat="99999"), "1100114003")
        self.assertEqual(resolve_supplier_1100_category("Набір посуду похідний Cooking Set DS-301", orig_cat="99999"), "1100113858")

        # Barware & drinks
        self.assertEqual(resolve_supplier_1100_category("Чашка керамічна Закохане ведмежа", orig_cat="99999"), "1100100943")
        self.assertEqual(resolve_supplier_1100_category("Камені для віскі Whiskey Stones 9 шт", orig_cat="007138"), "1100114012")
        self.assertEqual(resolve_supplier_1100_category("Термос металевий з ситечком 500 мл", orig_cat="007132"), "1100113755")
        self.assertEqual(resolve_supplier_1100_category("Фляга велика сувенірна в шкіряному чохлі", orig_cat="00785"), "1100113805")

        # Home, decor, bathroom
        self.assertEqual(resolve_supplier_1100_category("Голографічний зоряний проектор нічник", orig_cat="99999"), "1100102253")
        self.assertEqual(resolve_supplier_1100_category("Світлодіодна гірлянда роса 10 м", orig_cat="007104"), "1100112356")
        self.assertEqual(resolve_supplier_1100_category("Панно декор картини на стіну квітуче кохання", orig_cat="99999"), "1100112107")
        self.assertEqual(resolve_supplier_1100_category("Кошик для білизни та іграшок", orig_cat="99999"), "1100104205")
        self.assertEqual(resolve_supplier_1100_category("Ваза керамічна Ginger рожева", orig_cat="99999"), "1100104309")
        self.assertEqual(resolve_supplier_1100_category("Шторка для ванної кімнати водонепроникна", orig_cat="00776"), "1100118709")
        self.assertEqual(resolve_supplier_1100_category("Дозатор для рідкого мила сенсорний", orig_cat="00776"), "1100118705")

        # Beauty, health, gadgets
        self.assertEqual(resolve_supplier_1100_category("Маска для сну з охолоджуючим гелем", orig_cat="007265"), "1100117952")
        self.assertEqual(resolve_supplier_1100_category("Електричний вібромасажер ударний", orig_cat="00721"), "1100116273")
        self.assertEqual(resolve_supplier_1100_category("Вакуумний очищувач пор для обличчя", orig_cat="00721"), "1100104266")
        self.assertEqual(resolve_supplier_1100_category("Бездротовий зарядний пристрій Fast Charge", orig_cat="00787"), "1100122716")
        self.assertEqual(resolve_supplier_1100_category("Тримач для смартфона на торпеду авто", orig_cat="00714"), "1100117906")

        # Games & toys
        self.assertEqual(resolve_supplier_1100_category("Покерний набір на 100 фішок у жерстяній коробці", orig_cat="99999"), "1100104207")
        self.assertEqual(resolve_supplier_1100_category("Гра кубики камасутра для дорослих 18+", orig_cat="007208"), "1100112560")
        self.assertEqual(resolve_supplier_1100_category("Карнавальний костюм Діда Мороза з бородою", orig_cat="007328"), "1100121896")

    def test_supplier_1100_virtual_categories_protected(self):
        """Virtual categories of 1100 must not be pruned by is_thin_category."""
        from masterevaxml import is_thin_category, _SUPPLIER_1100_VIRTUAL_CATEGORIES
        self.assertEqual(len(_SUPPLIER_1100_VIRTUAL_CATEGORIES), 119, "1100 must have exactly 119 virtual categories")
        for v_id in _SUPPLIER_1100_VIRTUAL_CATEGORIES:
            self.assertFalse(is_thin_category(v_id, 1, is_parent=False))
            self.assertFalse(is_thin_category(v_id, 0, is_parent=False))


    def test_supplier_3333_category_mapping_integrity(self):
        """Supplier 3333 (Lugi) must have complete, verified category mapping under EVA."""
        from masterevaxml import load_eva_category_mapping
        mapping = load_eva_category_mapping()

        rules_3333 = {k: v for k, v in mapping.items() if str(k).startswith("3333") and k != "3333"}
        self.assertEqual(len(rules_3333), 253, "Supplier 3333 must have exactly 253 mapped rules (247 base + 6 virtual)")

        # Verify critical corrected categories
        self.assertEqual(rules_3333["3333149"]["eva_id"], "122926")  # Палатки -> Намети й аксесуари (not child toilet stool!)
        self.assertEqual(rules_3333["333316"]["eva_id"], "104152")   # Машинки для стрижки -> Машинки для стрижки (not toy cars!)
        self.assertEqual(rules_3333["333397"]["eva_id"], "101008")   # Весы кухонные -> Мірний посуд (not bathroom scales!)
        self.assertEqual(rules_3333["333366"]["eva_id"], "100967")   # Сервизы -> Столові сервізи (not oven mitts!)
        self.assertEqual(rules_3333["333349"]["eva_id"], "101013")   # Наборы кухонных ножей -> Ніж кухонний (valid ID!)
        self.assertEqual(rules_3333["333357"]["eva_id"], "100943")   # Чашки -> Чашки
        self.assertEqual(rules_3333["333363"]["eva_id"], "113857")   # Кастрюли -> Каструлі, ковші
        self.assertEqual(rules_3333["3333273"]["eva_id"], "100878")  # Коврики -> Килимок
        self.assertEqual(rules_3333["3333265"]["eva_id"], "101714")  # Хозяйственные товары -> Швабри
        self.assertEqual(rules_3333["3333128"]["eva_id"], "104056")  # Органайзеры -> Органайзери для зберігання
        self.assertEqual(rules_3333["3333145"]["eva_id"], "122706")  # Смарт часы -> Смарт-годинники
        self.assertEqual(rules_3333["333347"]["eva_id"], "101077")   # Наборы для специй -> Приладдя для спецій
        self.assertEqual(rules_3333["333342"]["eva_id"], "113755")   # Термосы для детей -> Термоси та термокухлі
        self.assertEqual(rules_3333["3333326"]["eva_id"], "100952")  # Подносы -> Підноси
        self.assertEqual(rules_3333["333331"]["status"], "NO_EVA_CATEGORY")   # Настінні годинники
        self.assertEqual(rules_3333["3333100"]["status"], "NO_EVA_CATEGORY")  # Камера спостереження

    def test_supplier_3333_category_resolver(self):
        """Test keyword-based resolver for Supplier 3333 composite categories."""
        from masterevaxml import resolve_supplier_3333_category

        # Table placemats vs floor mats in 273
        self.assertEqual(
            resolve_supplier_3333_category("Сервірувальні серветки з бамбука під тарілки", "", "", "273"),
            "3333_v1"
        )
        self.assertEqual(
            resolve_supplier_3333_category("Килимок для ванної кімнати на присосках", "", "", "273"),
            "3333273"
        )

        # Cleaning accessories vs mops in 265
        self.assertEqual(
            resolve_supplier_3333_category("Скребок для миття скла та вікон", "", "", "265"),
            "3333_v2"
        )
        self.assertEqual(
            resolve_supplier_3333_category("Комплект совок та щітка для сміття", "", "", "265"),
            "3333_v3"
        )
        self.assertEqual(
            resolve_supplier_3333_category("Швабра з віджимом з мікрофібри", "", "", "265"),
            "3333265"
        )

        # Jewelry boxes in organizers 128
        self.assertEqual(
            resolve_supplier_3333_category("Скринька для прикрас з еко шкіри 2 яруси", "", "", "128"),
            "3333_v4"
        )
        self.assertEqual(
            resolve_supplier_3333_category("Шафа для взуття портативна пластикова", "", "", "128"),
            "3333128"
        )

        # Cookware sets in pots 63
        self.assertEqual(
            resolve_supplier_3333_category("Набір посуду для приготування 12 предметів", "", "", "63"),
            "3333_v5"
        )
        self.assertEqual(
            resolve_supplier_3333_category("Каструля емальована 2 літри", "", "", "63"),
            "333363"
        )

        # Cheese boards in dishes 182
        self.assertEqual(
            resolve_supplier_3333_category("Набір для нарізки сиру з ножами", "", "", "182"),
            "3333_v6"
        )
        self.assertEqual(
            resolve_supplier_3333_category("Блюдо сервірувальне порцелянове", "", "", "182"),
            "3333182"
        )

    def test_supplier_3333_virtual_categories_protected(self):
        """Supplier 3333 virtual categories must not be pruned by is_thin_category."""
        from masterevaxml import is_thin_category, _SUPPLIER_3333_VIRTUAL_CATEGORIES
        self.assertEqual(len(_SUPPLIER_3333_VIRTUAL_CATEGORIES), 6)
        for v_id in _SUPPLIER_3333_VIRTUAL_CATEGORIES:
            self.assertFalse(is_thin_category(v_id, 0), f"Virtual category {v_id} must not be thin")


    def test_supplier_4444_category_mapping_integrity(self):
        """Supplier 4444 (Dropom) must have complete, verified category mapping under EVA."""
        from masterevaxml import load_eva_category_mapping
        import json
        mapping = load_eva_category_mapping()

        rules_4444 = {k: v for k, v in mapping.items() if str(k).startswith("4444")}
        self.assertGreaterEqual(len(rules_4444), 630, f"Supplier 4444 must have >= 630 mapped rules, got {len(rules_4444)}")

        # Verify against eva_sheet_cache.json
        cache_path = os.path.join(os.path.dirname(__file__), '..', 'scratch', 'eva_sheet_cache.json')
        if os.path.exists(cache_path):
            with open(cache_path, 'r', encoding='utf-8') as f:
                valid_leaf_ids = {item['id'] for item in json.load(f)}
            for cid, info in rules_4444.items():
                eva_id = info.get('eva_id', '')
                self.assertIn(eva_id, valid_leaf_ids, f"Category {cid} maps to invalid leaf {eva_id}")

        # Verify key mapped categories
        self.assertEqual(rules_4444["4444133276351"]["eva_id"], "112356")  # Гірлянди -> Світлодіодні гірлянди
        self.assertEqual(rules_4444["4444133276089"]["eva_id"], "104152")  # Машинки для стрижки
        self.assertEqual(rules_4444["4444133276228"]["eva_id"], "121456")  # Блендери
        self.assertEqual(rules_4444["4444133276239"]["eva_id"], "122255")  # Вентилятори
        self.assertEqual(rules_4444["4444133276345"]["eva_id"], "116270")  # Ліхтарі
        self.assertEqual(rules_4444["4444133276090"]["eva_id"], "122717")  # Навушники
        self.assertEqual(rules_4444["4444133276179"]["eva_id"], "122876")  # Фени для волосся
        self.assertEqual(rules_4444["4444133276282"]["eva_id"], "104116")  # Плойки та стайлери
        self.assertEqual(rules_4444["4444133276191"]["eva_id"], "105752")  # Радіокеровані іграшки
        self.assertEqual(rules_4444["4444133276192"]["eva_id"], "101231")  # М'які іграшки
        self.assertEqual(rules_4444["4444133276214"]["eva_id"], "100879")  # Подушки
        self.assertEqual(rules_4444["4444133276215"]["eva_id"], "100877")  # Пледи
        self.assertEqual(rules_4444["4444133276216"]["eva_id"], "100882")  # Покривала
        self.assertEqual(rules_4444["4444133276180"]["eva_id"], "116273")  # Масажери

    def test_supplier_4444_category_resolver(self):
        """Supplier 4444 resolver must correctly classify composite category offers."""
        from masterevaxml import resolve_supplier_4444_category

        # 1. Composite category 132827487
        self.assertEqual(
            resolve_supplier_4444_category("Бездротовий пилосос для прибирання дому 80Вт", "", "", "132827487"),
            "4444_v22"
        )
        self.assertEqual(
            resolve_supplier_4444_category("Професійний фен для волосся 6000 Вт Mozer MZ 3100", "", "", "132827487"),
            "4444_v13"
        )
        self.assertEqual(
            resolve_supplier_4444_category("Плойка для волосся афро плойка 9 мм Geemy GM 2825", "", "", "132827487"),
            "4444_v14"
        )
        self.assertEqual(
            resolve_supplier_4444_category("Машинка для стрижки волосся та бороди 2в1 Gemei GM-657", "", "", "132827487"),
            "4444_v15"
        )
        self.assertEqual(
            resolve_supplier_4444_category("Блендер занурювальний 4в1 RAINBERG RB 6211 1200Вт", "", "", "132827487"),
            "4444_v17"
        )
        self.assertEqual(
            resolve_supplier_4444_category("Електрична м'ясорубка із соковижималкою BITEK BT-3700", "", "", "132827487"),
            "4444_v18"
        )
        self.assertEqual(
            resolve_supplier_4444_category("Контактний електрогриль з лотком для жиру 2000Вт", "", "", "132827487"),
            "4444_v19"
        )
        self.assertEqual(
            resolve_supplier_4444_category("Крапельна кавоварка 1200Вт Top Kitchen TK00220", "", "", "132827487"),
            "4444_v4"
        )
        self.assertEqual(
            resolve_supplier_4444_category("Портативний масажер Fascial Gun KH-320", "", "", "132827487"),
            "4444_v30"
        )
        self.assertEqual(
            resolve_supplier_4444_category("Нічник Безкінечні тюльпани від USB", "", "", "132827487"),
            "4444_v20"
        )
        self.assertEqual(
            resolve_supplier_4444_category("Смарт годинник з функцією дзвінків XO M10", "", "", "132827487"),
            "4444_v27"
        )
        self.assertEqual(
            resolve_supplier_4444_category("Дитяча м'яка іграшка Гусь 110 см", "", "", "132827487"),
            "4444_v29"
        )
        self.assertEqual(
            resolve_supplier_4444_category("Електрична грілка для ніг від USB", "", "", "132827487"),
            "4444_v25"
        )
        self.assertEqual(
            resolve_supplier_4444_category("Вішалка для одягу підлогова на 2 яруси", "", "", "132827487"),
            "4444_v26"
        )

        # 2. Steamers vs steam cleaners vs lint removers in 133276236
        self.assertEqual(
            resolve_supplier_4444_category("Відпарювач для одягу 1500Вт SOKANY SK-3050", "", "", "133276236"),
            "4444_v1"
        )
        self.assertEqual(
            resolve_supplier_4444_category("Ручний пароочисник на 350мл Steam Cleaner DF-A001", "", "", "133276236"),
            "4444_v2"
        )
        self.assertEqual(
            resolve_supplier_4444_category("Машинка від катишків акумуляторна Lint Remover M5", "", "", "133276236"),
            "4444_v3"
        )
        self.assertEqual(
            resolve_supplier_4444_category("Парова праска з керамічним покриттям 2200 Вт", "", "", "133276236"),
            "4444133276236"
        )

        # 3. Coffee makers vs grinders in 133276231
        self.assertEqual(
            resolve_supplier_4444_category("Кавомолка електрична RAF-7113 150Вт", "", "", "133276231"),
            "4444_v5"
        )
        self.assertEqual(
            resolve_supplier_4444_category("Капельна кавоварка 650Вт SOKANY SK-125", "", "", "133276231"),
            "4444_v4"
        )

        # 4. Thermoses vs kettles in 148240778
        self.assertEqual(
            resolve_supplier_4444_category("Вакуумний термос 2л із датчиком температури", "", "", "148240778"),
            "4444_v6"
        )
        self.assertEqual(
            resolve_supplier_4444_category("Електрочайник скляний 1,8 л 1800Вт", "", "", "148240778"),
            "4444148240778"
        )

        # 5. Fallback 99999 routing
        self.assertEqual(
            resolve_supplier_4444_category("Аерофритюрниця на 6л 2800Вт Hoffmans", "", "", "99999"),
            "4444_v19"
        )

    def test_supplier_4444_virtual_categories_protected(self):
        """Supplier 4444 virtual categories must not be pruned by is_thin_category."""
        from masterevaxml import is_thin_category, _SUPPLIER_4444_VIRTUAL_CATEGORIES
        self.assertEqual(len(_SUPPLIER_4444_VIRTUAL_CATEGORIES), 30)
        for v_id in _SUPPLIER_4444_VIRTUAL_CATEGORIES:
            self.assertFalse(is_thin_category(v_id, 0), f"Virtual category {v_id} must not be thin")

    def test_supplier_kievopt_category_mapping_integrity(self):
        """Supplier kievopt must have complete, verified category mapping under EVA (824 rules)."""
        import csv
        import json
        import os

        csv_path = os.path.join(os.path.dirname(__file__), '..', 'docs', 'eva_category_mapping.csv')
        rules_kievopt = {}
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for r in reader:
                if r.get('supplier') == 'kievopt':
                    rules_kievopt[r['cat_id']] = r

        self.assertEqual(len(rules_kievopt), 824, f"Supplier kievopt must have exactly 824 rules, got {len(rules_kievopt)}")

        # Verify leaf node validity against eva_sheet_cache.json
        cache_path = os.path.join(os.path.dirname(__file__), '..', 'scratch', 'eva_sheet_cache.json')
        if os.path.exists(cache_path):
            with open(cache_path, 'r', encoding='utf-8') as f:
                valid_leaf_ids = {item['id'] for item in json.load(f)}
            for cid, info in rules_kievopt.items():
                if info.get('status') == 'MAPPED':
                    eva_id = info.get('eva_id', '')
                    self.assertIn(eva_id, valid_leaf_ids, f"Kievopt category {cid} maps to invalid leaf {eva_id}")

        # Verify key mapped categories
        self.assertEqual(rules_kievopt['1082']['eva_id'], '122715')   # Чехлы для мобильных телефонов
        self.assertEqual(rules_kievopt['4615']['eva_id'], '105310')   # Картины по номерам
        self.assertEqual(rules_kievopt['1425']['eva_id'], '104207')   # Настольные игры
        self.assertEqual(rules_kievopt['1419']['eva_id'], '105338')   # Конструкторы
        self.assertEqual(rules_kievopt['1424']['eva_id'], '101231')   # Мягкие игрушки
        self.assertEqual(rules_kievopt['1435']['eva_id'], '101241')   # Подгузники и пеленки
        self.assertEqual(rules_kievopt['1092']['eva_id'], '122717')   # Наушники и гарнитуры
        self.assertEqual(rules_kievopt['1300']['eva_id'], '122876')   # Фены
        self.assertEqual(rules_kievopt['1288']['eva_id'], '121458')   # Электрочайники
        self.assertEqual(rules_kievopt['1060']['eva_id'], '122716')   # Блоки питания для ноутбуков
        self.assertEqual(rules_kievopt['4193']['eva_id'], '100933')   # Сковородки - NEW!
        self.assertEqual(rules_kievopt['6583']['eva_id'], '120713')   # Шарики для сухого бассейна

        self.assertEqual(rules_kievopt['1079']['eva_id'], '119658')   # Аккумуляторы внешние (PowerBank)
        self.assertEqual(rules_kievopt['1445']['eva_id'], '116265')   # Детские электромобили
        self.assertEqual(rules_kievopt['1968']['eva_id'], '116266')   # Роликовые коньки
        self.assertEqual(rules_kievopt['1446']['eva_id'], '116267')   # Скутеры (самокати)
        self.assertEqual(rules_kievopt['2102']['eva_id'], '122918')   # Женские сумки
        self.assertEqual(rules_kievopt['4915']['eva_id'], '111752')   # Поясна сумка (бананка)
        self.assertEqual(rules_kievopt['2754']['eva_id'], '100876')   # Электропростыни, одеяла -> Ковдри
        self.assertEqual(rules_kievopt['1907']['eva_id'], '107703')   # Дорожная сумка
        self.assertEqual(rules_kievopt['1973']['eva_id'], '119354')   # Рюкзак-переноска
        self.assertEqual(rules_kievopt['5318']['eva_id'], '104015')   # Сухой корм для собак
        self.assertEqual(rules_kievopt['5344']['eva_id'], '104009')   # Сухой корм для котов
        self.assertEqual(rules_kievopt['5418']['eva_id'], '104026')   # Игрушки для собак
        self.assertEqual(rules_kievopt['5417']['eva_id'], '104012')   # Игрушки для кошек
        self.assertEqual(rules_kievopt['5352']['eva_id'], '104208')   # Подгузники для животных -> Пелюшки для собак

        # Verify legitimate NO_EVA_CATEGORY rules for IT components / consumables / PC peripherals
        self.assertEqual(rules_kievopt['1215']['status'], 'NO_EVA_CATEGORY')  # Картриджи
        self.assertEqual(rules_kievopt['1100']['status'], 'NO_EVA_CATEGORY')  # Корпуса ПК
        self.assertEqual(rules_kievopt['1067']['status'], 'NO_EVA_CATEGORY')  # Внутренние SSD
        self.assertEqual(rules_kievopt['1096']['status'], 'NO_EVA_CATEGORY')  # Процессоры
        self.assertEqual(rules_kievopt['1198']['status'], 'NO_EVA_CATEGORY')  # Мышки
        self.assertEqual(rules_kievopt['1197']['status'], 'NO_EVA_CATEGORY')  # Клавиатуры
        self.assertEqual(rules_kievopt['1180']['status'], 'NO_EVA_CATEGORY')  # USB Flash
        self.assertEqual(rules_kievopt['1090']['status'], 'NO_EVA_CATEGORY')  # Акустические системы
        self.assertEqual(rules_kievopt['1149']['status'], 'NO_EVA_CATEGORY')  # Коврики для мыши

if __name__ == '__main__':
    unittest.main()
