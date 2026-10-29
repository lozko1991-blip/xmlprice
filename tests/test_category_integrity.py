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

if __name__ == '__main__':
    unittest.main()
