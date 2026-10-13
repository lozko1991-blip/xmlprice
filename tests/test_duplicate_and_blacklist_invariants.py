import unittest
from masterevaxml import Blacklist

class TestDuplicateAndBlacklistInvariants(unittest.TestCase):
    def test_single_item_retention_logic(self):
        """When duplicates exist within a supplier, retain exactly 1 instance."""
        incoming_offers = [
            {"id": "2000_5082733", "title": "Товар 1 (копія 1)"},
            {"id": "2000_5082733", "title": "Товар 1 (копія 2)"},
            {"id": "2000_5082734", "title": "Товар 2 (копія 1)"},
            {"id": "2000_5082734", "title": "Товар 2 (копія 2)"},
            {"id": "2000_5082735", "title": "Товар 3 (унікальний)"},
        ]
        
        seen_ids = set()
        kept_offers = []
        duplicate_count = 0

        for off in incoming_offers:
            off_id = off["id"]
            if off_id in seen_ids:
                duplicate_count += 1
                continue
            seen_ids.add(off_id)
            kept_offers.append(off)

        # Invariant checks
        self.assertEqual(len(kept_offers), 3)
        self.assertEqual(duplicate_count, 2)
        # All distinct IDs must still exist in the output
        kept_ids = [o["id"] for o in kept_offers]
        self.assertEqual(kept_ids, ["2000_5082733", "2000_5082734", "2000_5082735"])

    def test_blacklist_supplier_filtering(self):
        """Disabled supplier should block offers from that supplier."""
        bl = Blacklist(10)
        bl.disabled_suppliers.add("badsupplier")

        self.assertTrue(bl.is_supplier_disabled("badsupplier.com", "9999"))
        self.assertTrue(bl.is_offer_disabled("9999_123", "Товар", "art123", "badsupplier.com", "9999"))
        
        # Good supplier should not be disabled
        self.assertFalse(bl.is_supplier_disabled("opt-drop.com", "2222"))
        self.assertFalse(bl.is_offer_disabled("2222_106619", "Павербанк", "sp10144", "opt-drop.com", "2222"))

    def test_blacklist_keyword_filtering(self):
        """Forbidden keywords must filter matching offers."""
        bl = Blacklist(10)
        bl.disabled_keywords.append(("*", "вейп"))
        bl.disabled_keywords.append(("dropt", "кальян"))

        # Global keyword
        self.assertTrue(bl.is_offer_disabled("1000_1", "Електронний Вейп 100W", "art1", "dropt.in.ua", "1000"))
        self.assertTrue(bl.is_offer_disabled("2222_2", "Потужний ВЕЙП набір", "art2", "opt-drop.com", "2222"))

        # Supplier specific keyword
        self.assertTrue(bl.is_offer_disabled("1000_3", "Класичний Кальян", "art3", "dropt.in.ua", "1000"))
        # Same keyword for different supplier is allowed
        self.assertFalse(bl.is_offer_disabled("2222_4", "Класичний Кальян", "art4", "opt-drop.com", "2222"))

    def test_blacklist_item_id_filtering(self):
        """Individual offer IDs or vendor codes in disabled_ids must be blocked."""
        bl = Blacklist(10)
        bl.disabled_ids.add("BLOCKED_OFFER_123")
        bl.disabled_ids.add("SP_BAD_ARTICLE")

        self.assertTrue(bl.is_offer_disabled("BLOCKED_OFFER_123", "Нормальний товар", "art_ok", "opt-drop.com", "2222"))
        self.assertTrue(bl.is_offer_disabled("2222_555", "Нормальний товар", "SP_BAD_ARTICLE", "opt-drop.com", "2222"))
        self.assertFalse(bl.is_offer_disabled("2222_777", "Нормальний товар", "ART_GOOD", "opt-drop.com", "2222"))

    def test_blacklist_category_filtering(self):
        """Disabled categories must be recognized."""
        bl = Blacklist(10)
        bl.disabled_categories.append(("dropt", "павербанки"))

        self.assertTrue(bl.is_category_disabled("101", "Павербанки та зарядки", "dropt.in.ua", "1000"))
        self.assertFalse(bl.is_category_disabled("102", "Чохли для телефонів", "dropt.in.ua", "1000"))

if __name__ == '__main__':
    unittest.main()
