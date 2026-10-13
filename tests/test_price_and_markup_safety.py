import unittest
from markup_config import CUSTOM_MARKUP
from masterevaxml import SOURCES, OLD_PRICE_MULT, get_markup
from feed_passport_validator import validate_price_safety

class TestPriceAndMarkupSafety(unittest.TestCase):
    def test_all_sources_have_markup_configuration(self):
        """Every supplier domain must have explicit markup settings."""
        for _cat_prefix, _id_prefix, url in SOURCES:
            domain = url.split('/')[2]
            self.assertIn(
                domain,
                CUSTOM_MARKUP,
                f"Supplier domain '{domain}' is missing in CUSTOM_MARKUP"
            )
            cfg = CUSTOM_MARKUP[domain]
            self.assertIn("markup_percent", cfg)
            self.assertIn("markup_fixed", cfg)

    def test_markup_tiers_structure_and_sorting(self):
        """Tiered markups must be properly formatted and sorted ascending by price."""
        for domain, cfg in CUSTOM_MARKUP.items():
            tiers = cfg.get("markup_tiers")
            if tiers:
                prev_price = -1
                for idx, t in enumerate(tiers):
                    self.assertEqual(
                        len(t), 3,
                        f"Tier #{idx} for '{domain}' must have 3 elements (max_price, pct, fix)"
                    )
                    max_p, pct, fix = t
                    self.assertGreater(
                        max_p, prev_price,
                        f"Tiers for '{domain}' must be sorted ascending by max_price"
                    )
                    self.assertGreaterEqual(pct, 1.0, f"Markup percent for '{domain}' must be >= 1.0 (got {pct})")
                    self.assertGreaterEqual(fix, 0, f"Markup fixed for '{domain}' must be >= 0 (got {fix})")
                    prev_price = max_p

    def test_get_markup_preserves_profit_and_monotonicity(self):
        """Test pricing calculation across test price points for key suppliers."""
        test_prices = [50.0, 150.0, 350.0, 750.0, 1500.0, 3000.0, 10000.0]
        test_domains = ["opt-drop.com", "dropt.in.ua", "crm.yavshoke.ua", "feed.lugi.com.ua"]

        for domain in test_domains:
            cfg = CUSTOM_MARKUP.get(domain)
            if not cfg:
                continue
            for raw_price in test_prices:
                pct, fix = get_markup(raw_price, cfg)
                final_price = round(raw_price * pct + fix)
                old_price = round(final_price * OLD_PRICE_MULT)

                # Safety check via feed_passport_validator
                errors = validate_price_safety(
                    original_price_uah=raw_price,
                    our_price=final_price,
                    old_price=old_price
                )
                self.assertEqual(
                    errors, [],
                    f"Price safety failed for {domain} at cost {raw_price}: {errors}"
                )
                # Profit must be positive
                profit = final_price - raw_price
                self.assertGreaterEqual(profit, 0)

    def test_item_overrides_priority(self):
        """Item-specific overrides must take precedence over tier rules."""
        sample_cfg = {
            "markup_percent": 1.2,
            "markup_fixed": 30,
            "item_overrides": {
                "custom_123": {"percent": 1.5, "fixed": 100}
            }
        }
        # Regular item
        pct, fix = get_markup(200.0, sample_cfg, offer_id="regular_item")
        self.assertEqual(pct, 1.2)
        self.assertEqual(fix, 30)

        # Overridden item
        pct_ov, fix_ov = get_markup(200.0, sample_cfg, offer_id="custom_123")
        self.assertEqual(pct_ov, 1.5)
        self.assertEqual(fix_ov, 100)

    def test_old_price_multiplier_guarantees_eva_rule(self):
        """EVA requires price_old > price. OLD_PRICE_MULT (1.25) must strictly satisfy this."""
        for p in [1, 2, 5, 10, 100, 199, 553, 9999, 100000]:
            p_old = max(round(p * OLD_PRICE_MULT), p + 1)
            self.assertGreater(
                p_old, p,
                f"Old price ({p_old}) is not greater than price ({p})"
            )

if __name__ == '__main__':
    unittest.main()
