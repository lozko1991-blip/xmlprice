# Design Specification: Yavshoke Feed Consolidation & MasterEva Feed Size Optimization

**Date:** 2026-10-26  
**Status:** Approved by User  
**Target Marketplace:** EVA.UA (`Masterevanew.xml`)

---

## 1. Overview & Business Goals
The goal is to replace three separate category-specific feeds from supplier `crm.yavshoke.ua` with one comprehensive master feed (`https://crm.yavshoke.ua/media/export/all_drop_opt_price.xml`), optimize the overall XML feed size to satisfy EVA's strict limits, eliminate unwanted and low-margin products, and maintain 100% ID and URL continuity for previously moderated offers.

---

## 2. Invariants & Preservation Rules
1. **Offer ID Invariance:**
   - Supplier `crm.yavshoke.ua` retains ID prefix `"2000"` (`id_prefix = "2000"`).
   - Formatted Offer ID: `2000_{raw_id}` (e.g. `2000_4918401`).
   - All offers previously moderated on EVA from the three old feeds retain their exact IDs.
2. **Category ID Invariance:**
   - Category prefix remains `"2000"` (`cat_prefix = "2000"`).
   - Formatted Category ID: `2000{orig_id}` (e.g. `200017848`).
3. **Availability Invariant:**
   - Only in-stock items (`available="true"`) enter the XML feed. Out-of-stock items are strictly dropped.
4. **Single-Item Retention Deduplication:**
   - Handled via `seen_offer_ids`: if duplicate `offer_id` occurs across the feed, exactly one (first) is kept.

---

## 3. Feed Source Changes
In `masterevaxml.py` within `SOURCES`:
- **Remove:**
  ```python
  ("2000", "2000", "https://crm.yavshoke.ua/media/export/bt_opt_price.xml"),
  ("2000", "2000", "https://crm.yavshoke.ua/media/export/posuda_opt_price.xml"),
  ("2000", "2000", "https://crm.yavshoke.ua/media/export/top_aliexpress_opt_price.xml"),
  ```
- **Add:**
  ```python
  ("2000", "2000", "https://crm.yavshoke.ua/media/export/all_drop_opt_price.xml"),
  ```

---

## 4. Product Filtering & Feed Size Optimization

### 4.1. Unwanted Categories & Keywords Removal (Rolety & Plates)
In `blacklist.txt`:
- Under `[DISABLED_CATEGORIES]`:
  - `crm.yavshoke.ua: 40863` (Ролеты)
  - `crm.yavshoke.ua: 42165` (Ролеты открытого типа — 10,260 items)
  - `crm.yavshoke.ua: 42166` (Ролеты закрытого типа — 10,260 items)
  - `crm.yavshoke.ua: 15041` (Тарелки и пиалы — 1,253 items)
  - `crm.yavshoke.ua: 15055` (Детские тарелки и пиалы — 16 items)
- Under `[DISABLED_KEYWORDS]`:
  - `crm.yavshoke.ua: ролет`
  - `crm.yavshoke.ua: тарілк`
  - `crm.yavshoke.ua: тарелк`
*Total items filtered: ~21,800 items.*

### 4.2. Pricing Cutoff (Minimum 400 UAH Final Price)
In `markup_config.py` for `"crm.yavshoke.ua"`:
- Set `"min_price_final": 400`.
- Items with calculated final price below 400 UAH are excluded.
*Total items filtered: ~17,800 items.*

### 4.3. Child Category Pruning (< 10 Products for Yavshoke)
In Step 6.5 of `masterevaxml.py`:
- Identify leaf categories (categories where `cat_id not in parent_ids`).
- For categories belonging to `crm.yavshoke.ua` (`cat_id.startswith("2000")`), if valid product count is `< 10`:
  - Remove all products in that category.
  - Hide/remove the category from `<categories>` so it does not appear in the XML.
- For other suppliers, retain default threshold `cat_count <= MIN_OFFERS_PER_CATEGORY` (3).

### 4.4. Description Sentence-Boundary Truncation (Global)
In `clean_description(text, name_ua, vendor)` in `masterevaxml.py`:
- Set `DESC_LIMIT = 700`.
- Truncate at the last complete sentence boundary (`.`, `!`, `?`, or `.</p>`) within the 700-character budget (between 100 and 700 chars).
- Cleanly close any unclosed `<p>` tag (`</p>`).
- If no sentence delimiter is found, safely truncate at the nearest space/HTML boundary and append complete closing tag.
*Reduces overall MasterEva XML payload size by ~40%.*

---

## 5. Pricing & Tiered Markup Configuration
In `markup_config.py` under `"crm.yavshoke.ua"`:
```python
"crm.yavshoke.ua": {
    "min_price_final": 400,
    "min_profit_uah":  100,
    "markup_percent":  1.35,
    "markup_fixed":    50,
    "markup_tiers": [
        (1000,   1.40,  50),   # Up to 1000 UAH: 40% + 50 UAH
        (3000,   1.35,  50),   # 1000 to 3000 UAH: 35% + 50 UAH
        (999999, 1.30,  50),   # Above 3000 UAH: 30% + 50 UAH
    ],
}
```
- Multiplier for old price: `OLD_PRICE_MULT = 1.25` ($\text{price\_old} = \max(\text{round}(\text{price} \times 1.25), \text{price} + 1)$).

---

## 6. Product Parameters & Data Hygiene for EVA
1. **Grouped Products (`group_id`):**
   - Each variant retains its unique `id` and is output as a standalone valid `<offer id="2000_{id}">` (matching EVA requirement: every modification has a unique offer id).
   - If the raw offer contains `group_id`, attribute `group_id="2000_{raw_group_id}"` is preserved on `<offer>` for marketplace variant grouping.
2. **Default Vendor / Brand:**
   - If vendor is empty, missing, or `"NoBrand"`, set vendor to `"Brand"`.
   - Ensure brand is included in `name_ua` if not already present.
3. **Neutral Fallback Characteristics (when product has no params):**
   - When an offer has no characteristics from the supplier, populate the neutral set:
     * `<param name="Стан">Новий</param>`
     * `<param name="Колір">Комбінований</param>`
     * `<param name="Вид">Сучасний</param>`
     * `<param name="Вага">-</param>`
     * `<param name="Розмір Size">-</param>`
4. **Color Parameter for Existing Params:**
   - If an offer has parameters but lacks `"Колір"` (or `"Цвет"`), append `<param name="Колір">Комбінований</param>`.

---

## 7. Verification & Safety Testing
- All unit tests in `tests/` must pass (`python -m unittest discover tests`).
- New TDD test coverage for:
  - Truncation at complete sentence boundary within 700 chars.
  - Filtering leaf categories with `< 10` items for Yavshoke while leaving parents intact.
  - Adding `"Вид: Сучасний"`, `"Колір: Комбінований"`, and `"Brand"` fallbacks.
  - Price cutoff at 400 UAH.
  - Blacklist exclusion of rolety and tarelky categories and keywords.
