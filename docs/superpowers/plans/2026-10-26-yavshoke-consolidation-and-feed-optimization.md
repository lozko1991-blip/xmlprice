# Yavshoke Feed Consolidation and XML Size Optimization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Consolidate three category feeds from `crm.yavshoke.ua` into a single master feed (`all_drop_opt_price.xml`), enforce tiered markup starting at 40%+50 UAH with a 400 UAH final price cutoff, remove rolety and tarelky (~21,800 items), prune leaf categories with under 10 products, truncate descriptions at sentence boundaries to under 700 chars, add neutral parameters ("Вид: Сучасний", "Колір: Комбінований", "Brand"), preserve `group_id`, and ensure 100% ID and category continuity (`2000_`).

**Architecture:**
1. **Source Update:** Consolidate 3 URLs into one in `SOURCES` with `cat_prefix="2000"`, `id_prefix="2000"`.
2. **Exclusion Filters:** Category IDs (`40863`, `42165`, `42166`, `15041`, `15055`) and keywords (`ролет`, `тарілк`, `тарелк`) in `blacklist.txt`.
3. **Markup & Cutoff:** Configure `markup_config.py` with `min_price_final = 400`, tiers: `(1000, 1.40, 50)`, `(3000, 1.35, 50)`, `(999999, 1.30, 50)`.
4. **Sentence Truncation:** Set `DESC_LIMIT = 700` and break cleanly at period/sentence boundary before closing tags.
5. **Parameters & Brand Hygiene:** Neutral fallback parameters (Стан, Колір, Вид: Сучасний, Вага, Розмір Size), vendor default `"Brand"`, and preserve `group_id`.
6. **Category Pruning:** Leaf categories with `< 10` offers for Yavshoke are purged and hidden.

**Tech Stack:** Python 3.10+, `lxml`, `unittest`, GitHub Actions.

## Global Constraints
- Supplier ID prefix `2000_` and category prefix `2000` must remain unchanged.
- Preserve existing pricing logic for other 17 suppliers in `markup_config.py`.
- Strict compliance with `FEED_PASSPORT_EVA.md`.
- All tests must pass cleanly (`python -m unittest discover tests`).

---

### Task 1: Blacklist Rules for Rolety and Tarelky

**Files:**
- Modify: `blacklist.txt`
- Modify: `tests/test_duplicate_and_blacklist_invariants.py`

**Interfaces:**
- Produces: `blacklist_obj.is_category_disabled`, `blacklist_obj.is_offer_disabled`

- [ ] **Step 1: Write failing test in `tests/test_duplicate_and_blacklist_invariants.py`**
  Verify that rolety categories (`40863`, `42165`, `42166`) and tarelky categories (`15041`, `15055`) and keywords (`ролет`, `тарілк`, `тарелк`) are disabled for `crm.yavshoke.ua`.

- [ ] **Step 2: Run test to verify it fails**
  Run: `python -m unittest tests/test_duplicate_and_blacklist_invariants.py`

- [ ] **Step 3: Update `blacklist.txt`**
  Add rolety and tarelky categories and keywords under `[DISABLED_CATEGORIES]` and `[DISABLED_KEYWORDS]`.

- [ ] **Step 4: Run test to verify it passes**
  Run: `python -m unittest tests/test_duplicate_and_blacklist_invariants.py`

- [ ] **Step 5: Commit changes**
  ```bash
  git add blacklist.txt tests/test_duplicate_and_blacklist_invariants.py
  git commit -m "feat: blacklist rolety and tarelky categories and keywords for yavshoke"
  ```

---

### Task 2: Markup Tiers & Minimum Price Cutoff in `markup_config.py`

**Files:**
- Modify: `markup_config.py`
- Modify: `tests/test_price_and_markup_safety.py`

**Interfaces:**
- Produces: `CUSTOM_MARKUP["crm.yavshoke.ua"]`

- [ ] **Step 1: Write failing test in `tests/test_price_and_markup_safety.py`**
  Verify that `min_price_final` is 400, and tiers reflect 40%+50 up to 1000, 35%+50 up to 3000, 30%+50 above 3000.

- [ ] **Step 2: Run test to verify it fails**
  Run: `python -m unittest tests/test_price_and_markup_safety.py`

- [ ] **Step 3: Update `markup_config.py`**
  Set `min_price_final: 400`, `markup_percent: 1.35`, `markup_fixed: 50`, `markup_tiers: [(1000, 1.40, 50), (3000, 1.35, 50), (999999, 1.30, 50)]`.

- [ ] **Step 4: Run test to verify it passes**
  Run: `python -m unittest tests/test_price_and_markup_safety.py`

- [ ] **Step 5: Commit changes**
  ```bash
  git add markup_config.py tests/test_price_and_markup_safety.py
  git commit -m "feat: configure yavshoke markup tiers starting at 40%+50 UAH and min_price_final=400"
  ```

---

### Task 3: Description Truncation at Complete Sentence Boundary

**Files:**
- Modify: `masterevaxml.py` (`clean_description`, `DESC_LIMIT`)
- Modify: `tests/test_xml_sanitization_and_cdata.py`

**Interfaces:**
- Produces: `clean_description(text, name_ua, vendor)` with sentence boundary period truncation within 700 chars.

- [ ] **Step 1: Write failing test in `tests/test_xml_sanitization_and_cdata.py`**
  Verify that a long description (> 700 chars) truncates at the last period within 700 chars, does not cut mid-sentence, and cleanly closes `<p>`.

- [ ] **Step 2: Run test to verify it fails**
  Run: `python -m unittest tests/test_xml_sanitization_and_cdata.py`

- [ ] **Step 3: Update `clean_description` in `masterevaxml.py`**
  Set `DESC_LIMIT = 700`. Implement sentence boundary search (`. `, `.</p>`, `!`, `?`) and clean closing of tags.

- [ ] **Step 4: Run test to verify it passes**
  Run: `python -m unittest tests/test_xml_sanitization_and_cdata.py`

- [ ] **Step 5: Commit changes**
  ```bash
  git add masterevaxml.py tests/test_xml_sanitization_and_cdata.py
  git commit -m "feat: truncate descriptions at sentence boundary under 700 chars"
  ```

---

### Task 4: Fallback Parameters, Brand Vendor & Group ID Preservation

**Files:**
- Modify: `masterevaxml.py`
- Modify: `tests/test_feed_passport_validator.py`

**Interfaces:**
- Produces: Neutral params (Стан, Колір, Вид: Сучасний, Вага, Розмір Size), vendor `"Brand"`, and `group_id` attribute.

- [ ] **Step 1: Write failing test in `tests/test_feed_passport_validator.py`**
  Verify that fallback parameters include `Вид: Сучасний`, missing color adds `Колір: Комбінований`, empty vendor defaults to `"Brand"`, and `group_id` is retained.

- [ ] **Step 2: Run test to verify it fails**
  Run: `python -m unittest tests/test_feed_passport_validator.py`

- [ ] **Step 3: Update `masterevaxml.py`**
  Implement `Вид: Сучасний` in neutral fallback params, add `Колір: Комбінований` if color missing, set default vendor to `"Brand"`, and retain `group_id`.

- [ ] **Step 4: Run test to verify it passes**
  Run: `python -m unittest tests/test_feed_passport_validator.py`

- [ ] **Step 5: Commit changes**
  ```bash
  git add masterevaxml.py tests/test_feed_passport_validator.py
  git commit -m "feat: add neutral param 'Вид: Сучасний', default vendor 'Brand', and retain group_id"
  ```

---

### Task 5: Leaf Category Pruning (< 10 offers for Yavshoke)

**Files:**
- Modify: `masterevaxml.py` (Step 6.5)
- Modify: `tests/test_category_integrity.py`

**Interfaces:**
- Produces: Category pruning for Yavshoke (`cat_id.startswith("2000")`) when direct offers `< 10` and category is not a parent.

- [ ] **Step 1: Write failing test in `tests/test_category_integrity.py`**
  Verify that leaf categories from Yavshoke with `< 10` products are pruned, while parent categories and other suppliers' categories remain protected.

- [ ] **Step 2: Run test to verify it fails**
  Run: `python -m unittest tests/test_category_integrity.py`

- [ ] **Step 3: Update Step 6.5 in `masterevaxml.py`**
  Implement Yavshoke-specific threshold `< 10` for leaf categories (`cat_id not in parent_ids`).

- [ ] **Step 4: Run test to verify it passes**
  Run: `python -m unittest tests/test_category_integrity.py`

- [ ] **Step 5: Commit changes**
  ```bash
  git add masterevaxml.py tests/test_category_integrity.py
  git commit -m "feat: prune leaf categories with under 10 offers for yavshoke"
  ```

---

### Task 6: Consolidate SOURCES to Single Master Feed & Update Docs

**Files:**
- Modify: `masterevaxml.py` (`SOURCES`)
- Modify: `SUPPLIERS_PASSPORT.md`
- Modify: `README.md`

**Interfaces:**
- Replaces 3 Yavshoke entries with `("2000", "2000", "https://crm.yavshoke.ua/media/export/all_drop_opt_price.xml")`.

- [ ] **Step 1: Update `SOURCES` in `masterevaxml.py`**
- [ ] **Step 2: Update `SUPPLIERS_PASSPORT.md` and `README.md`**
- [ ] **Step 3: Run all unit tests to confirm 100% pass**
  Run: `python -m unittest discover tests`

- [ ] **Step 4: Commit changes**
  ```bash
  git add masterevaxml.py SUPPLIERS_PASSPORT.md README.md
  git commit -m "feat: consolidate yavshoke feeds into single master feed all_drop_opt_price.xml"
  ```

---

### Task 7: Full Verification & Delivery
- [ ] Run full test suite: `python -m unittest discover tests`.
- [ ] Verify git status and diff.
- [ ] Push to `origin/main` and verify GitHub Actions.
