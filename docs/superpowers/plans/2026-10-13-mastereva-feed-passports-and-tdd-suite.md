# MasterEva Feed Technical Passports and Comprehensive TDD Test Suite Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Establish technical passports (data contracts) for EVA Marketplace and all 18 suppliers, develop a feed passport validation engine (`feed_passport_validator.py`), build a comprehensive TDD test suite covering all EVA rules, prices, XML invariants, and duplicates, and wire the test verification gate into GitHub Actions to permanently eliminate errors upon any future code edits.

**Architecture:**
1. **Technical Passports Documentation**: Create `FEED_PASSPORT_EVA.md` and `SUPPLIERS_PASSPORT.md` formally declaring all schema contracts, invariants, tag restrictions, price constraints, and supplier rules.
2. **Passport Validator Engine (`feed_passport_validator.py`)**: Reusable module to validate offer elements, XML structure, price safety, and CDATA/ASCII hygiene against EVA standards.
3. **Comprehensive TDD Test Suite (`tests/`)**:
   - `tests/test_feed_passport_validator.py`: validates all mandatory tags, lengths, formats, and EVA constraints.
   - `tests/test_price_and_markup_safety.py`: verifies markup invariants (`price >= original - 1`, `price_old > price`, boundary conditions).
   - `tests/test_xml_sanitization_and_cdata.py`: verifies UTF-8 compliance, ASCII 0-31 removal, CDATA wrapping, contact phone stripping.
   - `tests/test_category_integrity.py`: verifies offer-to-category foreign keys and category 99999 fallback.
   - `tests/test_blacklist_enforcement.py`: verifies supplier, category, keyword, and stop-item filtering.
   - `tests/test_duplicate_and_clone_invariants.py`: verifies offer ID uniqueness, single-item retention, and clone isolation.
4. **CI/CD Integration**: Add automated test execution step (`python -m unittest discover tests`) to `.github/workflows/mastereva.yml` as a deployment gate.

**Tech Stack:** Python 3.10+, `lxml`, `unittest`, GitHub Actions.

## Global Constraints
- Preserve existing pricing and markup rules in `markup_config.py` untouched.
- Comply strictly with EVA Marketplace specifications from `вимоги до прайсів XML EVA маркетплейс.md`.
- No loss of backward compatibility for existing suppliers or overrides.
- All tests must pass cleanly (`python -m unittest discover tests`).

---

### Task 1: Create Technical Passport for EVA Marketplace (`FEED_PASSPORT_EVA.md`)

**Files:**
- Create: `FEED_PASSPORT_EVA.md`

**Interfaces:**
- Produces: Formal technical contract specifying every XML element, required attributes, validation ranges, error codes, and strict EVA invariants.

- [ ] **Step 1: Write `FEED_PASSPORT_EVA.md`**
  Cover root elements (`yml_catalog`, `shop`, `currencies`, `categories`, `offers`), offer mandatory fields (`id`, `available`, `stock_quantity`, `price`, `currencyId`, `categoryId`, `picture`, `vendor`, `name_ua`, `description_ua`, `param`), restrictions, and validation tolerances.

- [ ] **Step 2: Verify file readability and completeness**
  Ensure all rules from `вимоги до прайсів XML EVA маркетплейс.md` are accurately codified into the passport.

- [ ] **Step 3: Commit `FEED_PASSPORT_EVA.md`**
  ```bash
  git add FEED_PASSPORT_EVA.md
  git commit -m "docs: create EVA Marketplace technical feed passport (FEED_PASSPORT_EVA.md)"
  ```

---

### Task 2: Create Technical Passport for All 18 Suppliers (`SUPPLIERS_PASSPORT.md`)

**Files:**
- Create: `SUPPLIERS_PASSPORT.md`

**Interfaces:**
- Produces: Detailed registry of all 18 suppliers (domains, ID prefixes, stock policies, pricing currencies, Shkatulka-Dropt sync rules, deduplication policies).

- [ ] **Step 1: Write `SUPPLIERS_PASSPORT.md`**
  Document prefixes (`1000_`, `2000_`, `2222_`, `3333_`, `4444_`, `5555_`, `6666_`, `7777_`, `8888_`, `9999_`, `dropt_`, `shkatulka_`, `aveon_`, `sonechko_`, `dropship_`), categories handling, and specific supplier quirks.

- [ ] **Step 2: Commit `SUPPLIERS_PASSPORT.md`**
  ```bash
  git add SUPPLIERS_PASSPORT.md
  git commit -m "docs: create suppliers technical passport (SUPPLIERS_PASSPORT.md)"
  ```

---

### Task 3: Build Passport Validator Engine (`feed_passport_validator.py`) via TDD

**Files:**
- Create: `feed_passport_validator.py`
- Create: `tests/test_feed_passport_validator.py`

**Interfaces:**
- Produces:
  - `validate_offer_element(offer_el, valid_categories=None) -> list[str]`
  - `validate_feed_structure(root_el) -> list[str]`
  - `validate_price_safety(original_price_uah, our_price, old_price=None) -> list[str]`
  - `clean_ascii_control_chars(text: str) -> str`
  - `has_ascii_control_chars(text: str) -> bool`

- [ ] **Step 1: Write failing tests in `tests/test_feed_passport_validator.py`**
  Test missing required tags (`name_ua`, `vendor`, `stock_quantity`, `description_ua`), invalid `price_old <= price`, invalid photo URLs, length violations, and ASCII control characters.

- [ ] **Step 2: Run test to verify it fails**
  Run: `python -m unittest tests/test_feed_passport_validator.py`
  Expected: FAIL (module `feed_passport_validator` does not exist).

- [ ] **Step 3: Implement `feed_passport_validator.py`**
  Write minimal implementation satisfying all validator assertions based on `FEED_PASSPORT_EVA.md`.

- [ ] **Step 4: Run test to verify it passes**
  Run: `python -m unittest tests/test_feed_passport_validator.py`
  Expected: PASS.

- [ ] **Step 5: Commit**
  ```bash
  git add feed_passport_validator.py tests/test_feed_passport_validator.py
  git commit -m "feat: add feed passport validator engine with TDD tests"
  ```

---

### Task 4: Add TDD Tests for Price and Markup Safety (`tests/test_price_and_markup_safety.py`)

**Files:**
- Create: `tests/test_price_and_markup_safety.py`

**Interfaces:**
- Consumes: `markup_config.get_markup`, `feed_passport_validator.validate_price_safety`
- Verifies: Price safety invariants across various price tiers, zero/negative inputs, currency conversion checks, and promo pricing.

- [ ] **Step 1: Write test cases**
  - Invariant 1: `our_price >= original_price_uah - 1` for all price tiers.
  - Invariant 2: `price_old > price` whenever `price_old` is calculated.
  - Invariant 3: Zero or negative prices handled safely without throwing uncaught exceptions.
  - Invariant 4: Tier transitions (e.g. at 100 грн, 300 грн, 1000 грн, 5000 грн) yield monotonic and positive margins.

- [ ] **Step 2: Run tests**
  Run: `python -m unittest tests/test_price_and_markup_safety.py`
  Expected: PASS.

- [ ] **Step 3: Commit**
  ```bash
  git add tests/test_price_and_markup_safety.py
  git commit -m "test: add comprehensive price and markup safety TDD tests"
  ```

---

### Task 5: Add TDD Tests for XML Sanitization & CDATA Hygiene (`tests/test_xml_sanitization_and_cdata.py`)

**Files:**
- Create: `tests/test_xml_sanitization_and_cdata.py`

**Interfaces:**
- Consumes: `masterevaxml.clean_description`, `masterevaxml.fix_text`, `masterevaxml.ru_to_ua`
- Verifies:
  - Exclusion of non-printable ASCII characters 0-31 (except 9, 10, 13).
  - CDATA enclosure for HTML descriptions.
  - Removal of supplier phone numbers and links from descriptions.
  - Length constraint enforcement (30..60000 chars for descriptions).

- [ ] **Step 1: Write test cases in `tests/test_xml_sanitization_and_cdata.py`**
- [ ] **Step 2: Run tests**
  Run: `python -m unittest tests/test_xml_sanitization_and_cdata.py`
  Expected: PASS.

- [ ] **Step 3: Commit**
  ```bash
  git add tests/test_xml_sanitization_and_cdata.py
  git commit -m "test: add XML sanitization, CDATA, and description hygiene TDD tests"
  ```

---

### Task 6: Add TDD Tests for Category Integrity (`tests/test_category_integrity.py`)

**Files:**
- Create: `tests/test_category_integrity.py`

**Interfaces:**
- Consumes: `masterevaxml` category resolution logic
- Verifies:
  - Every offer's `categoryId` exists in the category mapping table.
  - Missing/unmapped categories properly divert to the fallback category `Інші товари [Постачальник]` (`PREFIX99999`).
  - Fallback category is created and present in `<categories>`.

- [ ] **Step 1: Write test cases in `tests/test_category_integrity.py`**
- [ ] **Step 2: Run tests**
  Run: `python -m unittest tests/test_category_integrity.py`
  Expected: PASS.

- [ ] **Step 3: Commit**
  ```bash
  git add tests/test_category_integrity.py
  git commit -m "test: add category integrity and fallback category TDD tests"
  ```

---

### Task 7: Add TDD Tests for Duplicate Removal and Blacklist Filtering (`tests/test_duplicate_and_blacklist_invariants.py`)

**Files:**
- Create: `tests/test_duplicate_and_blacklist_invariants.py`

**Interfaces:**
- Consumes: `masterevaxml.load_blacklist`, `masterevaxml.Blacklist`
- Verifies:
  - Single-item retention: duplicates within a supplier keep exactly the first item and drop subsequent duplicates.
  - Disabled suppliers, categories, keywords, and individual offer IDs / vendor codes in `blacklist.txt` are strictly filtered out.
  - Cloned items have strictly unique IDs from their base offer.

- [ ] **Step 1: Write test cases in `tests/test_duplicate_and_blacklist_invariants.py`**
- [ ] **Step 2: Run tests**
  Run: `python -m unittest tests/test_duplicate_and_blacklist_invariants.py`
  Expected: PASS.

- [ ] **Step 3: Commit**
  ```bash
  git add tests/test_duplicate_and_blacklist_invariants.py
  git commit -m "test: add duplicate single-item retention and blacklist TDD tests"
  ```

---

### Task 8: Integrate Test Gate into GitHub Actions Workflow & Master Script

**Files:**
- Modify: `.github/workflows/mastereva.yml`
- Modify: `README.md`

**Interfaces:**
- Workflow step: `python -m unittest discover tests` executed before running `masterevaxml.py`.
- If any test fails, CI halts immediately and prevents deployment of corrupt feeds.

- [ ] **Step 1: Update `.github/workflows/mastereva.yml` to include test execution step**
- [ ] **Step 2: Update `README.md` with passport and test documentation**
- [ ] **Step 3: Run full test suite locally to verify 100% green execution**
  Run: `python -m unittest discover tests`
  Expected: PASS with 0 failures across all test suites.

- [ ] **Step 4: Commit and push changes**
  ```bash
  git add .github/workflows/mastereva.yml README.md
  git commit -m "ci: add automated TDD test verification gate to GitHub Actions workflow"
  ```

---

### Task 9: Full Verification and Delivery
- [ ] Run all tests and verify all pass.
- [ ] Push to `origin/main` and verify GitHub Actions executes the test suite before generating feed.
- [ ] Provide user with comprehensive summary of passports, tests, and CI/CD protection.
