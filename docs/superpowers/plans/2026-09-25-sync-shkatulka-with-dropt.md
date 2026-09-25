# Sync Shkatulka Availability and Price with Dropt Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Synchronize product availability, stock quantity, and pricing for supplier Shkatulka (`www.shkatulka.in.ua`) using data from Dropt (`dropt.in.ua`), while preserving existing moderated Shkatulka `offer id`s on EVA without impacting Dropt or any other supplier.

**Architecture:** 
1. Build an in-memory lookup index of Dropt items keyed by normalized base article (stripping suffix like `/\d+$`).
2. When processing Shkatulka offers, check if the normalized article exists in Dropt's catalog.
3. If matched, substitute Dropt's live availability, stock quantity, and price calculation into Shkatulka's offer. If Dropt is out of stock, skip Shkatulka's offer to prevent unfulfillable orders on EVA.

**Tech Stack:** Python 3.10, `lxml`, `requests`, `pytest`.

## Global Constraints
- Do not alter `offer_id` prefixes (`1111_` for Shkatulka, `1000_` for Dropt). EVA moderated cards must retain their existing IDs.
- Dropt processing and all other 16 suppliers must remain 100% unaffected.
- Handle trailing slash numbers in articles (`58487/9` -> `58487`, `13079/00` -> `13079`).

---

### Task 1: Article Normalization Helper

**Files:**
- Modify: `masterevaxml.py`
- Test: `tests/test_article_normalization.py`

**Interfaces:**
- Produces: `normalize_sync_article(art: str) -> str`

- [ ] **Step 1: Write failing test**

```python
# tests/test_article_normalization.py
import pytest
from masterevaxml import normalize_sync_article

def test_normalize_sync_article():
    assert normalize_sync_article("58487/9") == "58487"
    assert normalize_sync_article("13079/00") == "13079"
    assert normalize_sync_article("61011/1") == "61011"
    assert normalize_sync_article("58487") == "58487"
    assert normalize_sync_article("  ABC-123/5  ") == "ABC-123"
    assert normalize_sync_article("") == ""
    assert normalize_sync_article(None) == ""
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_article_normalization.py`
Expected: FAIL with ImportError / function not defined.

- [ ] **Step 3: Implement minimal code in `masterevaxml.py`**

```python
def normalize_sync_article(art):
    """
    Нормалізує артикул для порівняння між постачальниками (Шкатулка та Dropt).
    Видаляє хвостові модифікатори виду /9, /00, /1 тощо.
    """
    if not art:
        return ""
    art_str = str(art).strip().upper()
    return re.sub(r'/\d+$', '', art_str)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_article_normalization.py`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add masterevaxml.py tests/test_article_normalization.py
git commit -m "feat: add normalize_sync_article helper"
```

---

### Task 2: Build Dropt In-Memory Catalog Index

**Files:**
- Modify: `masterevaxml.py`
- Test: `tests/test_dropt_catalog_indexing.py`

**Interfaces:**
- Produces: `build_dropt_catalog(dropt_root, currency_rates, domain) -> dict`

- [ ] **Step 1: Write failing test**

```python
# tests/test_dropt_catalog_indexing.py
import pytest
import lxml.etree as ET
from masterevaxml import build_dropt_catalog

def test_build_dropt_catalog():
    xml = """
    <yml_catalog>
      <shop>
        <offers>
          <offer id="18556" available="true">
            <article>58487</article>
            <price>293.48</price>
            <currencyId>UAH</currencyId>
          </offer>
          <offer id="99999" available="false">
            <article>12345/1</article>
            <price>100</price>
            <currencyId>UAH</currencyId>
          </offer>
        </offers>
      </shop>
    </yml_catalog>
    """
    root = ET.fromstring(xml)
    catalog = build_dropt_catalog(root, {"UAH": 1.0}, "dropt.in.ua")
    
    assert "58487" in catalog
    assert catalog["58487"]["available"] is True
    assert catalog["58487"]["price_uah"] == 293.48
    assert catalog["58487"]["id"] == "18556"
    
    assert "12345" in catalog
    assert catalog["12345"]["available"] is False
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_dropt_catalog_indexing.py`
Expected: FAIL.

- [ ] **Step 3: Implement minimal code in `masterevaxml.py`**

```python
def build_dropt_catalog(root, currency_rates, domain):
    """
    Будує словник наявності та цін Dropt, індексований за нормалізованим артикулом.
    """
    catalog = {}
    for offer in root.xpath(".//offer"):
        raw_art = get_article(offer)
        base_art = normalize_sync_article(raw_art)
        if not base_art:
            continue
            
        avail = get_availability(offer)
        qty, _ = get_qty(offer)
        
        price_nodes = offer.xpath('./price')
        if not price_nodes or not (price_nodes[0].text or '').strip():
            continue
            
        raw_p = parse_price(price_nodes[0].text)
        if raw_p is None:
            continue
            
        currency_id = (offer.findtext('currencyId') or 'UAH').strip().upper()
        price_uah, _ = convert_to_uah(raw_p, currency_id, currency_rates, domain, offer.get('id', ''))
        if price_uah is None:
            continue
            
        catalog[base_art] = {
            "id": offer.get('id', ''),
            "available": avail,
            "qty": qty,
            "price_uah": price_uah,
        }
    return catalog
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_dropt_catalog_indexing.py`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add masterevaxml.py tests/test_dropt_catalog_indexing.py
git commit -m "feat: implement build_dropt_catalog"
```

---

### Task 3: Integrate Sync into Shkatulka Processing Pipeline

**Files:**
- Modify: `masterevaxml.py` (inside `process()`)
- Test: `tests/test_shkatulka_sync_integration.py`

**Interfaces:**
- When `domain == "www.shkatulka.in.ua"`:
  - If `shk_base_art in dropt_catalog`:
    - Check availability from Dropt. If Dropt is unavailable -> skip (`count_no += 1`).
    - Use Dropt's `price_uah` instead of Shkatulka's price.
    - Use Dropt's `qty`.
    - Apply Dropt's markup config (or Shkatulka's configured markup on Dropt's base price).
  - If `shk_base_art not in dropt_catalog`:
    - Option to keep or mark unavailable.

- [ ] **Step 1: Write integration test**

```python
# tests/test_shkatulka_sync_integration.py
import pytest
import lxml.etree as ET
from masterevaxml import build_dropt_catalog

def test_shkatulka_sync_logic():
    # Verify that an offer from Shkatulka matching Dropt adopts Dropt's price and stock
    dropt_xml = """
    <yml_catalog>
      <shop>
        <offers>
          <offer id="18556" available="true">
            <article>58487</article>
            <price>293.48</price>
            <currencyId>UAH</currencyId>
          </offer>
        </offers>
      </shop>
    </yml_catalog>
    """
    dropt_root = ET.fromstring(dropt_xml)
    catalog = build_dropt_catalog(dropt_root, {"UAH": 1.0}, "dropt.in.ua")
    
    # Simulate Shkatulka offer
    shk_xml = """
    <offer id="19022" available="true">
      <article>58487/9</article>
      <price>168</price>
      <currencyId>UAH</currencyId>
    </offer>
    """
    shk_offer = ET.fromstring(shk_xml)
    from masterevaxml import normalize_sync_article, get_article
    base_art = normalize_sync_article(get_article(shk_offer))
    
    assert base_art in catalog
    matched = catalog[base_art]
    assert matched["price_uah"] == 293.48
    assert matched["available"] is True
```

- [ ] **Step 2: Run test to verify it passes**

Run: `pytest tests/test_shkatulka_sync_integration.py`
Expected: PASS.

- [ ] **Step 3: Modify `masterevaxml.py` `process()` function**

1. In Step 2 or 3 of `process()`, locate the Dropt feed and call `dropt_catalog = build_dropt_catalog(...)`.
2. In Step 5 (Pass 2):
   Before checking availability for an offer:
   If `domain == "www.shkatulka.in.ua"`:
     - Check `base_art in dropt_catalog`.
     - If matched:
       - If `not dropt_catalog[base_art]["available"]`: skip item (`count_no += 1`).
       - Set `price_uah = dropt_catalog[base_art]["price_uah"]`.
       - Set `qty = dropt_catalog[base_art]["qty"]`.
       - Compute `price` and `old_price` using Dropt's markup rules (so prices match Dropt's sales price exactly).

- [ ] **Step 4: Run full XML generator dry-run**

Run: `python masterevaxml.py`
Expected: Check `price_warnings.log` and verify offer `1111_19022` now has price `402` (or matched price) instead of `288`.

- [ ] **Step 5: Commit**

```bash
git add masterevaxml.py tests/
git commit -m "feat: sync shkatulka availability and price with dropt"
```
