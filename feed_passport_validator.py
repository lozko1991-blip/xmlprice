"""
feed_passport_validator.py
Двигун технічної паспортизації та валідації XML-прайсів для маркетплейсу EVA.UA.
Перевіряє відповідність товарів, структури та цін вимогам FEED_PASSPORT_EVA.md.
"""

import re
import urllib.parse
from typing import List, Set, Optional

# Регулярний вираз для пошуку недрукованих ASCII символів (0-31),
# за винятком 9 (табуляція), 10 (перенесення рядка), 13 (повернення каретки).
RE_ASCII_CTRL = re.compile(r'[\x00-\x08\x0b\x0c\x0e-\x1f]')

# Регулярний вираз для перевірки кирилиці у URL
RE_CYRILLIC = re.compile(r'[\u0400-\u04FF]')


def has_ascii_control_chars(text: str) -> bool:
    """Повертає True, якщо текст містить неприпустимі ASCII керуючі символи."""
    if not text:
        return False
    return bool(RE_ASCII_CTRL.search(text))


def clean_ascii_control_chars(text: str) -> str:
    """Видаляє неприпустимі ASCII керуючі символи з тексту."""
    if not text:
        return ""
    return RE_ASCII_CTRL.sub('', text)


def validate_price_safety(original_price_uah: float, our_price: float, old_price: Optional[float] = None) -> List[str]:
    """
    Валідує цінову безпеку:
    - our_price > 0
    - our_price >= original_price_uah - 1 (захист від збитку)
    - old_price > our_price (якщо задана)
    """
    errors = []
    if our_price <= 0:
        errors.append(f"ERR_PRICE_INVALID: price must be > 0 (got {our_price})")

    if our_price < original_price_uah - 1:
        errors.append(
            f"ERR_PRICE_BELOW_COST: our_price ({our_price}) is lower than cost ({original_price_uah:.2f} UAH) - 1"
        )

    if old_price is not None:
        if old_price <= our_price:
            errors.append(
                f"ERR_OLD_PRICE_LE_PRICE: old_price ({old_price}) must be strictly greater than our_price ({our_price})"
            )

    return errors


def validate_offer_element(offer_el, valid_categories: Optional[Set[str]] = None) -> List[str]:
    """
    Валідує окремий елемент <offer> на відповідність FEED_PASSPORT_EVA.md.
    Повертає список помилок (порожній, якщо товар повністю валідний).
    """
    errors = []

    # 1. ID
    offer_id = offer_el.get("id")
    if not offer_id or not str(offer_id).strip():
        errors.append("ERR_OFFER_NO_ID: offer missing 'id' attribute")
    elif has_ascii_control_chars(offer_id):
        errors.append(f"ERR_XML_ASCII_CTRL: offer id '{offer_id}' contains control characters")

    # 2. Available
    avail = offer_el.get("available")
    if avail not in ("true", "false", True, False):
        errors.append(f"ERR_OFFER_AVAILABILITY: invalid available attribute '{avail}'")

    # 3. Price
    price_text = offer_el.findtext("price")
    if not price_text:
        errors.append("ERR_PRICE_MISSING: <price> tag missing or empty")
    else:
        try:
            price_val = float(price_text.replace(',', '.'))
            if price_val <= 0:
                errors.append(f"ERR_PRICE_INVALID: price <= 0 ({price_val})")
        except ValueError:
            errors.append(f"ERR_PRICE_INVALID: unable to parse price '{price_text}'")

    # 4. Old Price
    old_price_text = offer_el.findtext("price_old") or offer_el.findtext("old_price")
    if old_price_text and price_text:
        try:
            p_val = float(price_text.replace(',', '.'))
            op_val = float(old_price_text.replace(',', '.'))
            if op_val <= p_val:
                errors.append(f"ERR_OLD_PRICE_LE_PRICE: price_old ({op_val}) <= price ({p_val})")
        except ValueError:
            errors.append(f"ERR_OLD_PRICE_INVALID: unable to parse old price '{old_price_text}'")

    # 5. Currency
    curr = offer_el.findtext("currencyId")
    if not curr or curr.strip() != "UAH":
        errors.append(f"ERR_CURRENCY_INVALID: currencyId must be 'UAH' (got '{curr}')")

    # 6. Stock Quantity
    stock_text = offer_el.findtext("stock_quantity") or offer_el.findtext("quantity_in_stock")
    if stock_text is None:
        errors.append("ERR_STOCK_QUANTITY_MISSING: <stock_quantity> tag missing")
    else:
        try:
            stock_val = int(stock_text)
            if stock_val < 0:
                errors.append(f"ERR_STOCK_QUANTITY_INVALID: stock_quantity < 0 ({stock_val})")
        except ValueError:
            errors.append(f"ERR_STOCK_QUANTITY_INVALID: stock_quantity not an integer '{stock_text}'")

    # 7. CategoryId
    cat_id = offer_el.findtext("categoryId")
    if not cat_id or not cat_id.strip():
        errors.append("ERR_CATEGORY_MISSING: <categoryId> tag missing or empty")
    elif valid_categories is not None and cat_id.strip() not in valid_categories:
        errors.append(f"ERR_CATEGORY_UNRESOLVED: categoryId '{cat_id}' not found in registered categories")

    # 8. Vendor
    vendor = offer_el.findtext("vendor")
    if not vendor or not vendor.strip():
        errors.append("ERR_VENDOR_MISSING: <vendor> tag missing or empty")
    elif has_ascii_control_chars(vendor):
        errors.append(f"ERR_XML_ASCII_CTRL: vendor '{vendor}' contains control characters")

    # 9. Name_UA
    name_ua = offer_el.findtext("name_ua")
    if not name_ua or not name_ua.strip():
        errors.append("ERR_NAME_UA_MISSING: <name_ua> tag missing or empty")
    else:
        name_clean = name_ua.strip()
        if len(name_clean) < 3 or len(name_clean) > 255:
            errors.append(f"ERR_NAME_UA_LENGTH: name_ua length must be 3-255 characters (got {len(name_clean)})")
        if has_ascii_control_chars(name_clean):
            errors.append("ERR_XML_ASCII_CTRL: name_ua contains control characters")

    # 10. Description_UA
    desc_ua = offer_el.findtext("description_ua")
    if not desc_ua or not desc_ua.strip():
        errors.append("ERR_DESC_UA_MISSING: <description_ua> tag missing or empty")
    else:
        desc_clean = desc_ua.strip()
        if len(desc_clean) < 30:
            errors.append(f"ERR_DESC_UA_TOO_SHORT: description_ua must be >= 30 chars (got {len(desc_clean)})")
        elif len(desc_clean) > 60000:
            errors.append(f"ERR_DESC_UA_TOO_LONG: description_ua must be <= 60000 chars (got {len(desc_clean)})")
        if has_ascii_control_chars(desc_clean):
            errors.append("ERR_XML_ASCII_CTRL: description_ua contains control characters")

    # 11. Pictures
    pictures = offer_el.findall("picture")
    if not pictures:
        errors.append("ERR_PICTURE_EMPTY: at least 1 <picture> is required")
    elif len(pictures) > 15:
        errors.append(f"ERR_PICTURE_LIMIT_EXCEEDED: max 15 pictures allowed (got {len(pictures)})")
    else:
        for idx, pic in enumerate(pictures):
            url = (pic.text or '').strip()
            if not url or not url.startswith(('http://', 'https://')):
                errors.append(f"ERR_PICTURE_URL_INVALID: picture[{idx}] has invalid URL '{url}'")
            elif ' ' in url or RE_CYRILLIC.search(url):
                errors.append(f"ERR_PICTURE_URL_INVALID: picture[{idx}] contains spaces or cyrillic '{url}'")

    # 12. Params
    params = offer_el.findall("param")
    if not params:
        errors.append("ERR_PARAM_EMPTY: offer must have at least 1 <param>")
    else:
        for p in params:
            p_name = p.get("name")
            p_val = (p.text or '').strip()
            if not p_name or not p_name.strip():
                errors.append("ERR_PARAM_INVALID: param missing 'name' attribute")
            elif len(p_name) > 255:
                errors.append(f"ERR_PARAM_INVALID: param name exceeds 255 chars ('{p_name[:30]}...')")
            if not p_val:
                errors.append(f"ERR_PARAM_INVALID: param '{p_name}' has empty value")
            elif len(p_val) > 255:
                errors.append(f"ERR_PARAM_INVALID: param '{p_name}' value exceeds 255 chars")

    return errors


def validate_feed_structure(root_el) -> List[str]:
    """
    Валідує кореневу структуру фіду YML:
    <yml_catalog date="...">
      <shop>
        <currencies>...
        <categories>...
        <offers>...
    """
    errors = []
    if root_el.tag != "yml_catalog":
        errors.append(f"ERR_ROOT_TAG: expected <yml_catalog>, got <{root_el.tag}>")
        return errors

    date_attr = root_el.get("date")
    if not date_attr:
        errors.append("ERR_CATALOG_DATE_MISSING: yml_catalog missing 'date' attribute")

    shop = root_el.find("shop")
    if shop is None:
        errors.append("ERR_SHOP_MISSING: <shop> element missing")
        return errors

    if shop.find("currencies") is None:
        errors.append("ERR_CURRENCIES_MISSING: <currencies> block missing")
    if shop.find("categories") is None:
        errors.append("ERR_CATEGORIES_MISSING: <categories> block missing")
    if shop.find("offers") is None:
        errors.append("ERR_OFFERS_MISSING: <offers> block missing")

    return errors
