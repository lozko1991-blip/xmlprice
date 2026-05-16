import requests
import lxml.etree as ET
from datetime import datetime
import re
from html import unescape

# ==============================================================================
# 1. КОНФІГУРАЦІЯ
# ==============================================================================
SOURCES = [
    ("1111", "https://shkatulka.in.ua/content/export/cb28b41c71e755eab59d094a399ecfd8.xml"),
    ("2222", "https://opt-drop.com/storage/xml/opt-drop-5.xml"),
    ("3333", "https://feed.lugi.com.ua/index.php?route=extension/feed/unixml/ukr_ru"),
    ("4444", "https://dropom.com.ua/products_feed.xml?hash_tag=b55924e4ebc0576fda79ae6941f7a2a5&languages=uk%2Cru"),
    ("",     "http://kievopt.com.ua/prices/rozetka-22294.yml"),
    ("5555", "https://dwn.royaltoys.com.ua/my/export/v2/e6f6dcf6-2539-4a43-a285-32667169f0db.xml")
]

MARKUP_PERCENT      = 1.35
MARKUP_FIXED        = 40
PROMO_DISCOUNT      = 0.07
MIN_PRICE_THRESHOLD = 150
DESC_LIMIT          = 2800

# Захист від підозрілих цін
MAX_PRICE_UAH      = 500_000   # максимально допустима ціна в грн
SUSPICIOUS_LOW_UAH = 10.0      # ціна нижче цього порогу підозріла навіть до наценки

# Запасні курси валют (якщо фід не дає курс або дає "CBR"/"НБУ")
FALLBACK_RATES = {
    "UAH": 1.0,
    "USD": 41.5,
    "EUR": 45.0,
    "RUB": 0.45,
    "RUR": 0.45,
    "BYN": 12.5,
    "PLN": 10.5,
    "GBP": 52.0,
}


# ==============================================================================
# 2. ДОПОМІЖНІ ФУНКЦІЇ
# ==============================================================================

def fix_text(text):
    """Подвійний unescape HTML-ентіті + нормалізація лапок."""
    if not text:
        return ""
    return unescape(unescape(text)).replace("'", "'").strip()


def clean_description(text, name_ua, vendor):
    """
    Чистить HTML-опис товару:
    - прибирає <script>/<style>
    - прибирає URL
    - прибирає inline style=
    - обрізає до DESC_LIMIT символів
    - якщо опис порожній — генерує заглушку
    """
    if not text:
        return f"<p>{name_ua} від виробника {vendor}.</p>"
    text = unescape(unescape(text))
    text = re.sub(r'<(script|style).*?>.*?</\1>', '', text, flags=re.DOTALL)
    text = re.sub(r'https?://\S+|www\.\S+', '', text)
    text = re.sub(r'\sstyle="[^"]*"', '', text)
    if len(text) > DESC_LIMIT:
        text = text[:DESC_LIMIT] + "..."
    return text.strip()


def parse_price(raw_text):
    """
    Розумний парсер рядка ціни → float або None.

    Обробляє формати:
      "1 299,00"   → 1299.0   (пробіл як тисячний, кома як десятковий)
      "1.299,00"   → 1299.0   (EU: крапка=тисячі, кома=десяткові)
      "1,299.00"   → 1299.0   (US: кома=тисячі, крапка=десяткові)
      "199,99"     → 199.99
      "1299"       → 1299.0
    Видаляє: звичайні пробіли, \xa0, \u2009, \u202f
    """
    if not raw_text:
        return None

    cleaned = raw_text.strip()
    # Видаляємо всі види пробілів і спецсимволів
    cleaned = cleaned.replace('\xa0', '').replace('\u2009', '').replace('\u202f', '')
    cleaned = cleaned.replace(' ', '').replace('\t', '')

    if ',' in cleaned and '.' in cleaned:
        # Обидва символи — визначаємо який десятковий
        if cleaned.rfind(',') > cleaned.rfind('.'):
            # Формат EU: 1.299,00
            cleaned = cleaned.replace('.', '').replace(',', '.')
        else:
            # Формат US: 1,299.00
            cleaned = cleaned.replace(',', '')
    elif ',' in cleaned:
        parts = cleaned.split(',')
        if len(parts) == 2 and len(parts[1]) <= 2:
            # 199,99 — кома як десятковий розділювач
            cleaned = cleaned.replace(',', '.')
        else:
            # 1,299 або 1,299,000 — кома як тисячний розділювач
            cleaned = cleaned.replace(',', '')

    try:
        result = float(cleaned)
        return result if result > 0 else None
    except (ValueError, TypeError):
        return None


def get_currency_rates(root):
    """
    Витягує курси валют із секції <currencies> XML-фіду.
    Якщо курс = 'CBR'/'НБУ'/'NBU'/'ECB' — підставляє FALLBACK_RATES.
    Повертає dict {currency_id: rate_float}.
    """
    rates = dict(FALLBACK_RATES)
    for cur in root.xpath(".//currencies/currency"):
        cur_id    = (cur.get('id') or '').upper().strip()
        rate_str  = cur.get('rate', '1')
        if not cur_id:
            continue
        if rate_str in ('CBR', 'НБУ', 'NBU', 'ECB'):
            rates.setdefault(cur_id, FALLBACK_RATES.get(cur_id, 1.0))
        else:
            parsed = parse_price(rate_str)
            if parsed and parsed > 0:
                rates[cur_id] = parsed
    return rates


def convert_to_uah(raw_price, currency_id, rates, domain, offer_id):
    """
    Конвертує ціну в гривні з чотирма рівнями захисту.
    Повертає (price_uah: float | None, warning: str | None).
    """
    currency_id = (currency_id or 'UAH').upper().strip()
    warning = None

    # Захист 1: невідома валюта
    if currency_id not in rates:
        warning = (f"[НЕВІДОМА ВАЛЮТА] {domain} offer={offer_id} "
                   f"currency={currency_id} — використовуємо UAH")
        currency_id = 'UAH'

    rate      = rates.get(currency_id, 1.0)
    price_uah = raw_price * rate

    # Захист 2: ціна в UAH підозріло мала
    if currency_id == 'UAH' and raw_price < SUSPICIOUS_LOW_UAH:
        warning = (f"[ПІДОЗРІЛА ЦІНА] {domain} offer={offer_id} "
                   f"price={raw_price} UAH < {SUSPICIOUS_LOW_UAH} грн — пропускаємо")
        return None, warning

    # Захист 3: іноземна валюта але число занадто велике (можливо вже в грн)
    if currency_id != 'UAH' and raw_price > 500:
        warning = (f"[УВАГА ВАЛЮТА] {domain} offer={offer_id} "
                   f"price={raw_price} {currency_id} — висока ціна в іноземній валюті, "
                   f"конвертуємо: {price_uah:.2f} UAH")

    # Захист 4: результат нереально великий
    if price_uah > MAX_PRICE_UAH:
        warning = (f"[ЦІНА ЗАВИСОКА] {domain} offer={offer_id} "
                   f"raw={raw_price} {currency_id} → {price_uah:.2f} UAH > {MAX_PRICE_UAH} — пропускаємо")
        return None, warning

    # Захист 5: після конвертації все одно підозріло мало
    if price_uah < SUSPICIOUS_LOW_UAH:
        warning = (f"[ЗАНИЗЬКА ПІСЛЯ КОНВЕРТАЦІЇ] {domain} offer={offer_id} "
                   f"raw={raw_price} {currency_id} → {price_uah:.2f} UAH — пропускаємо")
        return None, warning

    return price_uah, warning


# ==============================================================================
# 3. ГОЛОВНА ФУНКЦІЯ
# ==============================================================================

def process():
    final_categories = {}
    processed_offers = []
    source_results   = []
    category_id_map  = {}
    price_warnings   = []

    print("--- СТАРТ ОБРОБКИ ---")

    for prefix, url in SOURCES:
        domain = url.split('/')[2]
        try:
            r = requests.get(url, timeout=120)
            if not r.ok:
                print(f"[HTTP ERROR] {domain}: {r.status_code}")
                continue

            root = ET.fromstring(r.content, parser=ET.XMLParser(recover=True))

            # Курси валют для цього фіду
            currency_rates = get_currency_rates(root)
            visible_rates  = {k: v for k, v in currency_rates.items() if k in ('UAH', 'USD', 'EUR')}
            print(f"\n[{domain}] Курси валют: {visible_rates}")

            count_ok, count_low, count_no, count_price_err = 0, 0, 0, 0

            # ------------------------------------------------------------------
            # КАТЕГОРІЇ
            # ------------------------------------------------------------------
            for cat in root.xpath(".//category"):
                orig_id = cat.get('id')
                new_id  = f"{prefix}{orig_id}" if prefix else orig_id

                if new_id in category_id_map and category_id_map[new_id] != domain:
                    new_id = f"{new_id}9"
                category_id_map[new_id] = domain
                cat.set('id', new_id)

                if cat.get('parentId'):
                    parent = cat.get('parentId')
                    cat.set('parentId', f"{prefix}{parent}" if prefix else parent)

                final_categories[new_id] = cat

            # ------------------------------------------------------------------
            # ТОВАРИ
            # ------------------------------------------------------------------
            for offer in root.xpath(".//offer"):
                offer_id = offer.get('id', 'unknown')

                # Фільтр: доступність
                avail = offer.get('available', '').lower() in ['true', 'yes', '1']
                if not avail:
                    count_no += 1
                    continue

                # Кількість на складі
                qty_nodes = offer.xpath(".//quantity|.//stock_quantity|.//amount")
                qty = 3
                if qty_nodes and qty_nodes[0].text:
                    try:
                        qty = int(re.sub(r'\D', '', qty_nodes[0].text))
                        if qty <= 0:
                            qty = 3
                    except:
                        qty = 3

                # --------------------------------------------------------------
                # ЦІНА — ВИПРАВЛЕННЯ ДЛЯ LUGI ТА ІНШИХ "БРУДНИХ" XML
                #
                # Використовуємо offer.xpath('./price') замість offer.find('price').
                # './price' — шукає ТІЛЬКИ прямого дочірнього нащадка поточного
                # тегу <offer>. Це гарантує що lxml не підхопить <price> з
                # сусіднього оффера або вкладеного елемента після відновлення
                # зламаного XML через XMLParser(recover=True).
                # --------------------------------------------------------------
                price_nodes = offer.xpath('./price')
                if not price_nodes or not (price_nodes[0].text or '').strip():
                    count_price_err += 1
                    continue
                p_node = price_nodes[0]

                try:
                    # Крок 1: парсинг рядка ціни
                    raw_p = parse_price(p_node.text)
                    if raw_p is None:
                        price_warnings.append(
                            f"[НЕМОЖЛИВО РОЗПАРСИТИ] {domain} offer={offer_id} "
                            f"raw='{p_node.text}'"
                        )
                        count_price_err += 1
                        continue

                    # Крок 2: конвертація в гривні
                    currency_id       = offer.findtext('currencyId') or 'UAH'
                    price_uah, warn   = convert_to_uah(raw_p, currency_id, currency_rates, domain, offer_id)

                    if warn:
                        price_warnings.append(warn)
                    if price_uah is None:
                        count_price_err += 1
                        continue

                    # Крок 3: наценка
                    price = round(price_uah * MARKUP_PERCENT + MARKUP_FIXED)
                    if price < MIN_PRICE_THRESHOLD:
                        count_low += 1
                        continue

                    price_promo = round(price * (1 - PROMO_DISCOUNT))

                    # ----------------------------------------------------------
                    # ЗБІРКА ТОВАРУ
                    # ----------------------------------------------------------
                    vendor  = offer.findtext('vendor') or "NoBrand"
                    name_ua = fix_text(offer.findtext('name_ua') or offer.findtext('name'))
                    if vendor.lower() not in name_ua.lower():
                        name_ua = f"{name_ua} {vendor}"

                    new_off = ET.Element("offer", id=offer_id, available="true")

                    ET.SubElement(new_off, "name_ua").text       = name_ua[:250]
                    ET.SubElement(new_off, "price").text         = str(price)
                    ET.SubElement(new_off, "price_promo").text   = str(price_promo)
                    ET.SubElement(new_off, "currencyId").text    = "UAH"

                    orig_cat = offer.findtext('categoryId')
                    cat_id   = f"{prefix}{orig_cat}" if prefix else orig_cat
                    ET.SubElement(new_off, "categoryId").text    = cat_id

                    ET.SubElement(new_off, "vendor").text        = vendor
                    ET.SubElement(new_off, "stock_quantity").text = str(qty)

                    desc = clean_description(
                        offer.findtext('description_ua') or offer.findtext('description'),
                        name_ua, vendor
                    )
                    ET.SubElement(new_off, "description_ua").text = ET.CDATA(desc)

                    for pic in offer.findall('picture'):
                        if pic.text:
                            ET.SubElement(new_off, "picture").text = pic.text

                    params = offer.findall('param')
                    if not params:
                        ET.SubElement(new_off, "param", name="Стан").text = "Новий"
                    else:
                        for p in params:
                            p.text = fix_text(p.text)
                            new_off.append(p)

                    processed_offers.append(new_off)
                    count_ok += 1

                except Exception as e:
                    price_warnings.append(
                        f"[ВИНЯТОК] {domain} offer={offer_id} "
                        f"price='{p_node.text}' err={e}"
                    )
                    count_price_err += 1
                    continue

            source_results.append(
                f"{domain}: OK={count_ok} | LOW={count_low} | "
                f"NOT_AVAIL={count_no} | PRICE_ERR={count_price_err}"
            )

        except Exception as e:
            print(f"[ПОМИЛКА ФІДУ] {domain}: {e}")
            continue

    # ==========================================================================
    # 4. ЗБІРКА ФІНАЛЬНОГО XML
    # ==========================================================================
    yml  = ET.Element("yml_catalog", date=datetime.now().strftime("%Y-%m-%d %H:%M"))
    shop = ET.SubElement(yml, "shop")

    currencies = ET.SubElement(shop, "currencies")
    ET.SubElement(currencies, "currency", id="UAH", rate="1")

    cats_n = ET.SubElement(shop, "categories")
    for c in final_categories.values():
        cats_n.append(c)

    offers_n = ET.SubElement(shop, "offers")
    for o in processed_offers:
        offers_n.append(o)

    with open("Masterevanew.xml", "wb") as f:
        f.write(ET.tostring(yml, encoding='utf-8', xml_declaration=True, pretty_print=True))

    # ==========================================================================
    # 5. ЗВІТ
    # ==========================================================================
    print("\n=== ПІДСУМОК ПО ДЖЕРЕЛАХ ===")
    for s in source_results:
        print(f"  {s}")

    print(f"\n=== ПОПЕРЕДЖЕННЯ ПО ЦІНАХ ({len(price_warnings)}) ===")
    for w in price_warnings[:50]:
        print(f"  {w}")
    if len(price_warnings) > 50:
        print(f"  ... і ще {len(price_warnings) - 50} попереджень")

    if price_warnings:
        with open("price_warnings.log", "w", encoding="utf-8") as f:
            f.write('\n'.join(price_warnings))
        print(f"\n  Всі попередження збережено у price_warnings.log")

    print(f"\n  Всього товарів у файлі: {len(processed_offers)}")
    print("--- ГОТОВО ---")


if __name__ == "__main__":
    process()
