import requests
import lxml.etree as ET
from datetime import datetime
import re
from html import unescape
from collections import defaultdict

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
OLD_PRICE_MULT      = 1.25     # old_price = price × 1.25 для всіх
MIN_PRICE_THRESHOLD = 150
DESC_LIMIT          = 2800
DEFAULT_QTY         = 2        # кількість якщо постачальник не вказав або вказав 0

# Індивідуальні налаштування наценки по доменах
# Якщо домену нема в словнику — використовується глобальна наценка вище
CUSTOM_MARKUP = {
    "kievopt.com.ua": {
        "markup_percent": 1.0,   # без наценки — ціна постачальника як є
        "markup_fixed":   0,
    },
    "feed.lugi.com.ua": {
        "markup_percent": 1.20,  # +20%
        "markup_fixed":   50,    # +50 грн
    },
}

# Захист від підозрілих цін
MAX_PRICE_UAH      = 500_000
SUSPICIOUS_LOW_UAH = 10.0

# Запасні курси валют
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
    - прибирає <img> теги (EVA не дозволяє картинки в описі)
    - прибирає URL
    - прибирає inline style=
    - обрізає до DESC_LIMIT символів
    - якщо опис порожній або коротший 30 символів — генерує заглушку
    """
    fallback = f"<p>{name_ua} від виробника {vendor}.</p>"
    if not text:
        return fallback
    text = unescape(unescape(text))
    text = re.sub(r'<(script|style).*?>.*?</\1>', '', text, flags=re.DOTALL)
    text = re.sub(r'<img[^>]*>', '', text)
    text = re.sub(r'https?://\S+|www\.\S+', '', text)
    text = re.sub(r'\sstyle="[^"]*"', '', text)
    if len(text) > DESC_LIMIT:
        text = text[:DESC_LIMIT] + "..."
    text = text.strip()
    # Перевірка мінімум 30 символів чистого тексту (вимога EVA)
    plain = re.sub(r'<[^>]+>', '', text).strip()
    if len(plain) < 30:
        return fallback
    return text


def parse_price(raw_text):
    """
    Розумний парсер рядка ціни → float або None.
    Обробляє формати: "1 299,00", "1.299,00", "1,299.00", "199,99", "1299"
    Видаляє: пробіли, \xa0, \u2009, \u202f
    """
    if not raw_text:
        return None
    cleaned = raw_text.strip()
    cleaned = cleaned.replace('\xa0', '').replace('\u2009', '').replace('\u202f', '')
    cleaned = cleaned.replace(' ', '').replace('\t', '')

    if ',' in cleaned and '.' in cleaned:
        if cleaned.rfind(',') > cleaned.rfind('.'):
            # EU формат: 1.299,00
            cleaned = cleaned.replace('.', '').replace(',', '.')
        else:
            # US формат: 1,299.00
            cleaned = cleaned.replace(',', '')
    elif ',' in cleaned:
        parts = cleaned.split(',')
        if len(parts) == 2 and len(parts[1]) <= 2:
            cleaned = cleaned.replace(',', '.')
        else:
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
    """
    rates = dict(FALLBACK_RATES)
    for cur in root.xpath(".//currencies/currency"):
        cur_id   = (cur.get('id') or '').upper().strip()
        rate_str = cur.get('rate', '1')
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
    Конвертує ціну в гривні з 5 рівнями захисту.
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

    # Захист 3: іноземна валюта але число занадто велике
    if currency_id != 'UAH' and raw_price > 500:
        warning = (f"[УВАГА ВАЛЮТА] {domain} offer={offer_id} "
                   f"price={raw_price} {currency_id} — конвертуємо: {price_uah:.2f} UAH")

    # Захист 4: результат нереально великий
    if price_uah > MAX_PRICE_UAH:
        warning = (f"[ЦІНА ЗАВИСОКА] {domain} offer={offer_id} "
                   f"raw={raw_price} {currency_id} → {price_uah:.2f} UAH > {MAX_PRICE_UAH} — пропускаємо")
        return None, warning

    # Захист 5: після конвертації підозріло мало
    if price_uah < SUSPICIOUS_LOW_UAH:
        warning = (f"[ЗАНИЗЬКА ПІСЛЯ КОНВЕРТАЦІЇ] {domain} offer={offer_id} "
                   f"raw={raw_price} {currency_id} → {price_uah:.2f} UAH — пропускаємо")
        return None, warning

    return price_uah, warning


def get_qty(offer):
    """
    Читає кількість товару на складі.
    Підтримує: quantity, stock_quantity, amount, outlets (kievopt).
    Якщо кількість = 0 або тег відсутній → повертає DEFAULT_QTY.
    Повертає (qty: int, used_default: bool).
    """
    # Числові теги кількості
    qty_nodes = offer.xpath(".//quantity|.//quantity_in_stock|.//stock_quantity|.//amount")
    if qty_nodes and (qty_nodes[0].text or '').strip():
        try:
            qty = int(re.sub(r'\D', '', qty_nodes[0].text))
            if qty > 0:
                return qty, False
        except:
            pass

    # Для kievopt — читаємо <outlets count="N">
    outlets = offer.xpath(".//outlets")
    if outlets:
        try:
            qty = int(outlets[0].get('count', '0'))
            if qty > 0:
                return qty, False
        except:
            pass

    # Кількість не вказана або 0 — ставимо DEFAULT_QTY
    return DEFAULT_QTY, True


def load_blacklist():
    """
    Читає blacklist.txt.
    Якщо файл відсутній — повертає порожню множину і не ламає прайс.
    Формат файлу: один offer id на рядок, # для коментарів.
    """
    try:
        with open("blacklist.txt", "r", encoding="utf-8") as f:
            ids = {
                line.strip()
                for line in f
                if line.strip() and not line.strip().startswith('#')
            }
        print(f"Blacklist завантажено: {len(ids)} товарів")
        return ids, len(ids)
    except FileNotFoundError:
        print("blacklist.txt не знайдено — крок пропускається")
        return set(), 0


# ==============================================================================
# 3. ГОЛОВНА ФУНКЦІЯ
# ==============================================================================

def process():
    final_categories = {}
    category_id_map  = {}
    price_warnings   = []
    source_results   = []

    # Структури для звіту
    report_stats     = {}
    cross_duplicates = []   # колізії між постачальниками
    inner_duplicates = []   # дублікати всередині одного постачальника
    blacklist_hits   = defaultdict(int)
    category_errors  = []

    print("--- СТАРТ ОБРОБКИ ---")

    # --------------------------------------------------------------------------
    # КРОК 1: Завантаження blacklist
    # --------------------------------------------------------------------------
    blacklisted_ids, blacklist_count = load_blacklist()

    # --------------------------------------------------------------------------
    # КРОК 2: Завантаження всіх фідів в пам'ять
    # --------------------------------------------------------------------------
    feeds = []  # (prefix, url, domain, root, currency_rates)

    for prefix, url in SOURCES:
        domain = url.split('/')[2]
        try:
            r = requests.get(url, timeout=120)
            if not r.ok:
                print(f"[HTTP ERROR] {domain}: {r.status_code}")
                report_stats[domain] = {"http_error": r.status_code}
                continue
            root           = ET.fromstring(r.content, parser=ET.XMLParser(recover=True))
            currency_rates = get_currency_rates(root)
            visible_rates  = {k: v for k, v in currency_rates.items() if k in ('UAH', 'USD', 'EUR')}
            print(f"[{domain}] Завантажено. Курси: {visible_rates}")
            feeds.append((prefix, url, domain, root, currency_rates))
        except Exception as e:
            print(f"[ПОМИЛКА ФІДУ] {domain}: {e}")
            report_stats[domain] = {"feed_error": str(e)}

    # --------------------------------------------------------------------------
    # КРОК 3: ПРОХІД 1 — збір всіх offer id для виявлення дублікатів
    # --------------------------------------------------------------------------
    id_registry = defaultdict(list)  # offer_id → [(domain, price_text), ...]

    for prefix, url, domain, root, currency_rates in feeds:
        for offer in root.xpath(".//offer"):
            offer_id = offer.get('id', '').strip()
            if not offer_id:
                continue
            price_nodes = offer.xpath('./price')
            price_text  = price_nodes[0].text if price_nodes else ''
            id_registry[offer_id].append((domain, price_text or ''))

    # Визначаємо конфліктні id
    conflict_ids = set()
    for offer_id, entries in id_registry.items():
        if len(entries) > 1:
            domains = [e[0] for e in entries]
            if len(set(domains)) == 1:
                # Дублікат всередині одного постачальника
                inner_duplicates.append({
                    "offer_id": offer_id,
                    "domain":   domains[0],
                    "count":    len(entries)
                })
            else:
                # Колізія між різними постачальниками
                cross_duplicates.append({
                    "offer_id": offer_id,
                    "entries":  entries
                })
            conflict_ids.add(offer_id)

    print(f"\nДублікати між постачальниками: {len(cross_duplicates)}")
    print(f"Дублікати всередині постачальника: {len(inner_duplicates)}")

    # --------------------------------------------------------------------------
    # КРОК 4: Обробка категорій (всі фіди)
    # --------------------------------------------------------------------------
    for prefix, url, domain, root, currency_rates in feeds:
        for cat in root.xpath(".//category"):
            orig_id = cat.get('id')
            new_id  = f"{prefix}{orig_id}" if prefix else orig_id

            # ВИПРАВЛЕНО: while замість if — обробляє будь-яку кількість колізій
            while new_id in category_id_map and category_id_map[new_id] != domain:
                new_id = f"{new_id}9"

            category_id_map[new_id] = domain
            cat.set('id', new_id)

            if cat.get('parentId'):
                parent = cat.get('parentId')
                cat.set('parentId', f"{prefix}{parent}" if prefix else parent)

            final_categories[new_id] = cat

    # --------------------------------------------------------------------------
    # КРОК 5: ПРОХІД 2 — основна обробка товарів
    # --------------------------------------------------------------------------
    processed_offers = []

    for prefix, url, domain, root, currency_rates in feeds:
        count_ok          = 0
        count_low         = 0
        count_no          = 0
        count_price_err   = 0
        count_duplicate   = 0
        count_blacklist   = 0
        count_default_qty = 0

        for offer in root.xpath(".//offer"):
            offer_id = offer.get('id', '').strip()

            # -- Перевірка 1: blacklist --
            if offer_id in blacklisted_ids:
                blacklist_hits[domain] += 1
                count_blacklist += 1
                continue

            # -- Перевірка 2: дублікати --
            if offer_id in conflict_ids:
                count_duplicate += 1
                continue

            # -- Перевірка 3: наявність --
            # відсутній атрибут available = вважаємо недоступним (безпечніше для EVA)
            avail_str = offer.get('available', 'false').lower().strip()
            avail     = avail_str in ['true', 'yes', '1']
            if not avail:
                count_no += 1
                continue

            # -- Кількість на складі --
            qty, used_default = get_qty(offer)
            if used_default:
                count_default_qty += 1

            # -- Перевірка 4: ціна --
            # offer.xpath('./price') — тільки прямий нащадок поточного offer (захист від Lugi)
            price_nodes = offer.xpath('./price')
            if not price_nodes or not (price_nodes[0].text or '').strip():
                count_price_err += 1
                continue
            p_node = price_nodes[0]

            # Перевірка price from="true" (ціна з діапазону)
            if p_node.get('from', 'false').lower() == 'true':
                price_warnings.append(
                    f"[ЦІНА З ДІАПАЗОНУ] {domain} offer={offer_id} "
                    f"price='{p_node.text}' — мінімальна ціна з діапазону"
                )

            try:
                # Парсинг ціни
                raw_p = parse_price(p_node.text)
                if raw_p is None:
                    price_warnings.append(
                        f"[НЕМОЖЛИВО РОЗПАРСИТИ] {domain} offer={offer_id} "
                        f"raw='{p_node.text}'"
                    )
                    count_price_err += 1
                    continue

                # ВИПРАВЛЕНО: .strip().upper() при читанні currencyId
                currency_id     = (offer.findtext('currencyId') or 'UAH').strip().upper()
                price_uah, warn = convert_to_uah(raw_p, currency_id, currency_rates, domain, offer_id)

                if warn:
                    price_warnings.append(warn)
                if price_uah is None:
                    count_price_err += 1
                    continue

                # Наценка — індивідуальна для kievopt і lugi, глобальна для решти
                cfg       = CUSTOM_MARKUP.get(domain, {})
                m_percent = cfg.get("markup_percent", MARKUP_PERCENT)
                m_fixed   = cfg.get("markup_fixed",   MARKUP_FIXED)

                price     = round(price_uah * m_percent + m_fixed)
                old_price = round(price * OLD_PRICE_MULT)

                if price < MIN_PRICE_THRESHOLD:
                    count_low += 1
                    continue

                # Захист: наша ціна не може бути меншою за оригінальну ціну постачальника
                if price < price_uah:
                    price_warnings.append(
                        f"[ЦІНА НИЖЧА ЗА ОРИГІНАЛ] {domain} offer={offer_id} "
                        f"original={price_uah:.0f} UAH our_price={price} UAH — видаляємо"
                    )
                    count_price_err += 1
                    continue

                # -- Збірка товару --
                vendor  = offer.findtext('vendor') or "NoBrand"
                name_ua = fix_text(offer.findtext('name_ua') or offer.findtext('name'))
                if vendor.lower() not in name_ua.lower():
                    name_ua = f"{name_ua} {vendor}"

                new_off = ET.Element("offer", id=offer_id, available="true")

                ET.SubElement(new_off, "name_ua").text    = name_ua[:250]
                ET.SubElement(new_off, "price").text      = str(price)
                ET.SubElement(new_off, "old_price").text  = str(old_price)
                ET.SubElement(new_off, "currencyId").text = "UAH"

                orig_cat = offer.findtext('categoryId')
                cat_id   = f"{prefix}{orig_cat}" if prefix else orig_cat
                ET.SubElement(new_off, "categoryId").text     = cat_id

                ET.SubElement(new_off, "vendor").text         = vendor
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
                    ET.SubElement(new_off, "param", name="Стан").text   = "Новий"
                    ET.SubElement(new_off, "param", name="Колір").text  = "Комбінований"
                    ET.SubElement(new_off, "param", name="Вага").text   = "-"
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

        report_stats[domain] = {
            "ok":          count_ok,
            "low":         count_low,
            "not_avail":   count_no,
            "price_err":   count_price_err,
            "duplicate":   count_duplicate,
            "blacklist":   count_blacklist,
            "default_qty": count_default_qty,
        }
        source_results.append(
            f"{domain}: OK={count_ok} | LOW={count_low} | NOT_AVAIL={count_no} | "
            f"PRICE_ERR={count_price_err} | DUPL={count_duplicate} | "
            f"BLACKLIST={count_blacklist} | DEFAULT_QTY={count_default_qty}"
        )

    # --------------------------------------------------------------------------
    # КРОК 6: Валідація categoryId перед записом XML
    # --------------------------------------------------------------------------
    valid_offers = []
    for offer in processed_offers:
        cat_id   = offer.findtext('categoryId')
        offer_id = offer.get('id', 'unknown')
        if cat_id and cat_id in final_categories:
            valid_offers.append(offer)
        else:
            category_errors.append(
                f"offer={offer_id} categoryId={cat_id} — категорія не знайдена, товар видалено"
            )

    if category_errors:
        print(f"\n[УВАГА] Видалено товарів через відсутню категорію: {len(category_errors)}")

    # --------------------------------------------------------------------------
    # КРОК 7: Збірка фінального XML
    # --------------------------------------------------------------------------
    yml  = ET.Element("yml_catalog", date=datetime.now().strftime("%Y-%m-%d %H:%M"))
    shop = ET.SubElement(yml, "shop")

    currencies = ET.SubElement(shop, "currencies")
    ET.SubElement(currencies, "currency", id="UAH", rate="1")

    cats_n = ET.SubElement(shop, "categories")
    for c in final_categories.values():
        cats_n.append(c)

    offers_n = ET.SubElement(shop, "offers")
    for o in valid_offers:
        offers_n.append(o)

    with open("Masterevanew.xml", "wb") as f:
        f.write(ET.tostring(yml, encoding='utf-8', xml_declaration=True, pretty_print=True))

    # --------------------------------------------------------------------------
    # КРОК 8: Збереження price_warnings.log
    # --------------------------------------------------------------------------
    if price_warnings:
        with open("price_warnings.log", "w", encoding="utf-8") as f:
            f.write('\n'.join(price_warnings))

    # --------------------------------------------------------------------------
    # КРОК 9: Генерація REPORT.md
    # --------------------------------------------------------------------------
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")

    has_critical = any("http_error" in v or "feed_error" in v for v in report_stats.values())
    has_warnings = (len(price_warnings) > 0 or
                    len(cross_duplicates) > 0 or
                    len(inner_duplicates) > 0 or
                    len(category_errors) > 0)

    if has_critical:
        status = "🔴 КРИТИЧНО"
    elif has_warnings:
        status = "🟡 УВАГА"
    else:
        status = "🟢 ОК"

    feeds_ok    = sum(1 for v in report_stats.values() if "ok" in v)
    feeds_total = len(SOURCES)

    md = []
    md.append("# MASTEREVANEW — Звіт запуску")
    md.append(f"**Дата:** {now_str}  ")
    md.append(f"**Статус:** {status}\n")

    md.append("## Загальний підсумок")
    md.append("| Показник | Значення |")
    md.append("|---|---|")
    md.append(f"| Всього товарів у прайсі | {len(valid_offers):,} |")
    md.append(f"| Постачальників оброблено | {feeds_ok}/{feeds_total} |")
    md.append(f"| Видалено через дублі (між постачальниками) | {len(cross_duplicates)} |")
    md.append(f"| Видалено через дублі (всередині постачальника) | {len(inner_duplicates)} |")
    md.append(f"| Видалено через blacklist | {sum(blacklist_hits.values())} |")
    md.append(f"| Видалено через відсутню категорію | {len(category_errors)} |")
    md.append(f"| Попереджень по цінах | {len(price_warnings)} |\n")

    md.append("## По постачальниках")
    md.append("| Постачальник | ✅ OK | 💰 Низька ціна | 🚫 Недоступні | ⚠️ Помилки ціни | 📦 Сток за замовч. | 🔁 Дублі | 🚷 Blacklist |")
    md.append("|---|---|---|---|---|---|---|---|")
    for prefix, url in SOURCES:
        domain = url.split('/')[2]
        v = report_stats.get(domain, {})
        if "http_error" in v:
            md.append(f"| {domain} | 🔴 HTTP {v['http_error']} | — | — | — | — | — | — |")
        elif "feed_error" in v:
            md.append(f"| {domain} | 🔴 ПОМИЛКА ЗАВАНТАЖЕННЯ | — | — | — | — | — | — |")
        else:
            md.append(
                f"| {domain} "
                f"| {v.get('ok', 0)} "
                f"| {v.get('low', 0)} "
                f"| {v.get('not_avail', 0)} "
                f"| {v.get('price_err', 0)} "
                f"| {v.get('default_qty', 0)} "
                f"| {v.get('duplicate', 0)} "
                f"| {v.get('blacklist', 0)} |"
            )

    if cross_duplicates:
        md.append("\n## ⚠️ Дублікати між постачальниками (видалено з прайсу)")
        md.append("| offer id | Постачальник 1 | Ціна 1 | Постачальник 2 | Ціна 2 |")
        md.append("|---|---|---|---|---|")
        for d in cross_duplicates[:50]:
            entries = d["entries"]
            e1 = entries[0] if len(entries) > 0 else ("—", "—")
            e2 = entries[1] if len(entries) > 1 else ("—", "—")
            md.append(f"| {d['offer_id']} | {e1[0]} | {e1[1]} | {e2[0]} | {e2[1]} |")
        if len(cross_duplicates) > 50:
            md.append(f"\n*... і ще {len(cross_duplicates) - 50} дублікатів*")

    if inner_duplicates:
        md.append("\n## ⚠️ Дублікати всередині постачальника (видалено з прайсу)")
        md.append("| offer id | Постачальник | Кількість входжень |")
        md.append("|---|---|---|")
        for d in inner_duplicates[:50]:
            md.append(f"| {d['offer_id']} | {d['domain']} | {d['count']} |")
        if len(inner_duplicates) > 50:
            md.append(f"\n*... і ще {len(inner_duplicates) - 50} дублікатів*")

    if price_warnings:
        md.append("\n## ⚠️ Попередження по цінах")
        md.append("```")
        for w in price_warnings[:50]:
            md.append(w)
        if len(price_warnings) > 50:
            md.append(f"... і ще {len(price_warnings) - 50} попереджень (див. price_warnings.log)")
        md.append("```")

    if category_errors:
        md.append("\n## ⚠️ Товари видалені через відсутню категорію")
        md.append("```")
        for e in category_errors[:20]:
            md.append(e)
        if len(category_errors) > 20:
            md.append(f"... і ще {len(category_errors) - 20}")
        md.append("```")

    md.append("\n## 🚷 Blacklist")
    if blacklist_count == 0:
        md.append("blacklist.txt не знайдено або порожній — крок пропущено")
    else:
        md.append(f"Файл blacklist.txt завантажено: **{blacklist_count}** id у списку  ")
        md.append(f"Видалено товарів з прайсу: **{sum(blacklist_hits.values())}**")

    md.append(f"\n---")
    md.append(f"*Звіт сформовано автоматично: {now_str}*")

    with open("REPORT.md", "w", encoding="utf-8") as f:
        f.write('\n'.join(md))

    # --------------------------------------------------------------------------
    # КРОК 10: Консольний звіт
    # --------------------------------------------------------------------------
    print("\n=== ПІДСУМОК ПО ДЖЕРЕЛАХ ===")
    for s in source_results:
        print(f"  {s}")

    print(f"\n=== ПОПЕРЕДЖЕННЯ ПО ЦІНАХ ({len(price_warnings)}) ===")
    for w in price_warnings[:20]:
        print(f"  {w}")
    if len(price_warnings) > 20:
        print(f"  ... і ще {len(price_warnings) - 20} попереджень у price_warnings.log")

    if cross_duplicates:
        print(f"\n=== ДУБЛІКАТИ МІЖ ПОСТАЧАЛЬНИКАМИ: {len(cross_duplicates)} ===")
        for d in cross_duplicates[:10]:
            print(f"  offer_id={d['offer_id']}: {[e[0] for e in d['entries']]}")

    if inner_duplicates:
        print(f"\n=== ДУБЛІКАТИ ВСЕРЕДИНІ ПОСТАЧАЛЬНИКА: {len(inner_duplicates)} ===")
        for d in inner_duplicates[:10]:
            print(f"  offer_id={d['offer_id']} domain={d['domain']} count={d['count']}")

    if category_errors:
        print(f"\n=== ВИДАЛЕНО ЧЕРЕЗ ВІДСУТНЮ КАТЕГОРІЮ: {len(category_errors)} ===")

    print(f"\n  Статус: {status}")
    print(f"  Всього товарів у прайсі: {len(valid_offers):,}")
    print(f"  Звіт збережено у REPORT.md")
    print("--- ГОТОВО ---")


if __name__ == "__main__":
    process()
