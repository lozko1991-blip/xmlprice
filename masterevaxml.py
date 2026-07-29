import requests
import lxml.etree as ET
from datetime import datetime
import re
import time
from html import unescape
from collections import defaultdict, Counter

# ==============================================================================
# 1. КОНФІГУРАЦІЯ
# ==============================================================================
# (cat_prefix, id_prefix, url)
# cat_prefix  — додається до categoryId з фіду постачальника
# id_prefix   — додається до offer id (через "_"); "" = без префіксу
SOURCES = [
    ("1000", "1000",  "https://dropt.in.ua/index.php?route=export/prom&markup=15"),
    ("2222", "2222",  "https://opt-drop.com/storage/xml/opt-drop-1.xml"),
    ("3333", "3333",  "https://feed.lugi.com.ua/index.php?route=extension/feed/unixml/ukr_ru"),
    ("4444", "4444",  "https://dropom.com.ua/products_feed.xml?hash_tag=b55924e4ebc0576fda79ae6941f7a2a5&languages=uk%2Cru"),
    ("",     "",      "http://kievopt.com.ua/prices/rozetka-22294.yml"),
    ("5555", "",      "https://dwn.royaltoys.com.ua/my/export/v2/e6f6dcf6-2539-4a43-a285-32667169f0db.xml"),
    ("7777", "7777",  "https://posudograd.ua/dropship/19155/prom"),
    ("8888", "8888",  "https://i-posud.com.ua/assets/export/xml/prom_export_sklad.xml"),
    ("9999", "9999",  "https://www.websklad.biz.ua/wp-content/uploads/randomize_prom_84230.xml"),
    ("1111", "1111",  "https://www.shkatulka.in.ua/content/export/cb28b41c71e755eab59d094a399ecfd8.xml"),
    ("1100", "1100",  "https://forus.com.ua/vugruzka/forus_opt_prom_stock.xml"),
    ("1200", "1200",  "https://aveon.net.ua/products_feed.xml?hash_tag=7b71fadcc4a12f03cf26a304da032fba&sales_notes=&product_ids=&label_ids=&exclude_fields=&html_description=0&yandex_cpa=&process_presence_sure=&languages=uk&group_ids="),
    ("1300", "1300",  "https://sonechko233.com.ua/products_feed.xml?hash_tag=220ed1761695cce1df21b74fc555efcd&sales_notes=&product_ids=&label_ids=&exclude_fields=&html_description=0&yandex_cpa=&process_presence_sure=&languages=uk%2Cru&extra_fields=&group_ids="),
    ("2000", "2000",  "https://crm.yavshoke.ua/media/export/bt_opt_price.xml"),
    ("2000", "2000",  "https://crm.yavshoke.ua/media/export/posuda_opt_price.xml"),
    ("2000", "2000",  "https://crm.yavshoke.ua/media/export/top_aliexpress_opt_price.xml"),
    ("3000", "3000",  "https://api.dropshipping.ua/api/feeds/bestsellers.xml"),
    ("3000", "3000",  "https://api.dropshipping.ua/api/feeds/4452.xml"),
]

OLD_PRICE_MULT      = 1.25     # old_price = price × 1.25 для всіх
MIN_PRICE_THRESHOLD = 199      # мінімальна ціна в грн
MIN_OFFERS_PER_CATEGORY = 3   # категорії з ≤ N прямих товарів видаляються (якщо не батьківські)
DESC_LIMIT          = 1000     # максимальна довжина опису
DEFAULT_QTY         = 2        # кількість якщо постачальник не вказав або вказав 0
REQUEST_DELAY       = 6        # затримка між запитами в секундах (щоб не отримати 429)
MAX_FETCH_ATTEMPTS  = 5        # скільки разів пробувати завантажити фід
RETRY_BACKOFF_BASE  = 20       # базова пауза перед повтором (сек); далі росте 20→40→80…
RETRY_BACKOFF_MAX   = 120      # стеля паузи між повторами (сек)


# Наценка по доменах — ОБОВ'ЯЗКОВА для КОЖНОГО постачальника зі SOURCES.
# Глобальної наценки за замовчуванням більше немає: якщо домену тут нема,
# validate_markup_config() зупинить запуск з чіткою помилкою на старті.
#
# Обов'язкові ключі:  markup_percent, markup_fixed  (резерв якщо markup_tiers не задані)
# Опційні ключі:      min_price_raw / min_price_final
# Тієрна наценка:     markup_tiers — список кортежів (max_ціна, percent, fixed_грн)
#                     Перший тієр де ціна_товару <= max_ціна — той і застосовується.
#                     Тієри МАЮТЬ бути відсортовані за зростанням max_ціни.
#                     Останній тієр (999999) покриває всі ціни вище 15 000 грн.
#
# Формат тієру: (максимальна_ціна_постачальника, markup_percent, markup_fixed)
#   Приклад: price_uah = 800 → шукаємо перший рядок де 800 <= max_ціна
#            (500, 1.20, 40)  — ні, 800 > 500
#            (1000, 1.17, 35) — так! → ціна = round(800 * 1.17 + 35) = 971 грн
CUSTOM_MARKUP = {
    "dropt.in.ua": {
        "markup_percent": 1.20,  # резерв (якщо markup_tiers видалити)
        "markup_fixed":   40,
        "markup_tiers": [
            #  (до якої ціни,  %,     фікс.грн)     ← редагуйте цифри тут
            (500,    1.17,  40),   # до 500 грн     → поточна (не змінюємо)
            (1000,   1.15,  40),   # 500–1000 грн
            (2000,   1.12,  40),   # 1000–2000 грн
            (4000,   1.12,  40),   # 2000–4000 грн
            (8000,   1.12,  70),   # 4000–8000 грн
            (999999, 1.12,  70),   # вище 8000 грн
        ],
    },
    "opt-drop.com": {
        "markup_percent": 1.35,
        "markup_fixed":   40,
        "markup_tiers": [
            (500,    1.35,  40),   # до 500 грн     → поточна
            (1000,   1.32,  35),   # 500–1000 грн
            (2000,   1.28,  30),   # 1000–2000 грн
            (4000,   1.24,  20),   # 2000–4000 грн
            (8000,   1.24,  50),   # 4000–8000 грн
            (999999, 1.24,  50),   # вище 8000 грн
        ],
    },
    "kievopt.com.ua": {
        "markup_percent": 1.0,   # без наценки — ціна постачальника як є
        "markup_fixed":   0,
        "markup_tiers": [
            (500,    1.0,   0),    # до 500 грн     → поточна
            (1000,   1.0,   0),    # 500–1000 грн
            (2000,   1.0,   0),    # 1000–2000 грн
            (4000,   1.0,   0),    # 2000–4000 грн
            (8000,   1.0,   0),    # 4000–8000 грн
            (999999, 1.0,   0),    # вище 8000 грн
        ],
    },
    "dwn.royaltoys.com.ua": {   # домен з url.split('/')[2] — саме dwn.royaltoys.com.ua
        "markup_percent": 1.01,
        "markup_fixed":   5,
        "markup_tiers": [
            (500,    1.01,  20),    # до 500 грн     → поточна
            (1000,   1.01,  25),    # 500–1000 грн
            (2000,   1.01,  25),    # 1000–2000 грн
            (4000,   1.01,  35),    # 2000–4000 грн
            (8000,   1.01,  35),    # 4000–8000 грн
            (999999, 1.01,  35),    # вище 8000 грн
        ],
    },
    "feed.lugi.com.ua": {
        "markup_percent": 1.15,
        "markup_fixed":   50,
        "markup_tiers": [
            (500,    1.15,  50),   # до 500 грн     → поточна
            (1000,   1.13,  40),   # 500–1000 грн
            (2000,   1.10,  40),   # 1000–2000 грн
            (4000,   1.10,  40),   # 2000–4000 грн
            (8000,   1.10,  50),   # 4000–8000 грн
            (999999, 1.10,  50),   # вище 8000 грн
        ],
    },
    "dropom.com.ua": {
        "markup_percent": 1.35,
        "markup_fixed":   40,
        "markup_tiers": [
            (500,    1.35,  40),   # до 500 грн     → поточна
            (1000,   1.32,  35),   # 500–1000 грн
            (2000,   1.28,  30),   # 1000–2000 грн
            (4000,   1.24,  20),   # 2000–4000 грн
            (8000,   1.22,   0),   # 4000–8000 грн
            (999999, 1.22,   0),   # вище 8000 грн
        ],
    },
    "posudograd.ua": {
        "markup_percent": 1.0,
        "markup_fixed":   40,
        "min_price_raw":  70,      # мінімум від ціни постачальника
        "markup_tiers": [
            (500,    1.0,   40),   # до 500 грн     → поточна
            (1000,   1.0,   40),   # 500–1000 грн
            (2000,   1.0,   30),   # 1000–2000 грн
            (4000,   1.0,   30),   # 2000–4000 грн
            (8000,   1.0,   30),   # 4000–8000 грн
            (999999, 1.0,   30),   # вище 8000 грн
        ],
    },
    "i-posud.com.ua": {
        "markup_percent": 1.15,
        "markup_fixed":   40,
        "min_price_raw":  70,      # мінімум від ціни постачальника
        "markup_tiers": [
            (500,    1.20,  40),   # до 500 грн     → поточна
            (1000,   1.17,  35),   # 500–1000 грн
            (2000,   1.17,  30),   # 1000–2000 грн
            (4000,   1.17,  20),   # 2000–4000 грн
            (8000,   1.17,  50),   # 4000–8000 грн
            (999999, 1.05,  50),   # вище 8000 грн
        ],
    },
    "www.websklad.biz.ua": {    # URL має www. — ключ теж має бути з www.
        "markup_percent": 1.0,
        "markup_fixed":   30,
        "markup_tiers": [
            (500,    1.0,   30),   # до 500 грн     → поточна
            (1000,   1.0,   25),   # 500–1000 грн
            (2000,   1.0,   20),   # 1000–2000 грн
            (4000,   1.0,   15),   # 2000–4000 грн
            (8000,   1.0,    0),   # 4000–8000 грн
            (999999, 1.0,    0),   # вище 8000 грн
        ],
    },
    "www.shkatulka.in.ua": {    # URL має www. — ключ теж має бути з www.
        "markup_percent": 1.30,
        "markup_fixed":   40,
        "markup_tiers": [
            (500,    1.30,  30),   # до 500 грн     → поточна
            (1000,   1.27,  35),   # 500–1000 грн
            (2000,   1.27,  30),   # 1000–2000 грн
            (4000,   1.25,  20),   # 2000–4000 грн
            (8000,   1.20,  40),   # 4000–8000 грн
            (999999, 1.20,  40),   # вище 8000 грн
        ],
    },
    "forus.com.ua": {           # URL без www. — ключ без www.
        "markup_percent": 1.15,
        "markup_fixed":   40,
        "min_price_final": 130,   # мінімум від фінальної ціни (після наценки)
        "markup_tiers": [
            (500,    1.15,  40),   # до 500 грн     → поточна
            (1000,   1.15,  40),   # 500–1000 грн
            (2000,   1.10,  30),   # 1000–2000 грн
            (4000,   1.10,  20),   # 2000–4000 грн
            (8000,   1.10,  40),   # 4000–8000 грн
            (999999, 1.10,  40),   # вище 8000 грн
        ],
    },
    "aveon.net.ua": {
        "markup_percent": 1.25,
        "markup_fixed":   50,
        "markup_tiers": [
            (500,    1.25,  60),   # до 500 грн     → поточна
            (1000,   1.23,  45),   # 500–1000 грн
            (2000,   1.20,  40),   # 1000–2000 грн
            (4000,   1.20,  40),   # 2000–4000 грн
            (8000,   1.20,  50),   # 4000–8000 грн
            (999999, 1.20,  50),   # вище 8000 грн
        ],
    },
    "sonechko233.com.ua": {
        "markup_percent": 1.25,
        "markup_fixed":   50,
        "markup_tiers": [
            (500,    1.30,  50),   # до 500 грн     → поточна
            (1000,   1.25,  50),   # 500–1000 грн
            (2000,   1.25,  50),   # 1000–2000 грн
            (4000,   1.23,  50),   # 2000–4000 грн
            (8000,   1.23,  50),   # 4000–8000 грн
            (999999, 1.23,  50),   # вище 8000 грн
        ],
    },
    "crm.yavshoke.ua": {
        "markup_percent": 1.35,
        "markup_fixed":   50,
        "markup_tiers": [
            (1000,   1.35,  50),   # до 1000 грн    → 35% + 50 грн
            (3000,   1.32,  50),   # 1000–3000 грн  → 32% + 50 грн
            (999999, 1.30,  50),   # вище 3000 грн  → 30% + 50 грн
        ],
    },
    "api.dropshipping.ua": {
        "markup_percent": 1.35,
        "markup_fixed":   50,
        "markup_tiers": [
            (1000,   1.35,  50),   # до 1000 грн    → 35% + 50 грн
            (3000,   1.30,  50),   # 1000–3000 грн  → 30% + 50 грн
            (999999, 1.25,  50),   # вище 3000 грн  → 25% + 50 грн
        ],
    },
}

# Захист від підозрілих цін
MAX_PRICE_UAH      = 500_000
SUSPICIOUS_LOW_UAH = 10.0

# Запасні курси валют (використовуються якщо НБУ API недоступне)
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


# Фіксовані українські назви категорій для постачальників, що надають їх не тією мовою.
# Ключ: домен постачальника → словник {оригінальна_назва: українська_назва}
# При обробці (Крок 4) оригінальна назва замінюється на зафіксовану українську.
CATEGORY_NAME_OVERRIDES = {
    "sonechko233.com.ua": {
        "Товары для дома и сада":                          "Товари для дому та саду",
        "Сезонный  товар":                                 "Сезонний товар",
        "Сезонный товар":                                  "Сезонний товар",
        "Красота и здоровье":                              "Краса та здоров'я",
        "PowerBank, внешние аккумуляторы":                 "PowerBank, зовнішні акумулятори",
        "Все для кухни":                                   "Все для кухні",
        "Электроника":                                     "Електроніка",
        "Игровые девайсы для ПК":                          "Ігрові девайси для ПК",
        "Одежда и обувь":                                  "Одяг та взуття",
        "Охота и Рыбалка":                                 "Мисливство та Рибалка",
        "Автотовары, электроинструмент, ручной инструмент": "Автотовари, електроінструмент, ручний інструмент",
        "Детский мир, детские товары":                     "Дитячий світ, дитячі товари",
        "Спорт, здоровье, туризм":                         "Спорт, здоров'я, туризм",
    },
}


# ==============================================================================
# 2. ДОПОМІЖНІ ФУНКЦІЇ
# ==============================================================================

def get_source_label(url):
    """
    Повертає унікальну мітку джерела для звіту.
    Розрізняє окремі прайси crm.yavshoke.ua та api.dropshipping.ua.
    """
    domain = url.split('/')[2]
    if "bt_opt_price.xml" in url:
        return "crm.yavshoke.ua (Побутова техніка)"
    elif "posuda_opt_price.xml" in url:
        return "crm.yavshoke.ua (Посуд)"
    elif "top_aliexpress_opt_price.xml" in url:
        return "crm.yavshoke.ua (Топ Aliexpress)"
    elif "bestsellers.xml" in url:
        return "api.dropshipping.ua (Бестселери)"
    elif "4452.xml" in url:
        return "api.dropshipping.ua (Основний прайс)"
    return domain


def fix_text(text):
    """
    Подвійний unescape HTML-ентіті + нормалізація лапок.
    Безпечно обробляє None.
    """
    if not text:
        return ""
    text = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F]', '', str(text))
    return unescape(unescape(text)).replace("\u2019", "'").strip()


_UA_CHARS = frozenset('\u0457\u0454\u0491\u0407\u0404\u0490')
_RU_CHARS = frozenset('\u044b\u044a\u044d\u042b\u042a\u042d')

def _lang(text):
    """\u0412\u0438\u0437\u043d\u0430\u0447\u0430\u0454 \u043c\u043e\u0432\u0443 \u0442\u0435\u043a\u0441\u0442\u0443 \u0437\u0430 \u0443\u043d\u0456\u043a\u0430\u043b\u044c\u043d\u0438\u043c\u0438 \u0441\u0438\u043c\u0432\u043e\u043b\u0430\u043c\u0438: 'uk', 'ru' \u0430\u0431\u043e 'other'."""
    t = text or ''
    if any(c in _UA_CHARS for c in t): return 'uk'
    if any(c in _RU_CHARS for c in t): return 'ru'
    return 'other'


_RU_TO_UA = str.maketrans({'ы': 'и', 'Ы': 'И', 'э': 'е', 'Э': 'Е',
                            'ъ': '',  'Ъ': '',  'ё': 'е', 'Ё': 'Е'})

def ru_to_ua(text):
    if not text:
        return text
    return text.translate(_RU_TO_UA)


def clean_description(text, name_ua, vendor):
    """
    Нормалізує HTML-опис товару для EVA:
    - подвійний unescape (для opt-drop з закодованим HTML)
    - видаляє <script>/<style> з вмістом
    - видаляє <img> теги (EVA не приймає картинки в описі)
    - видаляє всі URL
    - видаляє inline style=
    - видаляє порожні HTML теги (залишки після очистки)
    - обрізає до DESC_LIMIT символів
    - якщо чистий текст < 30 символів — генерує заглушку (вимога EVA)
    """
    fallback = f"<p>{name_ua} від виробника {vendor}.</p>".replace(']]>', ']] >')
    if not text:
        return fallback

    # Подвійний unescape — для opt-drop який дає &lt;p&gt; замість <p>
    text = unescape(unescape(str(text)))
    # XML 1.0 забороняє ASCII 0-8, 11-12, 14-31
    text = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F]', '', text)

    # Видаляємо небажані теги (з вмістом)
    text = re.sub(r'<(script|style|iframe|video|audio)[^>]*>.*?</\1>', '', text, flags=re.DOTALL)
    # Самозакривні версії (наприклад <video/>)
    text = re.sub(r'<(video|audio|iframe)[^>]*/>', '', text)
    # HTML-коментарі
    text = re.sub(r'<!--.*?-->', '', text, flags=re.DOTALL)
    text = re.sub(r'<img[^>]*/?>', '', text)

    # Видаляємо URL
    text = re.sub(r'https?://\S+|www\.\S+', '', text)

    # Видаляємо inline стилі
    text = re.sub(r'\s+style="[^"]*"', '', text)
    text = re.sub(r"\s+style='[^']*'", '', text)

    # Видаляємо порожні теги (залишки після видалення img/url)
    text = re.sub(r'<(\w+)[^>]*>\s*</\1>', '', text)

    # Обрізаємо безпечно — не розрізаємо HTML-тег посередині
    if len(text) > DESC_LIMIT:
        cut_pos = text.rfind('>', 0, DESC_LIMIT)
        if cut_pos > 0:
            text = text[:cut_pos + 1] + "..."
        else:
            text = text[:DESC_LIMIT] + "..."

    text = text.strip()

    # Перевірка мінімум 30 символів чистого тексту (вимога EVA)
    plain = re.sub(r'<[^>]+>', '', text).strip()
    if len(plain) < 30:
        return fallback

    # Захист від ]]> що закриває CDATA передчасно і ламає весь XML
    text = text.replace(']]>', ']] >')

    return text


def parse_price(raw_text):
    """
    Розумний парсер рядка ціни → float або None.

    Обробляє формати:
      "1 299,00"  → 1299.0  (пробіл + кома)
      "1.299,00"  → 1299.0  (EU: крапка=тисячі, кома=десяткові)
      "1,299.00"  → 1299.0  (US: кома=тисячі, крапка=десяткові)
      "199,99"    → 199.99
      "329.7"     → 329.7
      "1299"      → 1299.0
    Видаляє: пробіли, \xa0, \u2009, \u202f
    """
    if not raw_text:
        return None

    cleaned = str(raw_text).strip()
    # Видаляємо всі види пробілів і спецсимволів
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
            # 199,99 — кома як десятковий розділювач
            cleaned = cleaned.replace(',', '.')
        else:
            # 1,299 — кома як тисячний розділювач
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
        cur_id   = (cur.get('id') or '').upper().strip()
        rate_str = cur.get('rate', '1')
        if not cur_id:
            continue
        if rate_str in ('CBR', 'НБУ', 'NBU', 'ECB', 'CB'):
            # Плаваючий курс — використовуємо FALLBACK
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

    # Захист 1: невідома валюта → лікуємо як UAH
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

    Підтримує всі варіанти тегів постачальників:
    - quantity        (shkatulka, opt-drop)
    - quantity_in_stock (lugi, dropom)
    - stock_quantity  (kievopt, royaltoys)
    - amount          (загальний)
    - outlets count="" (kievopt YML формат)

    Якщо кількість = 0 або тег відсутній → повертає DEFAULT_QTY.
    Повертає (qty: int, used_default: bool).
    """
    qty_nodes = offer.xpath(
        ".//quantity|.//quantity_in_stock|.//stock_quantity|.//amount"
    )
    if qty_nodes:
        node_text = (qty_nodes[0].text or '').strip()
        if node_text:
            try:
                qty = int(re.sub(r'\D', '', node_text))
                if qty > 0:
                    return qty, False
            except (ValueError, TypeError):
                pass

    # Для kievopt YML — <outlets count="N">
    outlets = offer.xpath(".//outlets")
    if outlets:
        try:
            qty = int(outlets[0].get('count', '0'))
            if qty > 0:
                return qty, False
        except (ValueError, TypeError):
            pass

    return DEFAULT_QTY, True


def get_availability(offer):
    """
    Читає наявність товару.

    Підтримує:
    - available атрибут (всі постачальники)
    - in_stock атрибут (lugi додатково)
    - <available>true</available> як дочірній тег

    Повертає True якщо товар доступний.
    """
    AVAIL_TRUE = {'true', 'yes', '1'}

    # Атрибут available
    avail_raw = offer.get('available', '').lower().strip()
    if avail_raw:
        return avail_raw in AVAIL_TRUE

    # Дочірній тег <available> (деякі постачальники)
    avail_tag = offer.findtext('available')
    if avail_tag is not None:
        return avail_tag.lower().strip() in AVAIL_TRUE

    # Атрибут in_stock (lugi)
    in_stock = offer.get('in_stock', '').lower().strip()
    if in_stock:
        return in_stock in AVAIL_TRUE

    return False


def get_name(offer):
    """
    Читає назву товару.
    Пріоритет: name_ua → name.
    Обробляє CDATA і звичайний текст.
    """
    name = fix_text(offer.findtext('name_ua') or '')
    if not name:
        name = fix_text(offer.findtext('name') or '')
    return name


def get_description(offer):
    """
    Читає опис товару.
    Пріоритет: description_ua → description.
    Обробляє CDATA і HTML-ентіті (opt-drop).
    """
    desc = offer.findtext('description_ua') or ''
    if not desc or not desc.strip():
        desc = offer.findtext('description') or ''
    return desc


def get_params(offer):
    """
    Читає характеристики товару.

    Нормалізує:
    - звичайні <param name="...">значення</param>
    - royaltoys: <param name="..."><value lang="uk">R R</value></param>
      → пропускаємо (немає корисного тексту)
    - пропускаємо порожні параметри

    Повертає список (name, value) або порожній список.
    """
    result = []
    for p in offer.findall('param'):
        # Читаємо прямий текст параметра
        val = fix_text(p.text)
        if not val:
            # Намагаємось знайти текст в <value lang="uk">
            for v in p.findall('value'):
                lang = v.get('lang', '').lower()
                if lang in ('uk', 'ua'):
                    val = fix_text(v.text)
                    break
            # Якщо українського нема — беремо перший будь-який
            if not val and p.findall('value'):
                val = fix_text(p.findall('value')[0].text)

        # Пропускаємо порожні і нерелевантні
        if not val or val in ('R R', 'r r'):
            continue

        name = (p.get('name') or '').strip()
        if name:
            result.append((name, val))

    return result


def get_article(offer):
    """
    Читає артикул товару.

    Підтримує:
    - <vendorCode> (shkatulka, lugi, dropom, kievopt)
    - <article>    (royaltoys)
    - <vendor_code> (інші варіанти)

    Повертає рядок (макс 255 символів) або '' якщо нема.
    """
    article = fix_text(
        offer.findtext('vendorCode') or
        offer.findtext('article')    or
        offer.findtext('vendor_code') or
        ''
    )
    return article[:255] if article else ''


def fetch_nbu_rates():
    """
    Отримує актуальні курси НБУ.
    API: https://bank.gov.ua/NBUStatService/v1/statdirectory/exchange?json
    Безкоштовно, без ключа. Повертає dict {ISO_CODE: rate_to_uah}.
    При будь-якій помилці — повертає FALLBACK_RATES без виключення.
    """
    try:
        r = requests.get(
            "https://bank.gov.ua/NBUStatService/v1/statdirectory/exchange?json",
            timeout=10
        )
        if not r.ok:
            return dict(FALLBACK_RATES)
        rates = {"UAH": 1.0}
        for item in r.json():
            code = item.get("cc", "").upper()
            rate = item.get("rate")
            if code and rate:
                rates[code] = float(rate)
        # RUR = RUB (деякі фіди використовують RUR)
        if "RUB" in rates:
            rates["RUR"] = rates["RUB"]
        print(f"[НБУ] Курси отримано: USD={rates.get('USD', '?'):.2f}, EUR={rates.get('EUR', '?'):.2f}")
        return rates
    except Exception as e:
        print(f"[НБУ] Помилка отримання курсів: {e} — використовуємо FALLBACK")
        return dict(FALLBACK_RATES)


import os

class Blacklist:
    def __init__(self, raw_lines_count=0):
        self.disable_all = False
        self.disabled_suppliers = set()  # set of domain patterns / prefixes
        self.disabled_categories = []    # list of (supplier_pattern, category_target_lower)
        self.disabled_keywords = []      # list of (supplier_pattern, keyword_lower)
        self.disabled_ids = set()        # set of uppercase offer_ids or articles
        self.total_entries = raw_lines_count

    def _match_supplier(self, pattern, domain, prefix):
        """
        Перевіряє чи збігається паттерн постачальника з його доменом чи префіксом.
        Підтримує:
        - "*" або "all" -> підходить для всіх
        - Повний домен: dropt.in.ua == dropt.in.ua
        - Префікс: 1000 == 1000
        - Короткий аліас: dropt in dropt.in.ua, shkatulka in www.shkatulka.in.ua
        """
        if not pattern or pattern in ('*', 'all'):
            return True
        p = pattern.lower().replace('www.', '').strip()
        d = domain.lower().replace('www.', '').strip()
        pref = prefix.lower().strip() if prefix else ""

        if p == d:
            return True
        if pref and p == pref:
            return True
        if p in d or d.startswith(p):
            return True
        return False

    def is_supplier_disabled(self, domain, id_prefix):
        if self.disable_all:
            return True
        for pattern in self.disabled_suppliers:
            if self._match_supplier(pattern, domain, id_prefix):
                return True
        return False

    def is_category_disabled(self, category_id, category_name, domain, id_prefix):
        if self.disable_all:
            return True
        if self.is_supplier_disabled(domain, id_prefix):
            return True

        cat_id_clean   = (category_id or '').lower().strip()
        cat_name_clean = (category_name or '').lower().strip()

        for supp_pattern, cat_target in self.disabled_categories:
            if self._match_supplier(supp_pattern, domain, id_prefix):
                # Перевірка за ID категорії
                if cat_id_clean and cat_id_clean == cat_target:
                    return True
                # Перевірка за назвою категорії (точне збігання або підрядок)
                if cat_name_clean and (cat_target == cat_name_clean or cat_target in cat_name_clean):
                    return True
        return False

    def is_offer_disabled(self, offer_id, offer_name, article, domain, id_prefix):
        if self.disable_all:
            return True
        if self.is_supplier_disabled(domain, id_prefix):
            return True
        if offer_id and offer_id.upper() in self.disabled_ids:
            return True
        if article and article.upper() in self.disabled_ids:
            return True

        # Перевірка за забороненими словами у назві товару
        name_clean = (offer_name or '').lower().strip()
        if name_clean:
            for supp_pattern, kw in self.disabled_keywords:
                if self._match_supplier(supp_pattern, domain, id_prefix):
                    if kw in name_clean:
                        return True
        return False

    def __contains__(self, offer_id):
        if self.disable_all:
            return True
        if offer_id.upper() in self.disabled_ids:
            return True
        parts = offer_id.split('_', 1)
        if len(parts) > 1:
            prefix = parts[0].lower()
            if prefix in self.disabled_suppliers:
                return True
        return False


def load_blacklist():
    """
    Читає blacklist.txt з підтримкою секцій:
    [DISABLED_SUPPLIERS]
    [DISABLED_CATEGORIES]
    [DISABLED_KEYWORDS]
    [STOP_ITEMS]
    Підтримує розділення як з двокрапкою ("dropt.in.ua : Павербанки"),
    так і просто через пробіл/пробіли ("dropt.in.ua Павербанки", "dropt Павербанки").
    """
    try:
        path = "blacklist.txt"
        if not os.path.exists(path) and os.path.exists("../blacklist.txt"):
            path = "../blacklist.txt"

        disable_all = False
        disabled_suppliers = set()
        disabled_categories = []
        disabled_keywords = []
        disabled_ids = set()
        raw_lines_count = 0

        current_section = "STOP_ITEMS"
        
        # Динамічно збираємо всі відомі домени та аліаси з SOURCES
        known_suppliers = {
            "1000", "2222", "3333", "4444", "5555", "7777", "8888", "9999", "1111", "1100", "1200", "1300", "2000", "3000",
            "dropt", "opt-drop", "optdrop", "lugi", "dropom", "kievopt", "royaltoys", "posudograd", "iposud", "i-posud",
            "websklad", "shkatulka", "forus", "aveon", "sonechko", "yavshoke", "dropshipping"
        }
        for _p, _id_p, url in SOURCES:
            d = url.split('/')[2].lower().replace('www.', '')
            known_suppliers.add(d)
            known_suppliers.add(d.split('.')[0])

        def parse_rule_line(line_str):
            if ":" in line_str:
                supp, val = line_str.split(":", 1)
                return supp.strip(), val.strip().lower()
            parts = line_str.split(None, 1)
            if len(parts) == 2:
                first_word = parts[0].lower().replace('www.', '').strip()
                is_supp = (
                    "." in first_word or
                    first_word in known_suppliers or
                    any(first_word in s for s in known_suppliers)
                )
                if is_supp:
                    return parts[0].strip(), parts[1].strip().lower()
            return "*", line_str.lower()

        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line_clean = line.strip()
                if not line_clean or line_clean.startswith('#'):
                    continue

                raw_lines_count += 1

                # Перевірка заголовків секцій
                if line_clean.startswith('[') and line_clean.endswith(']'):
                    sec = line_clean[1:-1].strip().upper()
                    if sec in ("DISABLED_SUPPLIERS", "SUPPLIERS"):
                        current_section = "SUPPLIERS"
                    elif sec in ("DISABLED_CATEGORIES", "CATEGORIES"):
                        current_section = "CATEGORIES"
                    elif sec in ("DISABLED_KEYWORDS", "KEYWORDS"):
                        current_section = "KEYWORDS"
                    elif sec in ("STOP_ITEMS", "DISABLED_IDS", "ITEMS", "GENERAL"):
                        current_section = "STOP_ITEMS"
                    continue

                val_upper = line_clean.upper()
                val_lower = line_clean.lower()

                if val_upper in ("ALL", "*", "VACATION", "DISABLE_ALL"):
                    disable_all = True
                    continue

                if current_section == "SUPPLIERS":
                    disabled_suppliers.add(val_lower)

                elif current_section == "CATEGORIES":
                    supp, cat = parse_rule_line(line_clean)
                    disabled_categories.append((supp, cat))

                elif current_section == "KEYWORDS":
                    supp, kw = parse_rule_line(line_clean)
                    disabled_keywords.append((supp, kw))

                elif current_section == "STOP_ITEMS":
                    # Захист зворотної сумісності для старих записів та префіксів cat: / keyword:
                    is_special = False
                    for pfx in ("category:", "cat:", "category_", "cat_"):
                        if val_lower.startswith(pfx):
                            rule_val = line_clean[len(pfx):].strip()
                            supp, cat = parse_rule_line(rule_val)
                            disabled_categories.append((supp, cat))
                            is_special = True
                            break

                    if not is_special:
                        for pfx in ("keyword:", "kw:", "keyword_", "kw_"):
                            if val_lower.startswith(pfx):
                                rule_val = line_clean[len(pfx):].strip()
                                supp, kw = parse_rule_line(rule_val)
                                disabled_keywords.append((supp, kw))
                                is_special = True
                                break

                    if not is_special:
                        if ":" in line_clean:
                            supp, cat = parse_rule_line(line_clean)
                            disabled_categories.append((supp, cat))
                        elif "." in val_lower or val_lower in known_suppliers:
                            disabled_suppliers.add(val_lower)
                        else:
                            disabled_ids.add(val_upper)

        blacklist_obj = Blacklist(raw_lines_count)
        blacklist_obj.disable_all = disable_all
        blacklist_obj.disabled_suppliers = disabled_suppliers
        blacklist_obj.disabled_categories = disabled_categories
        blacklist_obj.disabled_keywords = disabled_keywords
        blacklist_obj.disabled_ids = disabled_ids

        if disable_all:
            print(f"Blacklist: загальне вимкнення всього прайсу ({path})")
        else:
            print(
                f"Blacklist завантажено з {path}: "
                f"{len(disabled_suppliers)} постачальників, "
                f"{len(disabled_categories)} правил категорій, "
                f"{len(disabled_keywords)} правил слів, "
                f"{len(disabled_ids)} товарів/артикулів."
            )

        return blacklist_obj, raw_lines_count

    except FileNotFoundError:
        print("blacklist.txt не знайдено — крок пропускається")
        return Blacklist(0), 0


def normalize_feed_tags(root):
    """
    Приводить нестандартні теги до стандарту YML.

    Деякі постачальники (FORUS) використовують:
      <item>     замість <offer>
      <image>    замість <picture>
      parentID   замість parentId
    Якщо у фіді НЕМАЄ жодного <offer>, але є <item> — нормалізуємо теги,
    щоб уся подальша логіка (КРОК 3–10) працювала без змін.

    Стандартні фіди (з <offer>) НЕ зачіпаються — повертає False одразу.
    Повертає True, якщо нормалізація виконана.
    """
    if root.xpath(".//offer") or not root.xpath(".//item"):
        return False

    for it in root.xpath(".//item"):
        it.tag = "offer"
    for im in root.xpath(".//image"):
        im.tag = "picture"
    for cat in root.xpath(".//category[@parentID]"):
        cat.set("parentId", cat.get("parentID"))
        del cat.attrib["parentID"]

    return True


def validate_markup_config():
    """
    Перевіряє, що КОЖЕН постачальник зі SOURCES має наценку в CUSTOM_MARKUP
    (обов'язкові ключі markup_percent і markup_fixed).

    Глобальної наценки за замовчуванням більше немає, тож відсутність запису
    має зупиняти запуск з ЧІТКОЮ помилкою на старті — а не тихо ламатись
    посеред обробки чи відправляти товар без коректної наценки.

    Також перевіряє структуру markup_tiers якщо вони задані:
    - кожен тієр має бути кортежем з 3 елементів (max_price, percent, fixed)
    - тієри мають бути відсортовані за зростанням max_price
    """
    missing = []
    for _cat_prefix, _id_prefix, url in SOURCES:
        domain = url.split('/')[2]
        cfg = CUSTOM_MARKUP.get(domain)
        if not cfg or 'markup_percent' not in cfg or 'markup_fixed' not in cfg:
            missing.append(domain)
            continue
        tiers = cfg.get('markup_tiers')
        if tiers:
            for i, t in enumerate(tiers):
                if len(t) != 3:
                    raise SystemExit(
                        f"[КОНФІГ] markup_tiers[{i}] для '{domain}' має неправильний формат. "
                        f"Очікується кортеж (max_price, percent, fixed), отримано: {t}"
                    )
            prices = [t[0] for t in tiers]
            if prices != sorted(prices):
                raise SystemExit(
                    f"[КОНФІГ] markup_tiers для '{domain}' не відсортовані за зростанням ціни! "
                    f"Поточний порядок: {prices}"
                )
    if missing:
        raise SystemExit(
            "[КОНФІГ] Немає наценки в CUSTOM_MARKUP для: " + ", ".join(missing) +
            ". Додай markup_percent і markup_fixed для кожного з них."
        )
    print(f"[КОНФІГ] Наценку перевірено: усі {len(SOURCES)} постачальників мають явні значення")


def get_markup(price_uah, cfg):
    """
    Повертає (markup_percent, markup_fixed) залежно від ціни товару.

    Якщо в конфігу постачальника є markup_tiers — перебирає тієри по порядку
    і повертає перший, де price_uah <= max_price.
    Якщо ціна перевищує всі тієри — повертає значення останнього тієру.
    Якщо markup_tiers відсутній або порожній — повертає глобальний
    markup_percent / markup_fixed (стара логіка без тієрів).

    Аргументи:
        price_uah (float): ціна товару постачальника у гривнях
        cfg (dict):        запис із CUSTOM_MARKUP для поточного домену

    Повертає:
        (float, float): (markup_percent, markup_fixed)
    """
    tiers = cfg.get('markup_tiers')
    if tiers:                                 # None або [] → стара логіка
        for max_price, pct, fixed in tiers:
            if price_uah <= max_price:
                return pct, fixed
        # Ціна вища за всі тієри — беремо останній
        return tiers[-1][1], tiers[-1][2]
    # Тієрів немає — стара плоска логіка
    return cfg['markup_percent'], cfg['markup_fixed']


# ==============================================================================
# 3. ГОЛОВНА ФУНКЦІЯ
# ==============================================================================

def process():
    final_categories = {}
    category_id_map  = {}
    price_warnings   = []
    source_results   = []

    report_stats     = {}
    cross_duplicates = []
    inner_duplicates = []
    blacklist_hits   = defaultdict(int)
    category_errors  = []

    print("--- СТАРТ ОБРОБКИ ---")

    # Перевірка конфігу наценок ПЕРЕД будь-якими запитами — fail fast
    validate_markup_config()

    # --------------------------------------------------------------------------
    # КРОК 0: Актуальні курси НБУ (один запит на початку, далі не звертаємось)
    # --------------------------------------------------------------------------
    live_rates = fetch_nbu_rates()
    # Оновлюємо глобальні FALLBACK_RATES актуальними курсами
    FALLBACK_RATES.update(live_rates)

    # --------------------------------------------------------------------------
    # КРОК 1: Завантаження blacklist
    # --------------------------------------------------------------------------
    blacklisted_ids, blacklist_count = load_blacklist()

    # --------------------------------------------------------------------------
    # КРОК 2: Завантаження всіх фідів в пам'ять
    # Затримка REQUEST_DELAY секунд між запитами — щоб не отримати 429
    # --------------------------------------------------------------------------
    feeds = []

    # Якщо увімкнено загальне вимкнення прайсу - пропускаємо завантаження фідів
    if blacklisted_ids.disable_all:
        print("[BLACKLIST] Увімкнено загальне вимкнення прайсу (VACATION/DISABLE_ALL). Завантаження фідів скасовано.")
        return

    for i, (prefix, id_prefix, url) in enumerate(SOURCES):
        domain = url.split('/')[2]

        # Перевірка вимкнення постачальника перед завантаженням фідів (економить час та трафік)
        if blacklisted_ids.is_supplier_disabled(domain, prefix):
            print(f"[BLACKLIST] Постачальник {domain} вимкнений у blacklist.txt — пропускаємо завантаження")
            report_stats[domain] = {
                "ok": 0, "low": 0, "not_avail": 0, "price_err": 0, "duplicate": 0, 
                "blacklist": 0, "default_qty": 0,
                "name_ua": 0, "name_ru": 0, "desc_ua": 0, "desc_ru": 0, "desc_none": 0,
                "multi_pic": 0, "no_params": 0, "no_article": 0, "saved_cat": 0,
                "avg_pics": 0, "avg_params": 0, "price_min": 0, "price_max": 0
            }
            continue

        # Затримка між запитами (крім першого)
        if i > 0:
            time.sleep(REQUEST_DELAY)

        last_error = None
        for attempt in range(1, MAX_FETCH_ATTEMPTS + 1):
            # Експоненційний backoff: 20 → 40 → 80 → 120 → 120… (стеля RETRY_BACKOFF_MAX)
            backoff = min(RETRY_BACKOFF_BASE * (2 ** (attempt - 1)), RETRY_BACKOFF_MAX)
            try:
                # Браузероподібні заголовки — деякі сервери ріжуть «голі» запити
                r = requests.get(url, timeout=120, headers={
                    'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                                   'AppleWebKit/537.36 (KHTML, like Gecko) '
                                   'Chrome/124.0 Safari/537.36'),
                    'Accept': 'application/xml,text/xml,*/*;q=0.9',
                    'Accept-Language': 'uk,ru;q=0.9,en;q=0.8',
                })
                if not r.ok:
                    last_error = f"HTTP {r.status_code}"
                    if attempt < MAX_FETCH_ATTEMPTS:
                        # 429/503 часто віддають Retry-After — поважаємо його
                        wait = backoff
                        if r.status_code in (429, 503):
                            ra = r.headers.get('Retry-After', '').strip()
                            if ra.isdigit():
                                wait = max(int(ra), backoff)
                        print(f"[RETRY {attempt}/{MAX_FETCH_ATTEMPTS}] {domain}: {last_error} — повтор через {wait}с")
                        time.sleep(wait)
                        continue
                    print(f"[HTTP ERROR] {domain}: {r.status_code}")
                    report_stats[domain] = {"http_error": r.status_code}
                    break

                # Перевірка Content-Type — захист від HTML-сторінки замість XML
                ct = r.headers.get('Content-Type', '')
                if 'html' in ct and 'xml' not in ct and len(r.content) < 50_000:
                    last_error = f"Content-Type={ct} (можливо HTML замість XML)"
                    if attempt < MAX_FETCH_ATTEMPTS:
                        print(f"[RETRY {attempt}/{MAX_FETCH_ATTEMPTS}] {domain}: {last_error} — повтор через {backoff}с")
                        time.sleep(backoff)
                        continue

                root           = ET.fromstring(r.content, parser=ET.XMLParser(recover=True))
                if normalize_feed_tags(root):
                    print(f"[{domain}] Нестандартні теги (item/image/parentID) нормалізовано")
                currency_rates = get_currency_rates(root)
                visible_rates  = {k: v for k, v in currency_rates.items() if k in ('UAH', 'USD', 'EUR')}
                print(f"[{domain}] Завантажено (спроба {attempt}). Курси: {visible_rates}")
                feeds.append((prefix, id_prefix, url, domain, root, currency_rates))
                break

            except Exception as e:
                last_error = str(e)
                if attempt < MAX_FETCH_ATTEMPTS:
                    print(f"[RETRY {attempt}/{MAX_FETCH_ATTEMPTS}] {domain}: {e} — повтор через {backoff}с")
                    time.sleep(backoff)
                else:
                    print(f"[ПОМИЛКА ФІДУ] {domain}: {e}")
                    report_stats[domain] = {"feed_error": last_error}

    # --------------------------------------------------------------------------
    # КРОК 3: ПРОХІД 1 — збір всіх offer id для виявлення дублікатів
    # XML вже в пам'яті — жодних додаткових запитів
    # --------------------------------------------------------------------------
    id_registry = defaultdict(list)

    for prefix, id_prefix, url, domain, root, currency_rates in feeds:
        for offer in root.xpath(".//offer"):
            raw_id   = offer.get('id', '').strip().upper()
            if not raw_id:
                continue
            offer_id  = f"{id_prefix}_{raw_id}" if id_prefix else raw_id
            price_nodes = offer.xpath('./price')
            price_text  = price_nodes[0].text if price_nodes else ''
            id_registry[offer_id].append((domain, price_text or ''))

    # Визначаємо конфліктні id (ТІЛЬКИ між РІЗНИМИ постачальниками)
    conflict_ids = set()
    for offer_id, entries in id_registry.items():
        if len(entries) > 1:
            domains = [e[0] for e in entries]
            if len(set(domains)) == 1:
                # Внутрішній дублікат одного постачальника (наприклад yavshoke або dropt)
                # 1 товар буде залишено при проході, решта зафіксована у звіт
                inner_duplicates.append({
                    "offer_id": offer_id,
                    "domain":   domains[0],
                    "count":    len(entries)
                })
            else:
                # Конфлікт між РІЗНИМИ постачальниками — повністю блокуємо
                cross_duplicates.append({
                    "offer_id": offer_id,
                    "entries":  entries
                })
                conflict_ids.add(offer_id)

    print(f"\nДублікати між постачальниками (видаляються повністю): {len(cross_duplicates)}")
    print(f"Дублікати всередині постачальника (залишається 1 товар): {len(inner_duplicates)}")

    # --------------------------------------------------------------------------
    # КРОК 4: Обробка категорій (всі фіди)
    # --------------------------------------------------------------------------
    for prefix, id_prefix, url, domain, root, currency_rates in feeds:
        for cat in root.xpath(".//category"):
            orig_id = cat.get('id')
            if not orig_id:
                continue
            new_id = f"{prefix}{orig_id}" if prefix else orig_id

            # while замість if — обробляє будь-яку кількість колізій
            while new_id in category_id_map and category_id_map[new_id] != domain:
                new_id = f"{new_id}9"

            category_id_map[new_id] = domain
            cat.set('id', new_id)

            if cat.get('parentId'):
                parent = cat.get('parentId')
                cat.set('parentId', f"{prefix}{parent}" if prefix else parent)

            # Нормалізуємо назву категорії — HTML-entities можуть зламати XML
            if cat.text:
                cat.text = fix_text(cat.text)

            # Застосовуємо фіксований переклад назви категорії (якщо є для цього домену)
            _cat_overrides = CATEGORY_NAME_OVERRIDES.get(domain, {})
            if _cat_overrides and cat.text and cat.text in _cat_overrides:
                cat.text = _cat_overrides[cat.text]

            final_categories[new_id] = cat

    # --------------------------------------------------------------------------
    # КРОК 5: ПРОХІД 2 — основна обробка товарів
    # --------------------------------------------------------------------------
    processed_offers = []
    seen_offer_ids   = set()

    for prefix, id_prefix, url, domain, root, currency_rates in feeds:
        count_ok          = 0
        count_low         = 0
        count_no          = 0
        count_price_err   = 0
        count_duplicate   = 0
        count_blacklist   = 0
        count_default_qty = 0
        
        # Статистика мов назв та описів (надійна, за наявністю тегів)
        count_name_ua            = 0  # оригінальна назва українською (name_ua)
        count_name_ru_translated = 0  # перекладена назва з російської (name)
        count_desc_ua            = 0  # оригінальний опис українською (description_ua)
        count_desc_ru_translated = 0  # перекладений опис з російської (description)
        count_desc_fallback      = 0  # використано заглушку (опис порожній або <30 символів)
        
        count_multi_pic   = 0
        count_no_params   = 0
        count_no_article  = 0
        count_saved_category = 0  # врятовано товарів у категорію "Інші"
        
        total_pics        = 0  # сума фото для середнього
        total_params      = 0  # сума параметрів для середнього
        
        price_min         = float('inf')
        price_max         = 0.0

        for offer in root.xpath(".//offer"):
            raw_id   = offer.get('id', '').strip().upper()
            if not raw_id:
                continue
            offer_id  = f"{id_prefix}_{raw_id}" if id_prefix else raw_id

            # Зчитуємо артикул, оригінальну категорію та назву для перевірки блеклиста
            article = get_article(offer)
            orig_cat = offer.findtext('categoryId') or ''
            cat_id   = f"{prefix}{orig_cat}" if prefix else orig_cat
            cat_element = final_categories.get(cat_id)
            cat_name = cat_element.text if cat_element is not None else ""
            offer_name = get_name(offer)

            # -- Перевірка 1: blacklist (загальне вимкнення, постачальник, категорія, ключові слова, ID товару або артикул) --
            if (blacklisted_ids.is_supplier_disabled(domain, prefix) or
                blacklisted_ids.is_category_disabled(cat_id, cat_name, domain, prefix) or
                blacklisted_ids.is_offer_disabled(offer_id, offer_name, article, domain, prefix)):
                
                blacklist_hits[domain] += 1
                count_blacklist += 1
                continue

            # -- Перевірка 2: дублікати --
            # a) Міжрізні постачальники — повне видалення
            if offer_id in conflict_ids:
                count_duplicate += 1
                continue

            # b) Внутрішні дублікати одного постачальника (або між прайсами yavshoke) — залишаємо 1-й товар
            if offer_id in seen_offer_ids:
                count_duplicate += 1
                continue

            # -- Перевірка 3: наявність --
            # Використовуємо get_availability() яка враховує available і in_stock
            if not get_availability(offer):
                count_no += 1
                continue

            # -- Кількість на складі --
            qty, used_default = get_qty(offer)
            if used_default:
                count_default_qty += 1

            # -- Перевірка 4: ціна --
            # offer.xpath('./price') — тільки ПРЯМИЙ нащадок поточного offer
            # Захист від Lugi де price стоїть після картинок
            price_nodes = offer.xpath('./price')
            if not price_nodes or not (price_nodes[0].text or '').strip():
                count_price_err += 1
                continue
            p_node = price_nodes[0]

            # Перевірка price from="true" (ціна з діапазону — мінімальна)
            if p_node.get('from', 'false').lower() == 'true':
                price_warnings.append(
                    f"[ЦІНА З ДІАПАЗОНУ] {domain} offer={offer_id} "
                    f"price='{p_node.text}' — мінімальна ціна з діапазону"
                )

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
                currency_id     = (offer.findtext('currencyId') or 'UAH').strip().upper()
                price_uah, warn = convert_to_uah(raw_p, currency_id, currency_rates, domain, offer_id)

                if warn:
                    price_warnings.append(warn)
                if price_uah is None:
                    count_price_err += 1
                    continue

                # Крок 3: тієрна наценка (домен гарантовано є — перевірено на старті)
                cfg                = CUSTOM_MARKUP[domain]
                m_percent, m_fixed = get_markup(price_uah, cfg)   # обирає тієр за ціною

                price     = round(price_uah * m_percent + m_fixed)
                old_price = round(price * OLD_PRICE_MULT)

                # Фільтр мінімальної ціни
                # min_price_raw   — поріг від ціни постачальника (до наценки)
                # min_price_final — поріг від фінальної ціни (після наценки)
                # якщо жодного нема — глобальний MIN_PRICE_THRESHOLD (фінальна)
                min_raw   = cfg.get("min_price_raw")
                min_final = cfg.get("min_price_final")
                if min_raw is not None:
                    if price_uah < min_raw:
                        count_low += 1
                        continue
                elif min_final is not None:
                    if price < min_final:
                        count_low += 1
                        continue
                elif price < MIN_PRICE_THRESHOLD:
                    count_low += 1
                    continue

                # Захист: наша фінальна ціна не може бути меншою за оригінальну
                # (допускаємо -1 грн похибки через round(), більше — помилка наценки)
                if price < price_uah - 1:
                    price_warnings.append(
                        f"[ЦІНА НИЖЧА ЗА ОРИГІНАЛ] {domain} offer={offer_id} "
                        f"original={price_uah:.0f} UAH our_price={price} UAH — видаляємо"
                    )
                    count_price_err += 1
                    continue

                # -- Збірка полів товару через нормалізуючі функції --
                vendor  = fix_text(offer.findtext('vendor') or '') or 'NoBrand'
                name_ua = ru_to_ua(get_name(offer))

                # Якщо назва порожня або занадто коротка — пропускаємо товар
                if not name_ua or len(name_ua.strip()) < 3:
                    count_price_err += 1
                    continue

                # Додаємо бренд в назву якщо його там нема
                if vendor != 'NoBrand' and vendor.lower() not in name_ua.lower():
                    name_ua = f"{name_ua} {vendor}"

                # Опис
                desc_raw = get_description(offer)
                desc     = ru_to_ua(clean_description(desc_raw, name_ua, vendor))

                # Категорія
                orig_cat = offer.findtext('categoryId') or ''
                cat_id   = f"{prefix}{orig_cat}" if prefix else orig_cat

                # Якщо категорія відсутня у списку категорій постачальника -
                # переносимо товар у фіксовану категорію "Інші товари [Постачальник]"
                # Це дозволяє врятувати близько 1000 товарів без зміни існуючих категорій.
                is_saved_cat = False
                if cat_id not in final_categories:
                    fixed_cat_id = f"{prefix}99999" if prefix else "99999"
                    clean_domain = domain.replace('www.', '')
                    cat_name = f"Інші товари {clean_domain}"
                    
                    # Додаємо фіксовану категорію в XML, якщо її ще немає
                    if fixed_cat_id not in final_categories:
                        new_cat = ET.Element("category", id=fixed_cat_id)
                        new_cat.text = cat_name
                        final_categories[fixed_cat_id] = new_cat
                    
                    # Перепризначаємо категорію для товару
                    cat_id = fixed_cat_id
                    is_saved_cat = True

                # -- Збірка XML елемента товару --
                # Порядок тегів відповідає прикладу з документації EVA
                # https://sellersupport.eva.ua/article/pidhotovka-prays-listu-xml
                new_off = ET.Element("offer", id=offer_id, available="true")

                ET.SubElement(new_off, "price").text          = str(price)
                ET.SubElement(new_off, "price_old").text      = str(old_price)
                ET.SubElement(new_off, "stock_quantity").text = str(min(qty, 9999))
                ET.SubElement(new_off, "currencyId").text     = "UAH"
                ET.SubElement(new_off, "categoryId").text     = cat_id

                # Картинки (мін 1 обов'язково, макс 15 за вимогою EVA)
                # Валідуємо URL (тільки http/https) та дедублікуємо
                pic_count = 0
                seen_pics = set()
                for pic in offer.findall('picture'):
                    if pic_count >= 15:
                        break
                    url_val = (pic.text or '').strip()
                    if url_val and url_val.startswith(('http://', 'https://')) and url_val not in seen_pics:
                        ET.SubElement(new_off, "picture").text = url_val
                        seen_pics.add(url_val)
                        pic_count += 1
                if pic_count == 0:
                    price_warnings.append(
                        f"[БЕЗ ФОТО] {domain} offer={offer_id} — пропускаємо"
                    )
                    count_price_err += 1
                    continue

                ET.SubElement(new_off, "vendor").text         = vendor

                # Артикул товару (якщо є)
                if article:
                    ET.SubElement(new_off, "article").text    = article

                ET.SubElement(new_off, "name_ua").text        = name_ua[:250]
                ET.SubElement(new_off, "description_ua").text = ET.CDATA(desc)

                # Параметри через нормалізуючу функцію
                params = get_params(offer)
                if not params:
                    # Якщо параметрів нема — додаємо базові
                    ET.SubElement(new_off, "param", name="Стан").text  = "Новий"
                    ET.SubElement(new_off, "param", name="Колір").text = "Комбінований"
                    ET.SubElement(new_off, "param", name="Вага").text  = "-"
                    ET.SubElement(new_off, "param", name="Розмір Size").text = "-"
                else:
                    for p_name, p_val in params:
                        ET.SubElement(new_off, "param", name=p_name[:100]).text = p_val[:255]
                    # Додаємо Розмір Size якщо його нема серед існуючих параметрів
                    existing_names = [p[0].lower() for p in params]
                    has_size = any(
                        w in n
                        for n in existing_names
                        for w in ('розмір', 'размер', 'size', 'габарит')
                    )
                    if not has_size:
                        ET.SubElement(new_off, "param", name="Розмір Size").text = "-"

                # -- Статистика якості даних для звіту --
                # Назва: чи була оригінально українською у фіді (тег name_ua)
                has_orig_name_ua = bool((offer.findtext('name_ua') or '').strip())
                if has_orig_name_ua:
                    count_name_ua += 1
                else:
                    count_name_ru_translated += 1

                # Опис: чи використано заглушку через порожній або короткий опис
                plain_desc = re.sub(r'<[^>]+>', '', desc_raw).strip() if desc_raw else ""
                if not desc_raw or len(plain_desc) < 30:
                    count_desc_fallback += 1
                else:
                    # Якщо опис є, перевіряємо чи він початково був українською
                    orig_desc_ua = (offer.findtext('description_ua') or '').strip()
                    if orig_desc_ua:
                        count_desc_ua += 1
                    else:
                        count_desc_ru_translated += 1

                if pic_count >= 2:  count_multi_pic  += 1
                if not params:      count_no_params  += 1
                if not article:     count_no_article += 1
                if is_saved_cat:    count_saved_category += 1
                
                total_pics   += pic_count
                total_params += len(params)
                
                if price < price_min: price_min = price
                if price > price_max: price_max = price

                processed_offers.append(new_off)
                count_ok += 1

            except Exception as e:
                price_warnings.append(
                    f"[ВИНЯТОК] {domain} offer={offer_id} "
                    f"price='{p_node.text if p_node is not None else 'N/A'}' err={e}"
                )
                count_price_err += 1
                continue

        source_label = get_source_label(url)
        report_stats[source_label] = {
            "ok":           count_ok,
            "low":          count_low,
            "not_avail":    count_no,
            "price_err":    count_price_err,
            "duplicate":    count_duplicate,
            "blacklist":    count_blacklist,
            "default_qty":  count_default_qty,
            
            # Якість даних
            "name_ua":      count_name_ua,
            "name_ru":      count_name_ru_translated,
            "desc_ua":      count_desc_ua,
            "desc_ru":      count_desc_ru_translated,
            "desc_none":    count_desc_fallback,
            "multi_pic":    count_multi_pic,
            "no_params":    count_no_params,
            "no_article":   count_no_article,
            
            # Додаткові метрики якості
            "saved_cat":    count_saved_category,
            "avg_pics":     round(total_pics / count_ok, 1) if count_ok > 0 else 0,
            "avg_params":   round(total_params / count_ok, 1) if count_ok > 0 else 0,
            
            "price_min":    int(price_min) if price_min != float('inf') else 0,
            "price_max":    int(price_max),
        }
        source_results.append(
            f"{source_label}: OK={count_ok} | LOW={count_low} | NOT_AVAIL={count_no} | "
            f"PRICE_ERR={count_price_err} | DUPL={count_duplicate} | "
            f"SAVED_CAT={count_saved_category}"
        )

    # --------------------------------------------------------------------------
    # КРОК 6: Валідація categoryId перед записом XML
    # Видаляємо товари що посилаються на неіснуючу категорію
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
    # КРОК 6.5: Видалення тонких/порожніх категорій (≤ MIN_OFFERS_PER_CATEGORY)
    # Батьківські категорії (ті що мають дочірні) НЕ видаляються — щоб не
    # зламати parentId дочірніх категорій.
    # --------------------------------------------------------------------------

    # Категорії що є чиїмось parentId — не чіпаємо
    parent_ids = {
        cat_el.get('parentId')
        for cat_el in final_categories.values()
        if cat_el.get('parentId') in final_categories
    }

    # Кількість прямих товарів для кожної категорії
    cat_direct_counts = Counter(
        offer.findtext('categoryId') for offer in valid_offers
    )

    # Тонкі/порожні категорії: ≤ порогу І не батьківські
    thin_cats = {
        cat_id
        for cat_id in final_categories
        if cat_direct_counts.get(cat_id, 0) <= MIN_OFFERS_PER_CATEGORY
        and cat_id not in parent_ids
    }

    if thin_cats:
        # Видаляємо товари що належать тонким категоріям
        new_valid = []
        thin_offers_removed = 0
        for offer in valid_offers:
            if offer.findtext('categoryId') in thin_cats:
                thin_offers_removed += 1
            else:
                new_valid.append(offer)
        valid_offers = new_valid

        # Видаляємо самі категорії
        for cat_id in thin_cats:
            del final_categories[cat_id]

        empty_removed = sum(
            1 for c in thin_cats if cat_direct_counts.get(c, 0) == 0
        )
        thin_removed = len(thin_cats) - empty_removed
        print(
            f"\n[КАТЕГОРІЇ] Видалено {len(thin_cats)} категорій "
            f"({empty_removed} порожніх + {thin_removed} з ≤{MIN_OFFERS_PER_CATEGORY} товарів), "
            f"{thin_offers_removed} товарів видалено"
        )
    else:
        print(f"\n[КАТЕГОРІЇ] Тонких/порожніх категорій не знайдено")

    # --------------------------------------------------------------------------
    # КРОК 7: Збірка фінального XML
    # --------------------------------------------------------------------------
    yml  = ET.Element("yml_catalog", date=datetime.now().strftime("%Y-%m-%d %H:%M"))
    shop = ET.SubElement(yml, "shop")
    ET.SubElement(shop, "name").text = "AVI"
    ET.SubElement(shop, "url").text  = "https://avi.in.ua"

    currencies = ET.SubElement(shop, "currencies")
    ET.SubElement(currencies, "currency", id="UAH", rate="1")

    cats_n = ET.SubElement(shop, "categories")
    for c in final_categories.values():
        cats_n.append(c)

    offers_n = ET.SubElement(shop, "offers")
    for o in valid_offers:
        offers_n.append(o)

    with open("Masterevanew.xml", "wb") as f:
        # Генеруємо XML
        xml_bytes = ET.tostring(yml, encoding='UTF-8', xml_declaration=True, pretty_print=True)
        # Додаємо DOCTYPE після XML декларації (як вимагає стандарт YML і EVA)
        xml_bytes = xml_bytes.replace(
            b"<?xml version='1.0' encoding='UTF-8'?>\n",
            b'<?xml version="1.0" encoding="UTF-8"?>\n<!DOCTYPE yml_catalog SYSTEM "shops.dtd">\n',
            1
        )
        # На випадок якщо lxml використовує подвійні лапки
        xml_bytes = xml_bytes.replace(
            b'<?xml version="1.0" encoding="UTF-8"?>\n<?xml',
            b'<?xml'
        )
        # Якщо DOCTYPE вже не додався через інший формат декларації
        if b'<!DOCTYPE' not in xml_bytes:
            xml_bytes = xml_bytes.replace(
                b'<?xml version="1.0" encoding="UTF-8"?>\n',
                b'<?xml version="1.0" encoding="UTF-8"?>\n<!DOCTYPE yml_catalog SYSTEM "shops.dtd">\n',
                1
            )
        f.write(xml_bytes)

    # --------------------------------------------------------------------------
    # КРОК 8: Збереження price_warnings.log
    # --------------------------------------------------------------------------
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
    for prefix, id_prefix, url in SOURCES:
        source_label = get_source_label(url)
        v = report_stats.get(source_label, {})
        if "http_error" in v:
            md.append(f"| {source_label} | 🔴 HTTP {v['http_error']} | — | — | — | — | — | — |")
        elif "feed_error" in v:
            md.append(f"| {source_label} | 🔴 ПОМИЛКА ЗАВАНТАЖЕННЯ | — | — | — | — | — | — |")
        else:
            md.append(
                f"| {source_label} "
                f"| {v.get('ok', 0)} "
                f"| {v.get('low', 0)} "
                f"| {v.get('not_avail', 0)} "
                f"| {v.get('price_err', 0)} "
                f"| {v.get('default_qty', 0)} "
                f"| {v.get('duplicate', 0)} "
                f"| {v.get('blacklist', 0)} |"
            )

    md.append("\n## Якість даних по постачальниках")
    md.append("| Постачальник | 🇺🇦 Назва UA (ориг) | 🔄 Назва UA (переклад) | 🇺🇦 Опис UA (ориг) | 🔄 Опис UA (переклад) | ⚠️ Опис (заглушка) | 📸 2+ фото (Сер) | ⚙️ Без парамів (Сер) | 🏷️ Врятовано в \"Інші\" | 💰 Ціна min–max |")
    md.append("|---|---|---|---|---|---|---|---|---|---|")
    for prefix, id_prefix, url in SOURCES:
        source_label = get_source_label(url)
        v = report_stats.get(source_label, {})
        if "http_error" in v or "feed_error" in v:
            md.append(f"| {source_label} | — | — | — | — | — | — | — | — | — |")
        else:
            p_min = v.get('price_min', 0)
            p_max = v.get('price_max', 0)
            price_range = f"{p_min}–{p_max} грн" if p_max > 0 else "—"
            
            avg_pics_str = f"{v.get('multi_pic', 0)} ({v.get('avg_pics', 0)})"
            avg_params_str = f"{v.get('no_params', 0)} ({v.get('avg_params', 0)})"
            
            md.append(
                f"| {source_label} "
                f"| {v.get('name_ua', 0)} "
                f"| {v.get('name_ru', 0)} "
                f"| {v.get('desc_ua', 0)} "
                f"| {v.get('desc_ru', 0)} "
                f"| {v.get('desc_none', 0)} "
                f"| {avg_pics_str} "
                f"| {avg_params_str} "
                f"| **{v.get('saved_cat', 0)}** "
                f"| {price_range} |"
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
