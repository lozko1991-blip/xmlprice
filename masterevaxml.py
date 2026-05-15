import requests
import lxml.etree as ET
from datetime import datetime
import re
import time
from collections import defaultdict
from html import unescape

# Налаштування джерел
SOURCES = [
    "https://shkatulka.in.ua/content/export/cb28b41c71e755eab59d094a399ecfd8.xml", # 1
    "https://opt-drop.com/storage/xml/opt-drop-5.xml",                             # 2
    "https://feed.lugi.com.ua/index.php?route=extension/feed/unixml/ukr_ru",       # 3
    "https://dropom.com.ua/products_feed.xml?hash_tag=b55924e4ebc0576fda79ae6941f7a2a5&languages=uk%2Cru", # 4
    "http://kievopt.com.ua/prices/rozetka-22294.yml",                             # 5
    "https://dwn.royaltoys.com.ua/my/export/v2/e6f6dcf6-2539-4a43-a285-32667169f0db.xml" # 6
]

MARKUP_PERCENT = 1.35
MARKUP_FIXED = 40
MIN_PRICE_THRESHOLD = 150
MAX_RETRIES = 3
RETRY_DELAY = 5

STOP_WORDS = [
    r"предоплата", r"передплата", r"наложка", r"накладений платіж", r"самовивіз",
    r"позвоніть", r"зателефонуйте", r"напишіть", r"пишіть", r"дзвоніть",
    r"вайбер", r"viber", r"телеграм", r"telegram", r"сайт", r"магазин"
]

def fetch_with_retry(url):
    # Додано повний User-Agent, щоб сайти не блокували запити
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36'}
    for i in range(MAX_RETRIES):
        try:
            r = requests.get(url, headers=headers, timeout=120)
            if r.ok:
                return r.content
        except Exception as e:
            print(f"Спроба {i+1} помилка для {url.split('/')[2]}: {e}")
            if i < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
    return None

def clean_text_soft(text):
    if not text: return ""
    text = unescape(text)
    text = re.sub(r'<(script|style).*?>.*?</\1>', '', text, flags=re.DOTALL)
    text = re.sub(r'<(p|br|li|div|h[1-6]).*?>', '\n', text)
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'(\+?38)?\s?\(?\d{3}\)?[\s\.-]?\d{3}[\s\.-]?\d{2}[\s\.-]?\d{2}', '', text)
    text = re.sub(r'https?://\S+|www\.\S+', '', text)
    for word in STOP_WORDS:
        text = re.sub(word, '', text, flags=re.IGNORECASE)
    lines = [line.strip() for line in text.split('\n')]
    return '\n'.join(line for line in lines if line)

def get_quantity(offer):
    avail_attr = offer.get('available')
    avail_tag = offer.findtext('available')
    # Перевірка всіх варіантів статусу наявності
    is_available_status = (avail_attr in ['true', 'yes', '1']) or (avail_tag in ['true', 'yes', '1'])
    
    q_nodes = offer.xpath(".//quantity | .//quantity_in_stock | .//stock_quantity")
    if q_nodes:
        try:
            val = int(q_nodes[0].text)
            if val >= 1: return val
            if val <= 0 and is_available_status: return 3
        except: pass
    
    return 3 if is_available_status else 0

def process():
    all_raw_data = []
    id_usage_count = {}
    source_stats = []

    print("--- ЗАВАНТАЖЕННЯ ДАНИХ ---")
    for index, url in enumerate(SOURCES):
        domain = url.split('/')[2]
        content = fetch_with_retry(url)
        
        if content:
            try:
                parser = ET.XMLParser(recover=True, encoding='utf-8')
                root = ET.fromstring(content, parser=parser)
                
                categories = root.xpath(".//category")
                offers = root.xpath(".//offer")
                
                # Реєструємо категорії для уникнення дублів
                current_source_ids = {c.get('id') for c in categories if c.get('id')}
                for cid in current_source_ids:
                    id_usage_count[cid] = id_usage_count.get(cid, 0) + 1
                
                all_raw_data.append({
                    'prefix': str(index + 1),
                    'domain': domain,
                    'categories': categories,
                    'offers': offers
                })
                print(f"Знайдено у {domain}: {len(offers)} товарів")
            except Exception as e:
                print(f"Помилка структури {domain}: {e}")
        else:
            print(f"НЕ ВДАЛОСЯ отримати дані з {domain}")

    duplicate_ids = {cid for cid, count in id_usage_count.items() if count > 1}
    processed_offers = []
    category_product_count = defaultdict(int)
    final_categories_map = {}

    print("\n--- ОБРОБКА ТОВАРІВ ---")
    for data in all_raw_data:
        prefix = data['prefix']
        domain = data['domain']
        count_ok = 0
        count_skip_price = 0
        count_skip_qty = 0
        
        # Крок 1: Обробка категорій постачальника
        for cat in data['categories']:
            cid = cat.get('id')
            f_id = f"{prefix}_{cid}" if cid in duplicate_ids else cid
            cat.set('id', f_id)
            pid = cat.get('parentId')
            if pid:
                cat.set('parentId', f"{prefix}_{pid}" if pid in duplicate_ids else pid)
            final_categories_map[f_id] = cat

        # Крок 2: Обробка товарів
        for offer in data['offers']:
            # 1. Наявність
            qty = get_quantity(offer)
            if qty <= 0:
                count_skip_qty += 1
                continue
            
            # 2. Назва
            name_node = offer.find('name_ua') or offer.find('name')
            if name_node is None or not name_node.text:
                continue
            item_name = name_node.text

            # 3. Ціна
            p_node = offer.find('price')
            if p_node is None or not p_node.text:
                continue
            
            try:
                raw_p = p_node.text.replace(',', '.')
                price = round(float(raw_p) * MARKUP_PERCENT + MARKUP_FIXED)
                
                if price < MIN_PRICE_THRESHOLD:
                    count_skip_price += 1
                    continue
                
                # Створюємо чистий об'єкт для EVA
                new_offer = ET.Element("offer", id=f"{prefix}_{offer.get('id')}", available="true")
                ET.SubElement(new_offer, "name").text = item_name
                ET.SubElement(new_offer, "price").text = str(price)
                
                op_node = offer.find('oldprice')
                if op_node is not None and op_node.text:
                    try:
                        old_p = round(float(op_node.text.replace(',', '.')) * MARKUP_PERCENT + MARKUP_FIXED)
                        ET.SubElement(new_offer, "oldprice").text = str(old_p)
                    except: pass
                
                # Опис
                raw_desc = offer.findtext('description_ua') or offer.findtext('description') or ""
                ET.SubElement(new_offer, "description").text = ET.CDATA(clean_text_soft(raw_desc))

                # Категорія
                cat_node = offer.find('categoryId')
                if cat_node is not None:
                    cid = cat_node.text
                    f_cid = f"{prefix}_{cid}" if cid in duplicate_ids else cid
                    ET.SubElement(new_offer, "categoryId").text = f_cid
                    category_product_count[f_cid] += 1

                # Артикул та картинки
                v_code = offer.findtext('vendorCode') or offer.findtext('article') or ""
                ET.SubElement(new_offer, "vendorCode").text = v_code
                for pic in offer.findall('picture'):
                    if pic.text:
                        ET.SubElement(new_offer, "picture").text = pic.text
                
                ET.SubElement(new_offer, "quantity").text = str(qty)
                
                processed_offers.append(new_offer)
                count_ok += 1
            except Exception:
                continue
            
        source_stats.append(f"{domain}: Додано {count_ok} (Пропущено ціна: {count_skip_price}, Пропущено залишок: {count_skip_qty})")

    # Збірка фінального XML
    yml = ET.Element("yml_catalog", date=datetime.now().strftime("%Y-%m-%d %H:%M"))
    shop = ET.SubElement(yml, "shop")
    ET.SubElement(shop, "name").text = "Master Shop EVA"
    currs = ET.SubElement(shop, "currencies")
    ET.SubElement(currs, "currency", id="UAH", rate="1")
    
    cats_node = ET.SubElement(shop, "categories")
    for f_id, cat_obj in final_categories_map.items():
        if category_product_count.get(f_id, 0) > 0:
            cats_node.append(cat_obj)
    
    offers_node = ET.SubElement(shop, "offers")
    for o in processed_offers:
        offers_node.append(o)

    with open("Masterevanew.xml", "wb") as f:
        f.write(ET.tostring(yml, encoding='utf-8', xml_declaration=True, pretty_print=True))

    print("\n--- СТАТИСТИКА ОБРОБКИ ---")
    for s in source_stats:
        print(s)
    print(f"Загальна кількість товарів у файлі: {len(processed_offers)}")

if __name__ == "__main__":
    process()
