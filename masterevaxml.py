import requests
import lxml.etree as ET
from datetime import datetime
import re
import time
from collections import defaultdict
from html import unescape

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
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36'}
    for i in range(MAX_RETRIES):
        try:
            r = requests.get(url, headers=headers, timeout=120)
            if r.ok: return r.content
        except Exception as e:
            print(f"Спроба {i+1} помилка {url.split('/')[2]}: {e}")
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
    # ПЕРЕВІРКА 1: Дивимось на атрибут available в самому тегу <offer>
    avail_attr = offer.get('available')
    
    # ПЕРЕВІРКА 2: Дивимось на вкладений тег <available> (буває в Opt-drop)
    avail_tag = offer.findtext('available')
    
    is_available = (avail_attr in ['true', 'yes', '1']) or (avail_tag in ['true', 'yes', '1'])
    
    # ПЕРЕВІРКА 3: Шукаємо цифру кількості в різних варіантах тегів
    q_nodes = offer.xpath(".//quantity | .//quantity_in_stock | .//stock_quantity | .//amount")
    
    if q_nodes:
        try:
            val = int(re.sub(r'\D', '', q_nodes[0].text)) # Витягуємо тільки цифри
            if val > 0: return val
        except: pass
    
    # ФІНАЛЬНЕ РІШЕННЯ: Якщо стоїть статус "в наявності", але цифри немає або вона 0
    return 3 if is_available else 0

def process():
    all_raw_data = []
    id_usage_count = {}
    source_stats = []

    print("--- ЗАВАНТАЖЕННЯ ---")
    for index, url in enumerate(SOURCES):
        domain = url.split('/')[2]
        content = fetch_with_retry(url)
        if content:
            try:
                parser = ET.XMLParser(recover=True, encoding='utf-8')
                root = ET.fromstring(content, parser=parser)
                offers = root.xpath(".//offer")
                categories = root.xpath(".//category")
                
                # Реєстрація категорій
                current_ids = {c.get('id') for c in categories if c.get('id')}
                for cid in current_ids:
                    id_usage_count[cid] = id_usage_count.get(cid, 0) + 1
                
                all_raw_data.append({
                    'prefix': str(index + 1),
                    'domain': domain,
                    'categories': categories,
                    'offers': offers
                })
                print(f"Отримано {domain}: {len(offers)} шт.")
            except Exception as e: print(f"Помилка {domain}: {e}")

    duplicate_ids = {cid for cid, count in id_usage_count.items() if count > 1}
    processed_offers = []
    category_product_count = defaultdict(int)
    final_categories_map = {}

    print("\n--- ОБРОБКА ---")
    for data in all_raw_data:
        prefix, domain = data['prefix'], data['domain']
        count_ok, count_skip_price, count_skip_qty = 0, 0, 0
        
        for cat in data['categories']:
            cid = cat.get('id')
            f_id = f"{prefix}_{cid}" if cid in duplicate_ids else cid
            cat.set('id', f_id)
            pid = cat.get('parentId')
            if pid:
                cat.set('parentId', f"{prefix}_{pid}" if pid in duplicate_ids else pid)
            final_categories_map[f_id] = cat

        for offer in data['offers']:
            qty = get_quantity(offer)
            if qty <= 0:
                count_skip_qty += 1
                continue
            
            name_node = offer.find('name_ua') or offer.find('name')
            p_node = offer.find('price')
            
            if name_node is None or p_node is None or not p_node.text:
                continue
            
            try:
                price = round(float(p_node.text.replace(',', '.')) * MARKUP_PERCENT + MARKUP_FIXED)
                if price < MIN_PRICE_THRESHOLD:
                    count_skip_price += 1
                    continue
                
                # Створення нового об'єкта
                new_off = ET.Element("offer", id=f"{prefix}_{offer.get('id')}", available="true")
                ET.SubElement(new_off, "name").text = name_node.text
                ET.SubElement(new_off, "price").text = str(price)
                
                op_node = offer.find('oldprice')
                if op_node is not None and op_node.text:
                    try:
                        old_p = round(float(op_node.text.replace(',', '.')) * MARKUP_PERCENT + MARKUP_FIXED)
                        ET.SubElement(new_off, "oldprice").text = str(old_p)
                    except: pass
                
                raw_desc = offer.findtext('description_ua') or offer.findtext('description') or ""
                ET.SubElement(new_off, "description").text = ET.CDATA(clean_text_soft(raw_desc))

                cat_id_node = offer.find('categoryId')
                if cat_id_node is not None:
                    cid = cat_id_node.text
                    f_cid = f"{prefix}_{cid}" if cid in duplicate_ids else cid
                    ET.SubElement(new_off, "categoryId").text = f_cid
                    category_product_count[f_cid] += 1

                v_code = offer.findtext('vendorCode') or offer.findtext('article') or ""
                ET.SubElement(new_off, "vendorCode").text = v_code
                for pic in offer.findall('picture'):
                    if pic.text: ET.SubElement(new_off, "picture").text = pic.text
                
                ET.SubElement(new_off, "quantity").text = str(qty)
                processed_offers.append(new_off)
                count_ok += 1
            except: continue
        
        source_stats.append(f"{domain}: OK:{count_ok}, LowPrice:{count_skip_price}, NoStock:{count_skip_qty}")

    # Фіналізація
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
    for o in processed_offers: offers_node.append(o)

    with open("Masterevanew.xml", "wb") as f:
        f.write(ET.tostring(yml, encoding='utf-8', xml_declaration=True, pretty_print=True))

    for s in source_stats: print(s)
    print(f"Всього у файлі: {len(processed_offers)}")

if __name__ == "__main__":
    process()
