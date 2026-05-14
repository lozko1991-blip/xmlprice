import requests
import lxml.etree as ET
from datetime import datetime
import re
import time
from collections import defaultdict
from html import unescape

# Налаштування джерел
SOURCES = [
    "https://shkatulka.in.ua/content/export/cb28b41c71e755eab59d094a399ecfd8.xml", # Shkatulka
    "https://opt-drop.com/storage/xml/opt-drop-5.xml", # Opt-drop
    "https://feed.lugi.com.ua/index.php?route=extension/feed/unixml/ukr_ru",
    "https://dropom.com.ua/products_feed.xml?hash_tag=b55924e4ebc0576fda79ae6941f7a2a5&languages=uk%2Cru",
    "http://kievopt.com.ua/prices/rozetka-22294.yml",
    "https://dwn.royaltoys.com.ua/my/export/v2/e6f6dcf6-2539-4a43-a285-32667169f0db.xml"
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
    for i in range(MAX_RETRIES):
        try:
            # Shkatulka потребує User-Agent
            headers = {'User-Agent': 'Mozilla/5.0'}
            r = requests.get(url, headers=headers, timeout=90)
            if r.ok:
                return r.content
        except Exception as e:
            print(f"Спроба {i+1} не вдалася для {url}: {e}")
            if i < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
    return None

def clean_text_soft(text):
    """М'яка чистка: видаляє HTML та стоп-слова, зберігаючи пунктуацію та структуру"""
    if text is None: return ""
    
    # 1. Розкодування HTML сутностей (&nbsp;, &quot; тощо)
    text = unescape(text)
    
    # 2. Видаляємо скрипти та стилі
    text = re.sub(r'<(script|style).*?>.*?</\1>', '', text, flags=re.DOTALL)
    
    # 3. Замінюємо переноси рядків та абзаци на нові рядки
    text = re.sub(r'<(p|br|li|div|h[1-6]).*?>', '\n', text)
    
    # 4. Видаляємо всі інші теги
    text = re.sub(r'<[^>]+>', '', text)
    
    # 5. Чистимо стоп-слова, телефони та посилання
    text = re.sub(r'(\+?38)?\s?\(?\d{3}\)?[\s\.-]?\d{3}[\s\.-]?\d{2}[\s\.-]?\d{2}', '', text)
    text = re.sub(r'https?://\S+|www\.\S+', '', text)
    for word in STOP_WORDS:
        text = re.sub(word, '', text, flags=re.IGNORECASE)
    
    # 6. Нормалізація пробілів, але збереження переносів рядків
    lines = [line.strip() for line in text.split('\n')]
    return '\n'.join(line for line in lines if line)

def get_quantity(offer):
    # Логіка для Opt-drop: перевірка тегу <available>
    avail_attr = offer.get('available')
    avail_tag = offer.findtext('available')
    is_available_status = (avail_attr in ['true', 'yes', '1']) or (avail_tag == 'true')
    
    q_nodes = offer.xpath(".//quantity | .//quantity_in_stock | .//stock_quantity")
    if q_nodes:
        try:
            val = int(q_nodes[0].text)
            if val >= 1: return val
            if val == 0 and is_available_status: return 3
        except: pass
    
    return 3 if is_available_status else 0

def process():
    all_raw_data = []
    id_usage_count = {}
    source_stats = []

    for index, url in enumerate(SOURCES):
        domain = url.split('/')[2]
        print(f"Завантаження: {domain}")
        content = fetch_with_retry(url)
        
        if content:
            try:
                # recover=True допомагає при помилках в XML постачальника
                parser = ET.XMLParser(recover=True, encoding='utf-8')
                root = ET.fromstring(content, parser=parser)
                
                categories = root.xpath(".//category")
                offers = root.xpath(".//offer")
                
                # Облік дублікатів ID категорій
                current_source_ids = {c.get('id') for c in categories if c.get('id')}
                for cid in current_source_ids:
                    id_usage_count[cid] = id_usage_count.get(cid, 0) + 1
                
                all_raw_data.append({
                    'prefix': str(index + 1),
                    'domain': domain,
                    'categories': categories,
                    'offers': offers
                })
            except Exception as e:
                print(f"Помилка {domain}: {e}")

    duplicate_ids = {cid for cid, count in id_usage_count.items() if count > 1}
    processed_offers = []
    category_product_count = defaultdict(int)

    for data in all_raw_data:
        prefix = data['prefix']
        count = 0
        for offer in data['offers']:
            qty = get_quantity(offer)
            if qty <= 0: continue
            
            # Назви (вирішує проблему Shkatulka CDATA)
            name_node = offer.find('name_ua') or offer.find('name')
            if name_node is None: continue
            item_name = name_node.text

            # Ціни (обробка ком для Opt-drop)
            p_node = offer.find('price')
            if p_node is None or not p_node.text: continue
            
            try:
                raw_p = p_node.text.replace(',', '.')
                price = round(float(raw_p) * MARKUP_PERCENT + MARKUP_FIXED)
                if price < MIN_PRICE_THRESHOLD: continue
                
                # Створюємо нові вузли, щоб не пошкодити структуру оригінального XML
                new_offer = ET.Element("offer", id=f"{prefix}_{offer.get('id')}", available="true")
                ET.SubElement(new_offer, "name").text = item_name
                ET.SubElement(new_offer, "price").text = str(price)
                
                op_node = offer.find('oldprice')
                if op_node is not None and op_node.text:
                    old_p = round(float(op_node.text.replace(',', '.')) * MARKUP_PERCENT + MARKUP_FIXED)
                    ET.SubElement(new_offer, "oldprice").text = str(old_p)
                
                # Описи (М'яка чистка)
                raw_desc = offer.findtext('description_ua') or offer.findtext('description') or ""
                clean_desc = clean_text_soft(raw_desc)
                ET.SubElement(new_offer, "description").text = ET.CDATA(clean_desc)

                # Категорії
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
                    ET.SubElement(new_offer, "picture").text = pic.text
                
                ET.SubElement(new_offer, "quantity").text = str(qty)
                
                processed_offers.append(new_offer)
                count += 1
            except: continue
            
        source_stats.append(f"{data['domain']}: {count}")

    # Фільтрація та формування категорій
    final_categories = []
    for data in all_raw_data:
        prefix = data['prefix']
        for cat in data['categories']:
            cid = cat.get('id')
            f_id = f"{prefix}_{cid}" if cid in duplicate_ids else cid
            if category_product_count.get(f_id, 0) >= 1: # Мінімальний поріг 1 товар
                cat.set('id', f_id)
                pid = cat.get('parentId')
                if pid:
                    cat.set('parentId', f"{prefix}_{pid}" if pid in duplicate_ids else pid)
                final_categories.append(cat)

    # Збереження
    yml = ET.Element("yml_catalog", date=datetime.now().strftime("%Y-%m-%d %H:%M"))
    shop = ET.SubElement(yml, "shop")
    ET.SubElement(shop, "name").text = "Master Shop EVA"
    currs = ET.SubElement(shop, "currencies")
    ET.SubElement(currs, "currency", id="UAH", rate="1")
    
    cats_node = ET.SubElement(shop, "categories")
    for c in final_categories: cats_node.append(c)
    
    offers_node = ET.SubElement(shop, "offers")
    for o in processed_offers: offers_node.append(o)

    with open("Masterevanew.xml", "wb") as f:
        f.write(ET.tostring(yml, encoding='utf-8', xml_declaration=True, pretty_print=True))

    print("\nСтатистика:")
    for s in source_stats: print(s)

if __name__ == "__main__":
    process()
