import requests
import lxml.etree as ET
from datetime import datetime
import re
import time
from collections import defaultdict

# Налаштування джерел
SOURCES = [
    "https://feed.lugi.com.ua/index.php?route=extension/feed/unixml/ukr_ru",
    "https://dropom.com.ua/products_feed.xml?hash_tag=b55924e4ebc0576fda79ae6941f7a2a5&sales_notes=&product_ids=&label_ids=&exclude_fields=&html_description=1&yandex_cpa=&process_presence_sure=&languages=uk%2Cru&extra_fields=quantityInStock%2Ckeywords&group_ids=",
    "https://opt-drop.com/storage/xml/opt-drop-5.xml",
    "http://kievopt.com.ua/prices/rozetka-22294.yml",
    "https://dwn.royaltoys.com.ua/my/export/v2/e6f6dcf6-2539-4a43-a285-32667169f0db.xml"
]

# Параметри ціноутворення та фільтрації
MARKUP_PERCENT = 1.35
MARKUP_FIXED = 40
MIN_PRICE_THRESHOLD = 150  # Мінімальна ціна продажу
MAX_RETRIES = 3            # Спроби завантаження при збої мережі
RETRY_DELAY = 5            # Пауза між спробами (сек)

# Список стоп-слів для очищення описів (вимоги EVA)
STOP_WORDS = [
    r"предоплата", r"передплата", r"наложка", r"накладений платіж", r"самовивіз",
    r"позвоніть", r"позвонить", r"зателефонуйте", r"зателефонувати", r"набрать", r"набрати",
    r"напишіть", r"написать", r"написати", r"пишіть", r"звоните", r"дзвоніть",
    r"вайбер", r"viber", r"телеграм", r"telegram", r"tg", r"whatsapp", r"ватсап",
    r"сайт", r"магазин", r"склад", r"в наявності", r"под замовлення"
]

def fetch_with_retry(url):
    """Завантажує прайс з повторними спробами"""
    for i in range(MAX_RETRIES):
        try:
            r = requests.get(url, timeout=90)
            if r.ok:
                return r.content
        except Exception as e:
            print(f"Спроба {i+1} не вдалася для {url.split('/')[2]}: {e}")
            if i < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
    return None

def clean_text(text):
    """Глибока чистка тексту від контактів, посилань та стоп-слів"""
    if text is None: return ""
    # Декодування та видалення тегів
    text = text.replace('&lt;', '<').replace('&gt;', '>').replace('&amp;', '&').replace('&nbsp;', ' ')
    text = re.sub(r'<(?!/?(p|br|b|strong|ul|li)\b)[^>]+>', '', text)
    # Телефони та посилання
    text = re.sub(r'(\+?38)?\s?\(?\d{3}\)?[\s\.-]?\d{3}[\s\.-]?\d{2}[\s\.-]?\d{2}', '', text)
    text = re.sub(r'https?://\S+|www\.\S+|\b\S+\.(?:com|ua|net|org|shop|biz)\b', '', text)
    # Стоп-слова
    for word in STOP_WORDS:
        text = re.sub(word, '', text, flags=re.IGNORECASE)
    # Emoji та зайві пробіли
    text = text.encode('ascii', 'ignore').decode('ascii') 
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

def get_quantity(offer):
    """Логіка наявності: враховуємо статус true навіть при цифрі 0"""
    avail = offer.get('available')
    is_available_status = avail in ['true', 'yes', '1']
    
    q_nodes = offer.xpath(".//quantity | .//quantity_in_stock | .//stock_quantity")
    if q_nodes:
        try:
            val = int(q_nodes[0].text)
            if val >= 1:
                return val
            if val == 0 and is_available_status:
                return 3  # Оживляємо товар: статус каже 'є', а цифра '0'
        except: pass
    
    return 3 if is_available_status else 0

def process():
    all_raw_data = []
    id_usage_count = {}
    source_stats = []

    # Завантаження даних
    for index, url in enumerate(SOURCES):
        domain = url.split('/')[2]
        print(f"Обробка джерела: {domain}")
        content = fetch_with_retry(url)
        
        if content:
            try:
                parser = ET.XMLParser(recover=True, encoding='utf-8')
                root = ET.fromstring(content, parser=parser)
                categories = root.xpath(".//category")
                offers = root.xpath(".//offer")
                
                # Рахуємо дублі ID категорій між постачальниками
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
                print(f"Помилка парсингу {domain}: {e}")

    duplicate_ids = {cid for cid, count in id_usage_count.items() if count > 1}
    processed_offers = []
    category_product_count = defaultdict(int)

    # Обробка товарів
    for data in all_raw_data:
        prefix = data['prefix']
        count = 0
        for offer in data['offers']:
            qty = get_quantity(offer)
            if qty <= 0: continue
            
            p_node = offer.find('price')
            if p_node is None or not p_node.text: continue
            
            try:
                # Націнка та поріг 150 грн
                price = round(float(p_node.text) * MARKUP_PERCENT + MARKUP_FIXED)
                if price < MIN_PRICE_THRESHOLD: continue
                p_node.text = str(price)
                
                op_node = offer.find('oldprice')
                if op_node is not None and op_node.text:
                    op_node.text = str(round(float(op_node.text) * MARKUP_PERCENT + MARKUP_FIXED))
            except: continue

            # Чистка описів
            for tag in ['description', 'description_ua']:
                node = offer.find(tag)
                if node is not None:
                    node.text = ET.CDATA(clean_text(node.text))

            # ID Категорій з префіксами
            cat_node = offer.find('categoryId')
            if cat_node is not None:
                cid = cat_node.text
                f_cid = f"{prefix}_{cid}" if cid in duplicate_ids else cid
                cat_node.text = f_cid
                category_product_count[f_cid] += 1

            # Формат кількості для EVA
            q_node = offer.find('quantity') or ET.SubElement(offer, 'quantity')
            q_node.text = str(qty)
            offer.set('available', 'true')
            
            processed_offers.append(offer)
            count += 1
        source_stats.append(f"{data['domain']}: {count} товарів")

    # Фільтрація категорій (менше 5 товарів та без підкатегорій)
    final_cats_map = {}
    for data in all_raw_data:
        prefix = data['prefix']
        for cat in data['categories']:
            cid = cat.get('id')
            pid = cat.get('parentId')
            f_id = f"{prefix}_{cid}" if cid in duplicate_ids else cid
            f_pid = f"{prefix}_{pid}" if pid and pid in duplicate_ids else pid
            cat.set('id', f_id)
            if f_pid: cat.set('parentId', f_pid)
            final_cats_map[f_id] = cat

    to_delete = {cid for cid, cnt in category_product_count.items() if cnt < 5 and not any(c.get('parentId') == cid for c in final_cats_map.values())}

    final_categories = [c for cid, c in final_cats_map.items() if cid not in to_delete]
    final_offers = [o for o in processed_offers if o.find('categoryId').text not in to_delete]

    # Збереження результату
    yml = ET.Element("yml_catalog", date=datetime.now().strftime("%Y-%m-%d %H:%M"))
    shop = ET.SubElement(yml, "shop")
    ET.SubElement(shop, "name").text = "Master Shop EVA"
    currs = ET.SubElement(shop, "currencies")
    ET.SubElement(currs, "currency", id="UAH", rate="1")
    
    cats_node = ET.SubElement(shop, "categories")
    for c in final_categories: cats_node.append(c)
    offers_node = ET.SubElement(shop, "offers")
    for o in final_offers: offers_node.append(o)

    with open("Masterevanew.xml", "wb") as f:
        f.write(ET.tostring(yml, encoding='utf-8', xml_declaration=True, pretty_print=True))

    print("\n--- ФІНАЛЬНА СТАТИСТИКА ---")
    for s in source_stats: print(s)
    print(f"Усього товарів: {len(final_offers)}")

if __name__ == "__main__":
    process()
