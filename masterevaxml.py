import requests
import lxml.etree as ET
from datetime import datetime
import re
from html import unescape

# Конфігурація джерел з новими префіксами для категорій
SOURCES = [
    ("1111", "https://shkatulka.in.ua/content/export/cb28b41c71e755eab59d094a399ecfd8.xml"),
    ("2222", "https://opt-drop.com/storage/xml/opt-drop-5.xml"),
    ("3333", "https://feed.lugi.com.ua/index.php?route=extension/feed/unixml/ukr_ru"),
    ("4444", "https://dropom.com.ua/products_feed.xml?hash_tag=b55924e4ebc0576fda79ae6941f7a2a5&languages=uk%2Cru"),
    ("",     "http://kievopt.com.ua/prices/rozetka-22294.yml"), # kievopt без префікса
    ("5555", "https://dwn.royaltoys.com.ua/my/export/v2/e6f6dcf6-2539-4a43-a285-32667169f0db.xml")
]

MARKUP_PERCENT = 1.35
MARKUP_FIXED = 40
PROMO_DISCOUNT = 0.07 # -7%
MIN_PRICE_THRESHOLD = 150

def fetch_with_retry(url):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    try:
        r = requests.get(url, headers=headers, timeout=120)
        return r.content if r.ok else None
    except: return None

def clean_description(text):
    if not text: return ""
    text = unescape(text)
    text = re.sub(r'<(script|style).*?>.*?</\1>', '', text, flags=re.DOTALL)
    text = re.sub(r'https?://\S+|www\.\S+', '', text)
    text = re.sub(r'(\+?38)?\s?\(?\d{3}\)?[\s\.-]?\d{3}[\s\.-]?\d{2}[\s\.-]?\d{2}', '', text)
    text = re.sub(r'<(?!p|br|li|ul|ol|b|strong|i)[^>]+>', '', text)
    return text.strip()

def process_name(name, vendor):
    if not name: return ""
    if not vendor or vendor == "NoBrand": return name
    if vendor.lower() not in name.lower():
        name = f"{name} {vendor}"
    return name[:254].strip()

def process():
    final_categories = {}
    processed_offers = []
    source_results = []
    category_id_map = {} # Для відстеження дублів

    print("--- ЗАПУСК ФІНАЛЬНОЇ ГЕНЕРАЦІЇ ДЛЯ EVA ---")

    for prefix, url in SOURCES:
        domain = url.split('/')[2]
        content = fetch_with_retry(url)
        
        count_ok, count_low_price, count_no_stock = 0, 0, 0
        if not content: continue

        try:
            parser = ET.XMLParser(recover=True, encoding='utf-8')
            root = ET.fromstring(content, parser=parser)
            
            # 1. Обробка категорій з новими ID
            for cat in root.xpath(".//category"):
                original_id = cat.get('id')
                new_id = f"{prefix}{original_id}" if prefix else original_id
                
                # Перевірка на дублікати
                if new_id in category_id_map and category_id_map[new_id] != domain:
                    new_id = f"{new_id}9"
                
                category_id_map[new_id] = domain
                cat.set('id', new_id)
                
                pid = cat.get('parentId')
                if pid:
                    new_pid = f"{prefix}{pid}" if prefix else pid
                    cat.set('parentId', new_pid)
                
                final_categories[new_id] = cat

            # 2. Обробка товарів
            for offer in root.xpath(".//offer"):
                # Наявність
                avail_attr = offer.get('available', '').lower()
                avail_tag = (offer.findtext('available') or '').lower()
                is_available = (avail_attr in ['true', 'yes', '1']) or (avail_tag in ['true', 'yes', '1'])
                
                if not is_available:
                    count_no_stock += 1
                    continue

                qty_nodes = offer.xpath(".//quantity | .//quantity_in_stock | .//stock_quantity | .//amount")
                qty = int(re.sub(r'\D', '', qty_nodes[0].text)) if qty_nodes and qty_nodes[0].text else 3
                if qty <= 0: qty = 3

                # Ціни
                price_node = offer.find('price')
                if price_node is None: continue
                
                try:
                    base_price = float(price_node.text.replace(',', '.'))
                    price = round(base_price * MARKUP_PERCENT + MARKUP_FIXED)
                    
                    if price < MIN_PRICE_THRESHOLD:
                        count_low_price += 1
                        continue
                    
                    # Промо ціна (-7%)
                    price_promo = round(price * (1 - PROMO_DISCOUNT))

                    vendor = offer.findtext('vendor') or "NoBrand"
                    name_ua = process_name(offer.findtext('name_ua') or offer.findtext('name'), vendor)

                    # Створення Offer
                    new_off = ET.Element("offer", id=f"{prefix if prefix else '6'}_{offer.get('id')}", available="true")
                    ET.SubElement(new_off, "name_ua").text = name_ua
                    ET.SubElement(new_off, "price").text = str(price)
                    ET.SubElement(new_off, "price_promo").text = str(price_promo) # Додаємо промо ціну
                    
                    # Стара ціна
                    old_p_node = offer.find('oldprice') or offer.find('price_old')
                    if old_p_node is not None:
                        old_p = round(float(old_p_node.text.replace(',', '.')) * MARKUP_PERCENT + MARKUP_FIXED)
                        if old_p > price:
                            ET.SubElement(new_off, "price_old").text = str(old_p)

                    ET.SubElement(new_off, "currencyId").text = "UAH"
                    
                    # Прив'язка до нової категорії
                    orig_cat_id = offer.findtext('categoryId')
                    final_cat_id = f"{prefix}{orig_cat_id}" if prefix else orig_cat_id
                    if f"{final_cat_id}9" in category_id_map: final_cat_id = f"{final_cat_id}9"
                    ET.SubElement(new_off, "categoryId").text = final_cat_id
                    
                    ET.SubElement(new_off, "vendor").text = vendor
                    ET.SubElement(new_off, "stock_quantity").text = str(qty)
                    
                    raw_desc = offer.findtext('description_ua') or offer.findtext('description') or ""
                    desc = clean_description(raw_desc)
                    if len(desc) < 30: desc = f"<p>{name_ua} від виробника {vendor}.</p>"
                    ET.SubElement(new_off, "description_ua").text = ET.CDATA(desc)

                    for pic in offer.findall('picture'):
                        if pic.text: ET.SubElement(new_off, "picture").text = pic.text

                    # Параметри
                    params = offer.findall('param')
                    if not params:
                        ET.SubElement(new_off, "param", name="Колір").text = "Комбінований"
                        ET.SubElement(new_off, "param", name="Розмір").text = "-"
                        ET.SubElement(new_off, "param", name="Стан").text = "Новий"
                    else:
                        for p in params: new_off.append(p)

                    processed_offers.append(new_off)
                    count_ok += 1
                except: continue

            source_results.append(f"{domain}: OK:{count_ok}, LowPrice:{count_low_price}, NoStock:{count_no_stock}")

        except Exception as e: print(f"Error {domain}: {e}")

    # Збірка
    yml = ET.Element("yml_catalog", date=datetime.now().strftime("%Y-%m-%d %H:%M"))
    shop = ET.SubElement(yml, "shop")
    ET.SubElement(shop, "name").text = "Master Shop EVA"
    currs = ET.SubElement(shop, "currencies")
    ET.SubElement(currs, "currency", id="UAH", rate="1")
    
    cats_node = ET.SubElement(shop, "categories")
    for c in final_categories.values(): cats_node.append(c)
    
    offers_node = ET.SubElement(shop, "offers")
    for o in processed_offers: offers_node.append(o)

    with open("Masterevanew.xml", "wb") as f:
        f.write(ET.tostring(yml, encoding='utf-8', xml_declaration=True, pretty_print=True))
    
    print("\n--- СТАТИСТИКА ---")
    for res in source_results: print(res)
    print(f"Всього товарів: {len(processed_offers)}")

if __name__ == "__main__":
    process()
