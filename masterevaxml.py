import requests
import lxml.etree as ET
from datetime import datetime
import re
import time
from collections import defaultdict
from html import unescape

# Джерела (префікси 1-6)
SOURCES = [
    ("1", "https://shkatulka.in.ua/content/export/cb28b41c71e755eab59d094a399ecfd8.xml"),
    ("2", "https://opt-drop.com/storage/xml/opt-drop-5.xml"),
    ("3", "https://feed.lugi.com.ua/index.php?route=extension/feed/unixml/ukr_ru"),
    ("4", "https://dropom.com.ua/products_feed.xml?hash_tag=b55924e4ebc0576fda79ae6941f7a2a5&languages=uk%2Cru"),
    ("5", "http://kievopt.com.ua/prices/rozetka-22294.yml"),
    ("6", "https://dwn.royaltoys.com.ua/my/export/v2/e6f6dcf6-2539-4a43-a285-32667169f0db.xml")
]

MARKUP_PERCENT = 1.35
MARKUP_FIXED = 40
MIN_PRICE_THRESHOLD = 150

def fetch_with_retry(url):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36'}
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
    
    print("--- СТАРТ ОБРОБКИ (EVA STANDARD + PRIORITY AVAILABILITY) ---")

    for prefix, url in SOURCES:
        domain = url.split('/')[2]
        content = fetch_with_retry(url)
        
        count_ok = 0
        count_low_price = 0
        count_no_stock = 0
        
        if not content:
            print(f"Помилка завантаження: {domain}")
            continue

        try:
            parser = ET.XMLParser(recover=True, encoding='utf-8')
            root = ET.fromstring(content, parser=parser)
            
            # Обробка категорій
            for cat in root.xpath(".//category"):
                cid = cat.get('id')
                f_id = f"{prefix}_{cid}"
                cat.set('id', f_id)
                pid = cat.get('parentId')
                if pid: cat.set('parentId', f"{prefix}_{pid}")
                final_categories[f_id] = cat

            # Обробка товарів
            offers = root.xpath(".//offer")
            for offer in offers:
                # 1. ПЕРЕВІРКА НАЯВНОСТІ (Пріоритет статусу true/false)
                avail_attr = offer.get('available', '').lower()
                avail_tag = (offer.findtext('available') or '').lower()
                
                # Визначаємо, чи вважає постачальник товар наявним
                is_available = (avail_attr in ['true', 'yes', '1']) or (avail_tag in ['true', 'yes', '1'])
                
                # ФІЛЬТР: Якщо наявність FALSE — відразу в NoStock (незалежно від цифри кількості)
                if not is_available:
                    count_no_stock += 1
                    continue

                # Якщо наявність TRUE — визначаємо кількість
                qty_nodes = offer.xpath(".//quantity | .//quantity_in_stock | .//stock_quantity | .//amount")
                qty = 0
                if qty_nodes:
                    try:
                        qty = int(re.sub(r'\D', '', qty_nodes[0].text))
                    except: pass
                
                # Ваша умова: якщо наявність true а сток 0 (або не знайдений), ставимо 3 одиниці
                if qty <= 0:
                    qty = 3

                # 2. Перевірка ціни
                price_node = offer.find('price')
                if price_node is None: continue
                
                try:
                    raw_price = float(price_node.text.replace(',', '.'))
                    price = round(raw_price * MARKUP_PERCENT + MARKUP_FIXED)
                    
                    if price < MIN_PRICE_THRESHOLD:
                        count_low_price += 1
                        continue

                    vendor = offer.findtext('vendor') or "NoBrand"
                    
                    # 3. Назви та Описи
                    raw_name = offer.findtext('name')
                    raw_name_ua = offer.findtext('name_ua')
                    
                    final_name = process_name(raw_name, vendor)
                    final_name_ua = process_name(raw_name_ua if raw_name_ua else raw_name, vendor)

                    desc_src = offer.findtext('description_ua') or offer.findtext('description') or ""
                    desc_cleaned = clean_description(desc_src)
                    if len(desc_cleaned) < 30:
                        desc_cleaned = f"<p>{final_name_ua}. Характеристики та опис товару від виробника {vendor}.</p>"

                    # 4. Створення Offer
                    new_off = ET.Element("offer", id=f"{prefix}_{offer.get('id')}", available="true")
                    
                    if raw_name: ET.SubElement(new_off, "name").text = final_name
                    ET.SubElement(new_off, "name_ua").text = final_name_ua
                    ET.SubElement(new_off, "price").text = str(price)
                    
                    old_p_node = offer.find('oldprice') or offer.find('price_old')
                    if old_p_node is not None:
                        try:
                            old_p = round(float(old_p_node.text.replace(',', '.')) * MARKUP_PERCENT + MARKUP_FIXED)
                            if old_p > price:
                                ET.SubElement(new_off, "price_old").text = str(old_p)
                        except: pass

                    ET.SubElement(new_off, "currencyId").text = "UAH"
                    ET.SubElement(new_off, "categoryId").text = f"{prefix}_{offer.findtext('categoryId')}"
                    ET.SubElement(new_off, "vendor").text = vendor
                    ET.SubElement(new_off, "article").text = offer.findtext('vendorCode') or offer.findtext('article') or offer.get('id')
                    ET.SubElement(new_off, "description_ua").text = ET.CDATA(desc_cleaned)
                    ET.SubElement(new_off, "stock_quantity").text = str(qty)

                    for pic in offer.findall('picture'):
                        if pic.text: ET.SubElement(new_off, "picture").text = pic.text

                    # Параметри (завжди заповнені для EVA)
                    params = offer.findall('param')
                    if not params:
                        ET.SubElement(new_off, "param", name="Колір").text = "Комбінований"
                        ET.SubElement(new_off, "param", name="Розмір").text = "-"
                        ET.SubElement(new_off, "param", name="Бренд").text = vendor
                        ET.SubElement(new_off, "param", name="Стан").text = "Новий"
                    else:
                        for p in params: new_off.append(p)

                    processed_offers.append(new_off)
                    count_ok += 1
                except: continue

            source_results.append(f"{domain}: OK:{count_ok}, LowPrice:{count_low_price}, NoStock:{count_no_stock}")
            print(f"Оброблено {domain}")

        except Exception as e:
            print(f"Помилка {domain}: {e}")

    # Збірка XML
    yml = ET.Element("yml_catalog", date=datetime.now().strftime("%Y-%m-%d %H:%M"))
    shop = ET.SubElement(yml, "shop")
    ET.SubElement(shop, "name").text = "Master Shop EVA"
    ET.SubElement(shop, "company").text = "Master Shop"
    currs = ET.SubElement(shop, "currencies")
    ET.SubElement(currs, "currency", id="UAH", rate="1")
    
    cats_node = ET.SubElement(shop, "categories")
    for c in final_categories.values(): cats_node.append(c)
    
    offers_node = ET.SubElement(shop, "offers")
    for o in processed_offers: offers_node.append(o)

    with open("Masterevanew.xml", "wb") as f:
        f.write(ET.tostring(yml, encoding='utf-8', xml_declaration=True, pretty_print=True))
    
    print("\n--- СТАТИСТИКА ОБРОБКИ ---")
    for res in source_results:
        print(res)
    print(f"Загальна кількість товарів у файлі: {len(processed_offers)}")

if __name__ == "__main__":
    process()
