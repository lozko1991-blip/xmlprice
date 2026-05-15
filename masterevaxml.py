import requests
import lxml.etree as ET
from datetime import datetime
import re
from html import unescape, escape

# Конфігурація джерел
SOURCES = [
    ("1111", "https://shkatulka.in.ua/content/export/cb28b41c71e755eab59d094a399ecfd8.xml"),
    ("2222", "https://opt-drop.com/storage/xml/opt-drop-5.xml"),
    ("3333", "https://feed.lugi.com.ua/index.php?route=extension/feed/unixml/ukr_ru"),
    ("4444", "https://dropom.com.ua/products_feed.xml?hash_tag=b55924e4ebc0576fda79ae6941f7a2a5&languages=uk%2Cru"),
    ("",     "http://kievopt.com.ua/prices/rozetka-22294.yml"),
    ("5555", "https://dwn.royaltoys.com.ua/my/export/v2/e6f6dcf6-2539-4a43-a285-32667169f0db.xml")
]

MARKUP_PERCENT, MARKUP_FIXED, PROMO_DISCOUNT, MIN_PRICE = 1.35, 40, 0.07, 150
DESC_LIMIT = 2500 # Обмеження для EVA

def clean_html_entities(text):
    """Виправляє подвійне кодування апострофів та спецсимволів"""
    if not text: return ""
    # Спочатку розкодовуємо все (напр. &amp;apos; -> &apos; -> ')
    text = unescape(unescape(text))
    return text

def clean_description(text):
    if not text: return ""
    text = clean_html_entities(text)
    # Видаляємо скрипти, посилання, телефони
    text = re.sub(r'<(script|style).*?>.*?</\1>', '', text, flags=re.DOTALL)
    text = re.sub(r'https?://\S+|www\.\S+', '', text)
    text = re.sub(r'(\+?38)?\s?\(?\d{3}\)?[\s\.-]?\d{3}[\s\.-]?\d{2}[\s\.-]?\d{2}', '', text)
    # Обрізка за довжиною
    if len(text) > DESC_LIMIT:
        text = text[:DESC_LIMIT] + "..."
    return text.strip()

def process():
    final_categories = {}
    processed_offers = []
    source_results = []
    category_id_map = {}

    for prefix, url in SOURCES:
        domain = url.split('/')[2]
        content = requests.get(url, timeout=120).content
        count_ok, count_low, count_no = 0, 0, 0
        
        parser = ET.XMLParser(recover=True, encoding='utf-8')
        root = ET.fromstring(content, parser=parser)
        
        # Обробка категорій
        for cat in root.xpath(".//category"):
            new_id = f"{prefix}{cat.get('id')}" if prefix else cat.get('id')
            if new_id in category_id_map and category_id_map[new_id] != domain:
                new_id = f"{new_id}9"
            category_id_map[new_id] = domain
            cat.set('id', new_id)
            if cat.get('parentId'):
                cat.set('parentId', f"{prefix}{cat.get('parentId')}" if prefix else cat.get('parentId'))
            final_categories[new_id] = cat

        # Обробка товарів
        for offer in root.xpath(".//offer"):
            # Логіка наявності (true/false)
            avail_attr = offer.get('available', '').lower()
            is_avail = avail_attr in ['true', 'yes', '1']
            if not is_avail:
                count_no += 1
                continue

            # Логіка стоків (ваша умова: якщо true але 0 -> 3)
            qty_n = offer.xpath(".//quantity|.//stock_quantity|.//amount")
            qty = int(re.sub(r'\D', '', qty_n[0].text)) if qty_n and qty_n[0].text else 3
            if qty <= 0: qty = 3

            try:
                p_node = offer.find('price')
                if p_node is None: continue
                price = round(float(p_node.text.replace(',','.')) * MARKUP_PERCENT + MARKUP_FIXED)
                if price < MIN_PRICE:
                    count_low += 1
                    continue
                
                price_promo = round(price * (1 - PROMO_DISCOUNT))
                vendor = offer.findtext('vendor') or "NoBrand"
                name_ua = offer.findtext('name_ua') or offer.findtext('name')
                name_ua = clean_html_entities(name_ua)
                if vendor.lower() not in name_ua.lower(): name_ua = f"{name_ua} {vendor}"

                new_off = ET.Element("offer", id=f"{prefix if prefix else '6'}_{offer.get('id')}", available="true")
                ET.SubElement(new_off, "name_ua").text = name_ua[:250]
                ET.SubElement(new_off, "price").text = str(price)
                ET.SubElement(new_off, "price_promo").text = str(price_promo)
                ET.SubElement(new_off, "currencyId").text = "UAH"
                
                cat_id = f"{prefix}{offer.findtext('categoryId')}" if prefix else offer.findtext('categoryId')
                ET.SubElement(new_off, "categoryId").text = cat_id
                ET.SubElement(new_off, "vendor").text = vendor
                ET.SubElement(new_off, "stock_quantity").text = str(qty)
                
                # Опис з обмеженням
                raw_desc = offer.findtext('description_ua') or offer.findtext('description') or ""
                desc = clean_description(raw_desc)
                ET.SubElement(new_off, "description_ua").text = ET.CDATA(desc)

                for pic in offer.findall('picture'):
                    if pic.text: ET.SubElement(new_off, "picture").text = pic.text

                # Параметри з очисткою апострофів
                params = offer.findall('param')
                if not params:
                    ET.SubElement(new_off, "param", name="Стан").text = "Новий"
                else:
                    for p in params:
                        p.text = clean_html_entities(p.text)
                        new_off.append(p)

                processed_offers.append(new_off)
                count_ok += 1
            except: continue
        source_results.append(f"{domain}: OK:{count_ok}, LowPrice:{count_low}, NoStock:{count_no}")

    # Фінальний запис
    yml = ET.Element("yml_catalog", date=datetime.now().strftime("%Y-%m-%d %H:%M"))
    shop = ET.SubElement(yml, "shop")
    cats_n = ET.SubElement(shop, "categories")
    for c in final_categories.values(): cats_n.append(c)
    offers_n = ET.SubElement(shop, "offers")
    for o in processed_offers: offers_n.append(o)

    with open("Masterevanew.xml", "wb") as f:
        f.write(ET.tostring(yml, encoding='utf-8', xml_declaration=True, pretty_print=True))
    
    for res in source_results: print(res)

if __name__ == "__main__":
    process()
