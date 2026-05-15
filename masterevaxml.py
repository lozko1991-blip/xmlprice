import requests
import lxml.etree as ET
from datetime import datetime
import re
from html import unescape

# 1. КОНФІГУРАЦІЯ
SOURCES = [
    ("1111", "https://shkatulka.in.ua/content/export/cb28b41c71e755eab59d094a399ecfd8.xml"),
    ("2222", "https://opt-drop.com/storage/xml/opt-drop-5.xml"),
    ("3333", "https://feed.lugi.com.ua/index.php?route=extension/feed/unixml/ukr_ru"),
    ("4444", "https://dropom.com.ua/products_feed.xml?hash_tag=b55924e4ebc0576fda79ae6941f7a2a5&languages=uk%2Cru"),
    ("",     "http://kievopt.com.ua/prices/rozetka-22294.yml"),
    ("5555", "https://dwn.royaltoys.com.ua/my/export/v2/e6f6dcf6-2539-4a43-a285-32667169f0db.xml")
]

MARKUP_PERCENT = 1.35
MARKUP_FIXED = 40
PROMO_DISCOUNT = 0.07 
MIN_PRICE_THRESHOLD = 150
DESC_LIMIT = 2800 

def fix_text(text):
    """Виправляє апострофи для запобігання помилок валідації"""
    if not text: return ""
    return unescape(unescape(text)).replace("'", "’").strip()

def clean_description(text, name_ua, vendor):
    if not text: 
        return f"<p>{name_ua} від виробника {vendor}.</p>"
    text = unescape(unescape(text))
    text = re.sub(r'<(script|style).*?>.*?</\1>', '', text, flags=re.DOTALL)
    text = re.sub(r'https?://\S+|www\.\S+', '', text)
    text = re.sub(r'\sstyle="[^"]*"', '', text) # Очищення стилів для EVA
    if len(text) > DESC_LIMIT:
        text = text[:DESC_LIMIT] + "..."
    return text.strip()

def process():
    final_categories = {}
    processed_offers = []
    source_results = []
    category_id_map = {}

    print("--- СТАРТ ОБРОБКИ З ВИПРАВЛЕННЯМ ВАЛЮТ ---")

    for prefix, url in SOURCES:
        domain = url.split('/')[2]
        try:
            r = requests.get(url, timeout=120)
            if not r.ok: continue
            root = ET.fromstring(r.content, parser=ET.XMLParser(recover=True))
            
            count_ok, count_low, count_no = 0, 0, 0

            # Обробка категорій
            for cat in root.xpath(".//category"):
                orig_id = cat.get('id')
                new_id = f"{prefix}{orig_id}" if prefix else orig_id
                if new_id in category_id_map and category_id_map[new_id] != domain:
                    new_id = f"{new_id}9"
                category_id_map[new_id] = domain
                cat.set('id', new_id)
                if cat.get('parentId'):
                    cat.set('parentId', f"{prefix}{cat.get('parentId')}" if prefix else cat.get('parentId'))
                final_categories[new_id] = cat

            # Обробка товарів
            for offer in root.xpath(".//offer"):
                avail = offer.get('available', '').lower() in ['true', 'yes', '1']
                if not avail:
                    count_no += 1
                    continue

                qty_nodes = offer.xpath(".//quantity|.//stock_quantity|.//amount")
                qty = 3
                if qty_nodes and qty_nodes[0].text:
                    try:
                        qty = int(re.sub(r'\D', '', qty_nodes[0].text))
                        if qty <= 0: qty = 3
                    except: qty = 3

                p_node = offer.find('price')
                if p_node is None: continue
                
                try:
                    raw_p = float(p_node.text.replace(',', '.'))
                    price = round(raw_p * MARKUP_PERCENT + MARKUP_FIXED)
                    if price < MIN_PRICE_THRESHOLD:
                        count_low += 1
                        continue
                    
                    price_promo = round(price * (1 - PROMO_DISCOUNT))
                    vendor = offer.findtext('vendor') or "NoBrand"
                    name_ua = fix_text(offer.findtext('name_ua') or offer.findtext('name'))
                    if vendor.lower() not in name_ua.lower():
                        name_ua = f"{name_ua} {vendor}"

                    new_off = ET.Element("offer", id=f"{prefix if prefix else '6'}_{offer.get('id')}", available="true")
                    ET.SubElement(new_off, "name_ua").text = name_ua[:250]
                    ET.SubElement(new_off, "price").text = str(price)
                    ET.SubElement(new_off, "price_promo").text = str(price_promo)
                    ET.SubElement(new_off, "currencyId").text = "UAH"
                    
                    orig_cat = offer.findtext('categoryId')
                    cat_id = f"{prefix}{orig_cat}" if prefix else orig_cat
                    ET.SubElement(new_off, "categoryId").text = cat_id
                    
                    ET.SubElement(new_off, "vendor").text = vendor
                    ET.SubElement(new_off, "stock_quantity").text = str(qty)
                    
                    desc = clean_description(offer.findtext('description_ua') or offer.findtext('description'), name_ua, vendor)
                    ET.SubElement(new_off, "description_ua").text = ET.CDATA(desc)

                    for pic in offer.findall('picture'):
                        if pic.text: ET.SubElement(new_off, "picture").text = pic.text

                    params = offer.findall('param')
                    if not params:
                        ET.SubElement(new_off, "param", name="Стан").text = "Новий"
                    else:
                        for p in params:
                            p.text = fix_text(p.text)
                            new_off.append(p)

                    processed_offers.append(new_off)
                    count_ok += 1
                except: continue
            
            source_results.append(f"{domain}: OK:{count_ok}, LowPrice:{count_low}, NoStock:{count_no}")
        except: continue

    # 3. ЗБІРКА XML З КОРЕКТНОЮ СТРУКТУРОЮ КУРСІВ
    yml = ET.Element("yml_catalog", date=datetime.now().strftime("%Y-%m-%d %H:%M"))
    shop = ET.SubElement(yml, "shop")
    
    # КРИТИЧНО: Повертаємо блок валют, який вимагає EVA
    currencies = ET.SubElement(shop, "currencies")
    ET.SubElement(currencies, "currency", id="UAH", rate="1")
    
    cats_n = ET.SubElement(shop, "categories")
    for c in final_categories.values(): cats_n.append(c)
    
    offers_n = ET.SubElement(shop, "offers")
    for o in processed_offers: offers_n.append(o)

    with open("Masterevanew.xml", "wb") as f:
        f.write(ET.tostring(yml, encoding='utf-8', xml_declaration=True, pretty_print=True))
    
    print("\n--- СТАТИСТИКА ПІСЛЯ ВИПРАВЛЕННЯ ---")
    for res in source_results: print(res)

if __name__ == "__main__":
    process()
