import requests
import lxml.etree as ET
from datetime import datetime
import sys

SOURCES = [
    "https://feed.lugi.com.ua/index.php?route=extension/feed/unixml/ukr_ru",
    "https://dropom.com.ua/products_feed.xml?hash_tag=b55924e4ebc0576fda79ae6941f7a2a5&sales_notes=&product_ids=&label_ids=&exclude_fields=&html_description=1&yandex_cpa=&process_presence_sure=&languages=uk%2Cru&extra_fields=quantityInStock%2Ckeywords&group_ids=",
    "https://opt-drop.com/storage/xml/opt-drop-5.xml",
    "http://kievopt.com.ua/prices/rozetka-22294.yml",
    "https://dwn.royaltoys.com.ua/my/export/v2/e6f6dcf6-2539-4a43-a285-32667169f0db.xml"
]

MARKUP_PERCENT = 1.35
MARKUP_FIXED = 40

def process():
    all_categories = {}
    all_offers = []
    stats = []
    cat_id_sources = {}

    for url in SOURCES:
        domain = url.split('/')[2]
        try:
            print(f"Завантаження {domain}...")
            r = requests.get(url, timeout=60)
            if not r.ok:
                stats.append(f"{domain}: Помилка HTTP {r.status_code}")
                continue
            
            # Використовуємо recover=True для ігнорування дрібних помилок в XML
            parser = ET.XMLParser(recover=True, encoding='utf-8')
            root = ET.fromstring(r.content, parser=parser)
            
            if root is None:
                stats.append(f"{domain}: Порожній або битий XML")
                continue

            # Обробка категорій
            for cat in root.xpath(".//category"):
                cid = cat.get('id')
                if cid:
                    name = cat.text or "Категорія"
                    if cid not in cat_id_sources: cat_id_sources[cid] = []
                    cat_id_sources[cid].append(f"{domain} ({name})")
                    all_categories[cid] = cat

            # Обробка товарів
            count = 0
            for offer in root.xpath(".//offer"):
                avail = offer.get('available')
                if avail in ['true', 'yes', '1']:
                    # Ціна
                    p_node = offer.find('price')
                    if p_node is not None and p_node.text:
                        try:
                            val = float(p_node.text)
                            p_node.text = str(round(val * MARKUP_PERCENT + MARKUP_FIXED))
                        except: pass
                    
                    # Стара ціна
                    op_node = offer.find('oldprice')
                    if op_node is not None and op_node.text:
                        try:
                            val = float(op_node.text)
                            op_node.text = str(round(val * MARKUP_PERCENT + MARKUP_FIXED))
                        except: pass

                    # EVA Налаштування
                    offer.set('available', 'true')
                    q = offer.find('quantity')
                    if q is None: q = ET.SubElement(offer, 'quantity')
                    q.text = "5"

                    if offer.find('vendor') is None or not offer.find('vendor').text:
                        v = offer.find('vendor')
                        if v is None: v = ET.SubElement(offer, 'vendor')
                        v.text = "Brand"
                    
                    all_offers.append(offer)
                    count += 1
            stats.append(f"{domain}: OK (+{count} товарів)")
        except Exception as e:
            stats.append(f"{domain}: Помилка ({str(e)[:50]})")

    if not all_offers:
        print("Критична помилка: Не вдалося завантажити жодного товару!")
        sys.exit(1)

    # Фільтрація
    cat_counts = {}
    for o in all_offers:
        cid = o.findtext('categoryId')
        cat_counts[cid] = cat_counts.get(cid, 0) + 1
    
    valid_ids = {cid for cid, n in cat_counts.items() if n >= 4}

    # Збірка фіналу
    yml = ET.Element("yml_catalog", date=datetime.now().strftime("%Y-%m-%d %H:%M"))
    shop = ET.SubElement(yml, "shop")
    ET.SubElement(shop, "name").text = "Master Shop EVA"
    
    currs = ET.SubElement(shop, "currencies")
    ET.SubElement(currs, "currency", id="UAH", rate="1")
    
    cats_node = ET.SubElement(shop, "categories")
    for cid, cat in all_categories.items():
        if cid in valid_ids: cats_node.append(cat)
        
    offers_node = ET.SubElement(shop, "offers")
    for o in all_offers:
        cid = o.findtext('categoryId')
        if cid in valid_ids: offers_node.append(o)

    # Запис
    stats_txt = "\n".join(stats)
    header = f"\n"
    
    with open("Masterevanew.xml", "wb") as f:
        f.write(header.encode('utf-8'))
        f.write(ET.tostring(yml, encoding='utf-8', xml_declaration=False))

if __name__ == "__main__":
    process()
