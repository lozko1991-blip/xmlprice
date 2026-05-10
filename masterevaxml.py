import requests
import lxml.etree as ET
from datetime import datetime

# Налаштування джерел
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
        source_domain = url.split('/')[2]
        try:
            print(f"Обробка {source_domain}...")
            resp = requests.get(url, timeout=60)
            if not resp.ok:
                stats.append(f"{source_domain}: Помилка завантаження (Код {resp.status_code})")
                continue
            
            parser = ET.XMLParser(recover=True, encoding='utf-8')
            root = ET.fromstring(resp.content, parser=parser)
            
            # Категорії
            for cat in root.xpath(".//category"):
                cid = cat.get('id')
                name = cat.text or "Без назви"
                if cid not in cat_id_sources:
                    cat_id_sources[cid] = []
                cat_id_sources[cid].append(f"{source_domain} ({name})")
                all_categories[cid] = cat

            # Товари
            count = 0
            for offer in root.xpath(".//offer"):
                avail = offer.get('available')
                if avail in ['true', 'yes', '1']:
                    # Ціни
                    p_node = offer.find('price')
                    if p_node is not None:
                        val = float(p_node.text or 0)
                        p_node.text = str(round(val * MARKUP_PERCENT + MARKUP_FIXED))
                    
                    op_node = offer.find('oldprice')
                    if op_node is not None and op_node.text:
                        val = float(op_node.text)
                        op_node.text = str(round(val * MARKUP_PERCENT + MARKUP_FIXED))

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
            stats.append(f"{source_domain}: Успішно ({count} товарів)")
        except Exception as e:
            stats.append(f"{source_domain}: Помилка ({str(e)[:40]})")

    # Фільтрація категорій < 4 товарів
    counts = {}
    for o in all_offers:
        cid = o.findtext('categoryId')
        counts[cid] = counts.get(cid, 0) + 1
    
    valid_ids = {cid for cid, n in counts.items() if n >= 4}
    duplicate_info = {cid: src for cid, src in cat_id_sources.items() if len(src) > 1}

    # Формування фіналу
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
        if o.findtext('categoryId') in valid_ids: offers_node.append(o)

    # Запис
    stats_txt = "\n".join(stats)
    dup_txt = "\n".join([f"ID {k}: {', '.join(v)}" for k, v in duplicate_info.items()]) or "Дублікатів не виявлено"
    
    header = f"""<?xml version="1.0" encoding="UTF-8"?>
\n"""
    
    with open("Masterevanew.xml", "wb") as f:
        f.write(header.encode('utf-8'))
        f.write(ET.tostring(yml, encoding='utf-8', pretty_print=True))

if __name__ == "__main__":
    process()
