import requests
import lxml.etree as ET
from datetime import datetime
import sys
import re

SOURCES = [
    "https://feed.lugi.com.ua/index.php?route=extension/feed/unixml/ukr_ru",
    "https://dropom.com.ua/products_feed.xml?hash_tag=b55924e4ebc0576fda79ae6941f7a2a5&sales_notes=&product_ids=&label_ids=&exclude_fields=&html_description=1&yandex_cpa=&process_presence_sure=&languages=uk%2Cru&extra_fields=quantityInStock%2Ckeywords&group_ids=",
    "https://opt-drop.com/storage/xml/opt-drop-5.xml",
    "http://kievopt.com.ua/prices/rozetka-22294.yml",
    "https://dwn.royaltoys.com.ua/my/export/v2/e6f6dcf6-2539-4a43-a285-32667169f0db.xml"
]

MARKUP_PERCENT = 1.35
MARKUP_FIXED = 40

def clean_text(text):
    if text is None: return ""
    text = text.replace('&lt;', '<').replace('&gt;', '>').replace('&amp;', '&').replace('&nbsp;', ' ')
    text = re.sub(r'<(?!/?(p|br|b|strong|ul|li)\b)[^>]+>', '', text)
    text = re.sub(r'([.,!?;:])(?=[^\s\d])', r'\1 ', text)
    text = re.sub(r'\r|\n|\t', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

def process():
    all_raw_data = []
    id_usage_count = {} # Словник для підрахунку дублікатів ID

    # Крок 1: Попереднє завантаження та пошук дублікатів ID
    for index, url in enumerate(SOURCES):
        domain = url.split('/')[2]
        try:
            print(f"Сканування {domain}...")
            r = requests.get(url, timeout=60)
            if r.ok:
                parser = ET.XMLParser(recover=True, encoding='utf-8')
                root = ET.fromstring(r.content, parser=parser)
                
                categories = root.xpath(".//category")
                offers = root.xpath(".//offer")
                
                # Рахуємо унікальні ID у розрізі постачальників
                current_source_ids = set()
                for cat in categories:
                    cid = cat.get('id')
                    if cid: current_source_ids.add(cid)
                
                for cid in current_source_ids:
                    id_usage_count[cid] = id_usage_count.get(cid, 0) + 1
                
                all_raw_data.append({
                    'prefix': str(index + 1),
                    'domain': domain,
                    'categories': categories,
                    'offers': offers
                })
        except Exception as e:
            print(f"Помилка при скануванні {domain}: {e}")

    # Визначаємо, які ID є проблемними (зустрічаються більше ніж в 1 постачальника)
    duplicate_ids = {cid for cid, count in id_usage_count.items() if count > 1}

    final_categories = []
    final_offers = []
    stats = []

    # Крок 2: Обробка даних з розумним перейменуванням
    for data in all_raw_data:
        prefix = data['prefix']
        domain = data['domain']
        count = 0
        
        # Обробка категорій
        for cat in data['categories']:
            cid = cat.get('id')
            pid = cat.get('parentId')
            
            if cid in duplicate_ids:
                cat.set('id', f"{prefix}_{cid}")
                if pid: cat.set('parentId', f"{prefix}_{pid}")
            final_categories.append(cat)

        # Обробка товарів
        for offer in data['offers']:
            avail = offer.get('available')
            if avail in ['true', 'yes', '1']:
                # Мапінг категорії товару
                cat_node = offer.find('categoryId')
                if cat_node is not None:
                    cid = cat_node.text
                    if cid in duplicate_ids:
                        cat_node.text = f"{prefix}_{cid}"

                # Ціни та тексти
                for tag in ['price', 'oldprice']:
                    n = offer.find(tag)
                    if n is not None and n.text:
                        try:
                            val = float(n.text)
                            n.text = str(round(val * MARKUP_PERCENT + MARKUP_FIXED))
                        except: pass

                for d_tag in ['description', 'description_ua']:
                    d_node = offer.find(d_tag)
                    if d_node is not None:
                        d_node.text = ET.CDATA(clean_text(d_node.text))

                # Очищення та кількість
                offer.set('available', 'true')
                for extra in ['stock_quantity', 'quantity_in_stock', 'pickup']:
                    node = offer.find(extra)
                    if node is not None: offer.remove(node)
                
                q = offer.find('quantity') or ET.SubElement(offer, 'quantity')
                q.text = "5"
                
                if offer.find('vendor') is None:
                    ET.SubElement(offer, 'vendor').text = "Brand"
                
                final_offers.append(offer)
                count += 1
        stats.append(f"{domain}: OK (+{count})")

    # Фінальна збірка
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

if __name__ == "__main__":
    process()
