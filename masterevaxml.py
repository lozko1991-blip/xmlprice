import os
import re
import requests
from lxml import etree as ET
from datetime import datetime
from html import unescape

# --- КОНФІГУРАЦІЯ ПОСТАЧАЛЬНИКІВ ТА ПРАВИЛА НАЦІНКИ ---
SOURCES = [
    {
        "url": "https://feed.lugi.com.ua/",  # URL фідів постачальника Lugi
        "prefix": "lg_",
        "domain": "lugi.com.ua"
    }
]

MARKUP_PERCENT = 1.35   # +35%
MARKUP_FIXED = 40       # +40 грн
MIN_PRICE_THRESHOLD = 150
DESC_LIMIT = 2800

def fix_text(text):
    if not text:
        return ""
    text = unescape(unescape(text))
    # Заміна стандартних апострофів на правильні для EVA
    text = text.replace("'", "’").replace("`", "’")
    return text.strip()

def clean_description(text, name_ua, vendor):
    if not text:
        return f"<p>{name_ua} від виробника {vendor}.</p>"
    text = unescape(unescape(text))
    text = re.sub(r'<(script|style).*?>.*?</\1>', '', text, flags=re.DOTALL)
    text = re.sub(r'https?://\S+|www\.\S+', '', text)
    text = re.sub(r'\sstyle="[^"]*"', '', text)
    if len(text) > DESC_LIMIT:
        text = text[:DESC_LIMIT] + "..."
    return text.strip()

def main():
    print("=== MASTEREVANEW1: СТАРТ ОБРОБКИ ===")
    
    # Створення базової структури фінального файлу
    xml_date = datetime.now().strftime("%Y-%m-%d %H:%M")
    root_out = ET.Element("yml_catalog", date=xml_date)
    shop_out = ET.SubElement(root_out, "shop")
    
    # ЗАПОВІДЬ: Валюта суворо UAH
    currencies_out = ET.SubElement(shop_out, "currencies")
    ET.SubElement(currencies_out, "currency", id="UAH", rate="1")
    
    categories_out = ET.SubElement(shop_out, "categories")
    offers_out = ET.SubElement(shop_out, "offers")
    
    category_id_map = {}
    final_categories = {}
    
    for src in SOURCES:
        url = src["url"]
        prefix = src["prefix"]
        domain = src["domain"]
        
        print(f"Завантаження даних з домену: {domain}...")
        try:
            response = requests.get(url, timeout=60)
            response.raise_for_status()
            parser = ET.XMLParser(recover=True, remove_blank_text=True)
            root = ET.fromstring(response.content, parser=parser)
        except Exception as e:
            print(f"Помилка завантаження/парсингу {domain}: {e}")
            continue
            
        # 1. ОБРОБКА КАТЕГОРІЙ
        for cat in root.xpath(".//category"):
            orig_id = cat.get('id')
            new_id = f"{prefix}{orig_id}" if prefix else orig_id
            
            if new_id in category_id_map and category_id_map[new_id] != domain:
                new_id = f"{new_id}9"
                
            category_id_map[new_id] = domain
            cat.set('id', new_id)
            
            if cat.get('parentId'):
                p_id = cat.get('parentId')
                cat.set('parentId', f"{prefix}{p_id}" if prefix else p_id)
                
            final_categories[new_id] = cat

        # 2. ОБРОБКА ТОВАРІВ
        count_ok = 0
        count_no = 0
        count_low = 0
        
        offers = root.xpath(".//offer")
        print(f"Знайдено товарів у джерела: {len(offers)}")
        
        for offer in offers:
            # Фільтрація за наявністю
            avail = offer.get('available', '').lower()
            if avail not in ['true', 'yes', '1']:
                count_no += 1
                continue
                
            # Корекція кількості на складі
            qty = 0
            for tag in ['quantity', 'stock_quantity', 'amount']:
                q_text = offer.findtext(tag)
                if q_text:
                    try:
                        qty = int(re.sub(r'\D', '', q_text))
                        break
                    except ValueError:
                        continue
            if qty <= 0:
                qty = 3
                
            vendor = fix_text(offer.findtext('vendor')) or "NoBrand"
            
            # Назва товару
            name_ua = offer.findtext('name_ua') or offer.findtext('name')
            name_ua = fix_text(name_ua)
            if vendor.lower() not in name_ua.lower():
                name_ua = f"{name_ua} {vendor}"
            name_ua = name_ua[:250]
            
            # ЗАПОВІДЬ: Створення нового offer, ID БЕЗ ПРЕФІКСІВ
            orig_offer_id = offer.get('id')
            new_offer = ET.Element("offer", id=orig_offer_id, available="true")
            
            # --- ОНОВЛЕНИЙ БЛОК ОБРОБКИ ЦІН (ФІКС LUGI: PRICE ТА OLD_PRICE) ---
            try:
                # Шукаємо тег price строго ЯК ПРЯМИЙ ДОЧІРНІЙ елемент поточного offer
                price_node = offer.find('./price')
                
                if price_node is None or not price_node.text:
                    # Резервний пошук всередині поточного дерева offer, якщо структура засунута глибше
                    price_node = offer.find('.//price')

                if price_node is None or not price_node.text:
                    count_no += 1
                    continue

                raw_price = price_node.text.strip().replace(',', '.')
                original_price = float(raw_price)

                # Математичний розрахунок націнки для нашої поточної ціни продажу
                calculated_price = round(original_price * MARKUP_PERCENT + MARKUP_FIXED)

                # Перевірка мінімального ліміту в 150 грн
                if calculated_price < MIN_PRICE_THRESHOLD:
                    count_low += 1
                    continue

                # Запис ціни продажу (price)
                new_price_node = ET.SubElement(new_offer, 'price')
                new_price_node.text = str(calculated_price)

                # Запис старої закресленої ціни (old_price)
                new_old_price_node = ET.SubElement(new_offer, 'old_price')
                new_old_price_node.text = str(calculated_price)

                # Валюта
                new_curr = ET.SubElement(new_offer, 'currencyId')
                new_curr.text = "UAH"

            except (ValueError, TypeError):
                count_no += 1
                continue
            # --- КІНЕЦЬ ОНОВЛЕНОГО БЛОКУ ОБРОБКИ ЦІН ---
            
            # Прив'язка категорії з префіксом
            orig_cat = offer.findtext('categoryId')
            cat_id = f"{prefix}{orig_cat}" if prefix else orig_cat
            ET.SubElement(new_offer, "categoryId").text = cat_id
            
            # Назва та бренд
            ET.SubElement(new_offer, "name_ua").text = name_ua
            ET.SubElement(new_offer, "vendor").text = vendor
            
            # Картинки
            for pic in offer.xpath("picture"):
                if pic.text:
                    ET.SubElement(new_offer, "picture").text = pic.text.strip()
                    
            # Опис товару в блоці CDATA
            desc_text = offer.findtext('description_ua') or offer.findtext('description')
            cleaned_desc = clean_description(desc_text, name_ua, vendor)
            desc_node = ET.SubElement(new_offer, "description_ua")
            desc_node.text = ET.CDATA(cleaned_desc)
            
            # Кількість
            ET.SubElement(new_offer, "quantity").text = str(qty)
            
            # Параметри / Характеристики
            params = offer.xpath("param")
            if not params:
                p_state = ET.SubElement(new_offer, "param", name="Стан")
                p_state.text = "Новий"
            else:
                for p in params:
                    p.text = fix_text(p.text)
                    new_offer.append(p)
                    
            offers_out.append(new_offer)
            count_ok += 1
            
        print(f"Успішно додано: {count_ok} | Пропущено (немає/відсутній): {count_no} | Відсіяно за ціною (<150): {count_low}")

    # Збирання категорій у фінальне дерево
    for c_id in sorted(final_categories.keys()):
        categories_out.append(final_categories[c_id])
        
    # Збереження готового результату
    output_filename = "Masterevanew.xml"
    tree_out = ET.ElementTree(root_out)
    tree_out.write(output_filename, xml_declaration=True, encoding="utf-8", pretty_print=True)
    print(f"=== ЗБЕРЕЖЕНО ФАЙЛ: {output_filename} ===")

if __name__ == "__main__":
    main()
