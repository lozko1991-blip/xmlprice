import requests
import re

# Налаштування
SOURCE_URL = "http://kievopt.com.ua/prices/prom-1704.yml"
MIN_PRODUCTS = 4  # Видаляємо, якщо менше 4 товарів

def process_xml():
    print("Завантаження даних...")
    response = requests.get(SOURCE_URL)
    response.encoding = 'utf-8'
    xml_content = response.text

    # --- 1. Аналіз категорій ---
    # Знаходимо всі ID категорій, які згадуються у товарах
    product_category_ids = re.findall(r'<categoryId>([^<]+)', xml_content)
    
    counts = {}
    for cid in product_category_ids:
        cid = cid.strip()
        counts[cid] = counts.get(cid, 0) + 1

    # Визначаємо, які ID ми залишаємо (мінімум 4 товари)
    keep_ids = {cid for cid, count in counts.items() if count >= MIN_PRODUCTS}
    print(f"Категорій до залишення: {len(keep_ids)}")

    # --- 2. Обробка блоку <categories> ---
    def category_filter(match):
        full_tag = match.group(0)
        cat_id = match.group(1)
        # Залишаємо категорію тільки якщо її ID є в списку keep_ids
        if cat_id in keep_ids:
            return full_tag
        return ""

    # Видаляємо порожні та "малі" категорії зі списку
    xml_content = re.sub(r'<category id="([^"]+)"[^>]*>.*?</category>', category_filter, xml_content)

    # --- 3. Обробка блоку <offers> ---
    def offer_processor(match):
        offer = match.group(0)
        
        # Перевірка категорії товару
        cat_match = re.search(r'<categoryId>([^<]+)', offer)
        if not cat_match:
            return ""
        
        current_cat_id = cat_match.group(1).strip()
        
        # Якщо категорія товару видалена — видаляємо і товар
        if current_cat_id not in keep_ids:
            return ""

        # Додаємо Vendor (Виробник), якщо немає
        if '<vendor>' not in offer:
            offer = offer.replace('</categoryId>', '</categoryId>\n      <vendor>No Brand</vendor>')
        
        # Додаємо Колір, якщо немає тегів "Цвет" або "Колір"
        if 'name="Цвет"' not in offer and 'name="Колір"' not in offer:
            # Вставляємо перед закриттям offer
            offer = offer.replace('</offer>', '      <param name="Цвет">Комбинированный</param>\n    </offer>')

        return offer

    # Очищуємо та модифікуємо товари
    xml_content = re.sub(r'<offer[\s\S]*?</offer>', offer_processor, xml_content)

    # --- 4. Фінальне очищення структури ---
    # Видаляємо зайві порожні рядки, які могли виникнути після видалення тегів
    xml_content = re.sub(r'\n\s*\n', '\n', xml_content)

    with open("feed.xml", "w", encoding="utf-8") as f:
        f.write(xml_content)
    print("Готово! Файл feed.xml збережено.")

if __name__ == "__main__":
    process_xml()
