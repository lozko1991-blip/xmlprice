import sys
import os
import json
import time
import requests
from lxml import etree as ET

# Attempt to import Google Translator
try:
    from deep_translator import GoogleTranslator
except ImportError:
    print("Встановіть deep-translator: pip install deep-translator")
    sys.exit(1)

# Додаємо поточну директорію в sys.path, щоб імпортувати SOURCES
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from masterevaxml import SOURCES

TRANSLATIONS_FILE = "translations.json"

def load_translations():
    if os.path.exists(TRANSLATIONS_FILE):
        with open(TRANSLATIONS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_translations(translations):
    with open(TRANSLATIONS_FILE, "w", encoding="utf-8") as f:
        json.dump(translations, f, ensure_ascii=False, indent=4)

from collections import Counter

from collections import Counter

def fetch_and_extract_params():
    print("Збираємо всі унікальні параметри з XML фідів...")
    name_counter = Counter()
    value_counter = Counter()
    
    for _id, _pass, url in SOURCES:
        if not url:
            continue
        try:
            print(f"Завантаження {url}...")
            resp = requests.get(url, timeout=20)
            resp.raise_for_status()
            
            parser = ET.XMLParser(recover=True, huge_tree=True)
            root = ET.fromstring(resp.content, parser=parser)
            
            # Шукаємо всі параметри
            for p in root.xpath(".//param"):
                name = (p.get('name') or "").strip()
                val = (p.text or "").strip()
                if name:
                    name_counter[name] += 1
                if val:
                    value_counter[val] += 1
                    
        except Exception as e:
            print(f"Помилка завантаження {url}: {e}")

    final_set = set()
    
    # Додаємо ТОП-1000 найпопулярніших назв
    top_1000_names = [name for name, count in name_counter.most_common(1000)]
    final_set.update(top_1000_names)
    print(f"Додано {len(top_1000_names)} найпопулярніших назв характеристик.")
    
    # Додаємо ТОП-100 найпопулярніших значень
    top_100_values = [val for val, count in value_counter.most_common(100)]
    final_set.update(top_100_values)
    print(f"Додано {len(top_100_values)} найпопулярніших значень характеристик.")

    return final_set

def translate_batch_and_save(texts, translations):
    if not texts:
        return
    
    translator = GoogleTranslator(source='ru', target='uk')
    
    chunk_size = 50
    for i in range(0, len(texts), chunk_size):
        chunk = texts[i:i+chunk_size]
        print(f"  Переклад слів {i+1}..{min(i+chunk_size, len(texts))} з {len(texts)}")
        
        chunk_translated = []
        try:
            chunk_translated = translator.translate_batch(chunk)
        except Exception as e:
            print(f"  Помилка перекладу: {e}. Переходимо на поштучний переклад пачки...")
            for word in chunk:
                try:
                    res = translator.translate(word)
                    chunk_translated.append(res)
                except Exception as ex:
                    print(f"    Не вдалось перекласти '{word}': {ex}")
                    chunk_translated.append(word)
                    
        # Зберігаємо одразу після кожної пачки
        for orig, trans in zip(chunk, chunk_translated):
            translations[orig] = trans if trans else orig
            
        save_translations(translations)
        time.sleep(1)
    
    return

def main():
    translations = load_translations()
    print(f"Завантажено {len(translations)} вже існуючих перекладів з {TRANSLATIONS_FILE}.")
    
    unique_texts = fetch_and_extract_params()
    print(f"Знайдено {len(unique_texts)} унікальних слів/фраз у фідах.")
    
    # Відфільтрувати те, що вже перекладено, або пусті рядки
    to_translate = []
    for text in unique_texts:
        if not text:
            continue
        # Пропускаємо суто цифри або дуже прості значення, які не потребують перекладу
        # Але краще просто перевірити, чи є вони в кеші
        if text not in translations:
            to_translate.append(text)
            
    if not to_translate:
        print("Всі параметри вже перекладені. Нічого робити!")
        return
        
    print(f"Потрібно перекласти нових слів: {len(to_translate)}")
    
    # Відправляємо на переклад і зберігаємо
    translate_batch_and_save(to_translate, translations)
    
    print(f"\nГотово! Словник оновлено. Тепер він містить {len(translations)} записів.")
    
if __name__ == "__main__":
    main()
