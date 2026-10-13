# content_overrides.py
# ==============================================================================
# НАЛАШТУВАННЯ НАЗВ, ОПИСІВ ТА ДУБЛІВ ТОВАРІВ
# ==============================================================================
# 
# УВАГА: Найпростіший спосіб змінювати товари та створювати дублі —
# це редагувати звичайний текстовий файл overrides.txt на GitHub!
# Там немає складного коду Python, лапок чи ком — впорається навіть дитина.
# ==============================================================================

import os
import re

OVERRIDES_FILE = os.path.join(os.path.dirname(__file__), "overrides.txt")

# Стандартні словники для сумісності з Python
PRODUCT_OVERRIDES = {}
PRODUCT_CLONES = []


def parse_overrides_text(text):
    """
    Парсить простий текстовий формат з overrides.txt.
    """
    overrides = {}
    clones = []
    
    current_block_type = None  # 'override' або 'clone'
    current_key = None         # ID або артикул
    current_data = {}
    current_desc_lines = []
    in_desc = False

    def finish_current_block():
        nonlocal current_block_type, current_key, current_data, current_desc_lines, in_desc
        if in_desc and current_desc_lines:
            desc_text = "\n".join(current_desc_lines).strip()
            if desc_text:
                current_data["description_ua"] = desc_text
            current_desc_lines = []
            in_desc = False

        if current_block_type == "override" and current_key and current_data:
            overrides[current_key.strip().upper()] = current_data
        elif current_block_type == "clone" and current_data:
            if not current_data.get("source") and current_key:
                current_data["source"] = current_key.strip().upper()
            if current_data.get("source"):
                # Автоматичний суфікс якщо не вказано
                if not current_data.get("id_suffix"):
                    art = current_data.get("article", "")
                    src = current_data.get("source", "")
                    if art and src and art.upper().startswith(src.upper()):
                        diff = art[len(src):].strip("-_ ")
                        current_data["id_suffix"] = diff if diff else "clone"
                    else:
                        current_data["id_suffix"] = "clone"
                if not current_data.get("article"):
                    current_data["article"] = f"{current_data['source']}-{current_data['id_suffix']}"
                clones.append(current_data)

        current_block_type = None
        current_key = None
        current_data = {}

    for line in text.splitlines():
        raw_line = line.strip()

        # Перевірка на заголовок блоку [ ... ] або === ... ===
        header_content = None
        if raw_line.startswith('[') and raw_line.endswith(']'):
            header_content = raw_line[1:-1].strip()
        elif raw_line.startswith('===') and raw_line.endswith('==='):
            header_content = raw_line.strip('=').strip()

        if header_content is not None:
            finish_current_block()
            
            hc_upper = header_content.upper()
            is_clone = any(w in hc_upper for w in ("ДУБЛЬ", "КЛОН", "CLONE", "DUPLICATE", "DUPL"))
            
            clean_hdr = re.sub(
                r'^(?:ДУБЛЬ|КЛОН|CLONE|DUPLICATE|ТОВАР|ARTICLE|ITEM|PRODUCT)\s*[:=\s]*', 
                '', 
                header_content, 
                flags=re.IGNORECASE
            ).strip()
            
            if is_clone:
                current_block_type = "clone"
                current_key = clean_hdr if clean_hdr else None
                current_data = {"params": {}}
                if current_key:
                    current_data["source"] = current_key.strip().upper()
            else:
                current_block_type = "override"
                current_key = clean_hdr if clean_hdr else None
                current_data = {}
            continue

        if in_desc:
            key_val_match = re.match(
                r'^(назва|код|артикул|оригінал|джерело|параметри?|суфікс)\s*[:=]\s*(.*)$', 
                raw_line, 
                flags=re.IGNORECASE
            )
            if not key_val_match:
                current_desc_lines.append(line)
                continue
            else:
                in_desc = False
                desc_text = "\n".join(current_desc_lines).strip()
                if desc_text:
                    current_data["description_ua"] = desc_text
                current_desc_lines = []

        if not raw_line or raw_line.startswith(('#', '//', ';')):
            continue

        if ':' in raw_line or '=' in raw_line:
            sep = ':' if ':' in raw_line else '='
            k_part, v_part = raw_line.split(sep, 1)
            k = k_part.strip().lower()
            v = v_part.strip()

            if k in ('назва', 'назва_ua', 'name', 'title'):
                current_data["name_ua"] = v
            elif k in ('артикул', 'article'):
                if current_block_type == "clone":
                    current_data["article"] = v
                else:
                    if not current_key:
                        current_key = v.strip().upper()
            elif k in ('оригінал', 'джерело', 'source'):
                if current_block_type == "clone":
                    current_data["source"] = v.strip().upper()
            elif k in ('код', 'код дубля', 'суфікс', 'suffix', 'id_suffix'):
                if current_block_type == "clone":
                    current_data["id_suffix"] = v
                else:
                    if not current_key:
                        current_key = v.strip().upper()
            elif k in ('опис', 'description', 'desc', 'опис_ua'):
                in_desc = True
                if v:
                    current_desc_lines.append(v)
            else:
                if current_block_type == "clone":
                    current_data.setdefault("params", {})[k_part.strip()] = v

    finish_current_block()
    return overrides, clones


def load_all_overrides():
    """
    Завантажує налаштування з overrides.txt та об'єднує з конфігом у Python.
    """
    all_overrides = {}
    all_clones = []

    # 1. Завантажуємо з текстового файлу overrides.txt (якщо існує)
    if os.path.exists(OVERRIDES_FILE):
        try:
            with open(OVERRIDES_FILE, "r", encoding="utf-8") as f:
                content = f.read()
            text_overrides, text_clones = parse_overrides_text(content)
            all_overrides.update(text_overrides)
            all_clones.extend(text_clones)
        except Exception as e:
            print(f"[ПОМИЛКА ЧИТАННЯ overrides.txt]: {e}")

    # 2. Об'єднуємо з налаштуваннями з Python (якщо вказані)
    for k, v in PRODUCT_OVERRIDES.items():
        all_overrides[str(k).strip().upper()] = v
    all_clones.extend(PRODUCT_CLONES)

    return all_overrides, all_clones


def get_product_override(offer_id=None, raw_id=None, article=None):
    """
    Знаходить перевизначення назви/опису для товару.
    Перевіряє: артикул, повний offer_id, чистий raw_id (нечутливо до регістру).
    """
    all_overrides, _ = load_all_overrides()
    if not all_overrides:
        return None

    # Прямий або регістронезалежний збіг
    candidates = []
    for item in (article, offer_id, raw_id):
        if item:
            candidates.append(str(item).strip().upper())

    for c in candidates:
        if c in all_overrides:
            return all_overrides[c]

    return None


def get_product_clones(offer_id=None, raw_id=None, article=None):
    """
    Повертає список конфігурацій дублів (клонів) для даного товару.
    """
    _, all_clones = load_all_overrides()
    if not all_clones:
        return []

    candidates = set()
    for item in (article, offer_id, raw_id):
        if item:
            candidates.add(str(item).strip().upper())

    matching_clones = []
    for clone_cfg in all_clones:
        source_key = clone_cfg.get("source")
        if source_key and str(source_key).strip().upper() in candidates:
            matching_clones.append(clone_cfg)

    return matching_clones
