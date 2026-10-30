#!/usr/bin/env python3
import sys
import os
import re
import json
import subprocess
from PIL import Image

def parse_image(image_path):
    try:
        im = Image.open(image_path)
    except Exception as e:
        return {"error": f"Cannot open image: {e}"}
        
    w, h = im.size
    
    # Resize for fast OCR if large
    if w > 1600:
        new_h = int(h * (1600 / w))
        im = im.resize((1600, new_h), Image.Resampling.LANCZOS)
        w, h = im.size
        
    temp_full = "/tmp/ocr_full.png"
    im.save(temp_full)
    
    # 1. Full image OCR
    full_ocr = subprocess.run(
        ["tesseract", temp_full, "stdout", "--psm", "6", "-l", "ukr+eng"],
        capture_output=True, text=True
    ).stdout
    
    # 2. Spec crop (inner right column, carefully avoiding outer table borders)
    spec_crop = im.crop((int(w * 0.58), int(h * 0.20), int(w * 0.92), int(h * 0.48)))
    temp_spec = "/tmp/ocr_spec.png"
    spec_crop.save(temp_spec)
    spec_ocr = subprocess.run(
        ["tesseract", temp_spec, "stdout", "--psm", "6", "-l", "eng"],
        capture_output=True, text=True
    ).stdout
    
    return extract_fields(full_ocr, spec_ocr)

def extract_fields(full_ocr, spec_ocr=""):
    lines = [l.strip() for l in spec_ocr.splitlines() if l.strip()]
    combined = full_ocr + "\n" + spec_ocr
    
    result = {
        "client_name": "",
        "total_buckets": 1,
        "material": "Ceresit CT 174 (1.5mm) Камінцева",
        "collection": "",
        "color_code": "",
        "bucket_weight": "25 кг",
        "base": "База A"
    }
    
    # 1. Quantity
    qty_m = re.search(r'(\d+)\s*(?:шт|ш/|wt|ut|pcs)', full_ocr, re.IGNORECASE)
    if qty_m:
        result["total_buckets"] = int(qty_m.group(1))
        
    # 2. Client name (line right before or beside quantity)
    client_m = re.search(r'(?:^|\n)([A-Za-zА-Яа-яІіЇїЄє0-9\s/№#\-]{2,25})\s+\d+\s*(?:шт|ш/|wt)', full_ocr)
    if client_m:
        cand = client_m.group(1).strip()
        bad_words = ['продукт', 'білий', 'хенкел', 'сума', 'рецепт', 'тара', 'база', 'колекція', 'ціна']
        if not any(bad in cand.lower() for bad in bad_words):
            result["client_name"] = cand
            
    # 3. Material
    mat_m = re.search(r'(?:CT|СТ|CТ|СT)\s*(\d{2,3})(?:/([0-9.]+))?', combined, re.IGNORECASE)
    if mat_m:
        code = mat_m.group(1)
        fraction = mat_m.group(2)
        if code == "174":
            result["material"] = f"Ceresit CT 174 ({fraction or '1.5'}mm) Камінцева"
        elif code == "60":
            result["material"] = f"Ceresit CT 60 ({fraction or '1.5'}mm) Камінцева"
        elif code == "16":
            result["material"] = "Ceresit CT 16 Грунтуюча фарба"
        elif code == "74":
            result["material"] = f"Ceresit CT 74 ({fraction or '1.5'}mm) Силіконова"
        elif code == "48":
            result["material"] = "Ceresit CT 48 Силіконова фарба"
        elif code == "54":
            result["material"] = "Ceresit CT 54 Силікатна фарба"
        elif code == "175":
            result["material"] = "Ceresit CT 175 Силікон-силікатна"
            
    # 4. Collection (RAL Classic, NCS, Colours of Nature, RAL Design)
    for l in lines:
        if re.search(r'RAL\s*Classic', l, re.IGNORECASE):
            result["collection"] = "RAL Classic"
            break
        elif re.search(r'\bNCS\b', l, re.IGNORECASE):
            result["collection"] = "NCS"
            break
        elif re.search(r'Colours?\s*of\s*Nature', l, re.IGNORECASE):
            result["collection"] = "Colours of Nature"
            break

    if not result["collection"]:
        if re.search(r'RAL\s*Classic', combined, re.IGNORECASE):
            result["collection"] = "RAL Classic"
        elif re.search(r'\bNCS\b', combined, re.IGNORECASE):
            result["collection"] = "NCS"
        elif re.search(r'Colours?\s*of\s*Nature', combined, re.IGNORECASE):
            result["collection"] = "Colours of Nature"
        
    # 5. Color Code
    cleaned_spec = re.sub(r'RAL\s*Classic|RAL|NCS|Colours\s*of\s*Nature', '', spec_ocr, flags=re.IGNORECASE).strip()
    cleaned_comb = re.sub(r'RAL\s*Classic|RAL|NCS|Colours\s*of\s*Nature', '', combined, flags=re.IGNORECASE).strip()
    
    color_m = re.search(r'(\b[A-Z0-9]{1,4}-\w+|\bS\s*\d{4}-[A-Z0-9]+)', cleaned_spec)
    if not color_m:
        color_m = re.search(r'(\b[A-Z0-9]{1,4}-\w+|\bS\s*\d{4}-[A-Z0-9]+)', cleaned_comb)
    if not color_m:
        for cand_m in re.finditer(r'(\b\d{4}\b|\d{4}\b)', cleaned_spec):
            c = cand_m.group(1).strip()
            if c not in ['1174', '174', '2026', '90000', '25', '1.5']:
                color_m = cand_m
                break
    if not color_m:
        for cand_m in re.finditer(r'(\b\d{4}\b|\d{4}\b)', cleaned_comb):
            c = cand_m.group(1).strip()
            if c not in ['1174', '174', '2026', '90000', '25', '1.5']:
                color_m = cand_m
                break
    if not color_m:
        for cand_m in re.finditer(r'([A-Za-zА-Яа-яІіЇїЄє]{3,}\s+\d+)', cleaned_comb):
            c = cand_m.group(1).strip()
            if not any(bad in c.lower() for bad in ['лац 1', 'ціна', 'сума', 'продукт', 'білий']):
                color_m = cand_m
                break
                
    if color_m:
        result["color_code"] = color_m.group(1).strip()

    # 5b. If collection is empty, infer from color pattern
    if not result["collection"] and result["color_code"]:
        if re.match(r'^[A-Z0-9]{1,4}-\w+$', result["color_code"]) or result["color_code"].startswith("S "):
            result["collection"] = "NCS"
        elif re.match(r'^\d{4}$', result["color_code"]):
            result["collection"] = "RAL Classic"
        
    # 6. Weight / Tara (25 кг, 15 кг, 10 л = 15 кг, 1.7 л = 2.5 кг, 2.5 кг)
    if re.search(r'25\s*(?:кг|kr|kg|xr)', combined, re.IGNORECASE):
        result["bucket_weight"] = "25 кг"
    elif re.search(r'15\s*(?:кг|kr|kg)', combined, re.IGNORECASE):
        result["bucket_weight"] = "15 кг"
    elif re.search(r'10\s*(?:л|l)\b', combined, re.IGNORECASE):
        result["bucket_weight"] = "10 л"
    elif re.search(r'1[.,]7\s*(?:л|l)\b', combined, re.IGNORECASE):
        result["bucket_weight"] = "1.7 л"
    elif re.search(r'2[.,]5\s*(?:кг|kr|kg)\b', combined, re.IGNORECASE):
        result["bucket_weight"] = "2.5 кг"
    elif re.search(r'14\s*(?:кг|kr|kg)', combined, re.IGNORECASE):
        result["bucket_weight"] = "14 кг"
    elif re.search(r'5\s*(?:кг|л)', combined, re.IGNORECASE):
        result["bucket_weight"] = "5 кг / 5 л"
        
    # 7. Base (A, B, C)
    base_found = False
    for l in lines:
        bm = re.search(r'[_\-\s]*([ABCАВС])[_\-\s]*', l)
        if bm and len(l) <= 6 and not any(d in l for d in ['1', '2', '3', '4', '5', '6', '7', '8', '9', '0']):
            b = bm.group(1).upper().replace('А', 'A').replace('В', 'B').replace('С', 'C')
            result["base"] = f"База {b}"
            base_found = True
            break
            
    if not base_found:
        bm = re.search(r'(?:База|Base)\s*[:=\s]*([ABCАВС])\b', combined, re.IGNORECASE)
        if bm:
            b = bm.group(1).upper().replace('А', 'A').replace('В', 'B').replace('С', 'C')
            result["base"] = f"База {b}"
            
    return result

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(json.dumps({"error": "No input provided"}))
        sys.exit(1)
        
    arg = sys.argv[1]
    if os.path.exists(arg):
        data = parse_image(arg)
    else:
        data = extract_fields(arg)
        
    print(json.dumps(data, ensure_ascii=False))
