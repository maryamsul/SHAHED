#!/usr/bin/env python3
"""
check_villages.py
-----------------
Checks which villages from an input dict are NOT found in villages.json.

Usage:
    python check_villages.py <villages_json_path> [input_json_path]

If input_json_path is omitted, edit the INPUT_VILLAGES dict below directly.
"""

import re
import json
import sys


# -- 1. Paste your villages to check here (or pass a JSON file as 2nd arg) --

INPUT_VILLAGES = {
    # 1. Nabatieh District
    "عدشيت": {"en": "Adchit", "dist": "النبطية"},
    "عين قانا": {"en": "Ain Qana", "dist": "النبطية"},
    "أنصار": {"en": "Ansar", "dist": "النبطية"},
    "عربصاليم": {"en": "Arabsalim", "dist": "النبطية"},
    "أرنون": {"en": "Arnoun", "dist": "النبطية"},
    "بريقع": {"en": "Braiqaa", "dist": "النبطية"},
    "الشرقية": {"en": "Charkiyeh", "dist": "النبطية"},
    "شوكين": {"en": "Choukine", "dist": "النبطية"},
    "دير الزهراني": {"en": "Deir el Zahrani", "dist": "النبطية"},
    "الدوير": {"en": "Doueir", "dist": "النبطية"},
    "عبا": {"en": "Ebba", "dist": "النبطية"},
    "حبوش": {"en": "Habbouch", "dist": "النبطية"},
    "حاروف": {"en": "Harouf", "dist": "النبطية"},
    "حومين الفوقا": {"en": "Houmin el Fawqa", "dist": "النبطية"},
    "حومين التحتا": {"en": "Houmin el Tahta", "dist": "النبطية"},
    "جرجوع": {"en": "Jarjouh", "dist": "النبطية"},
    "جباع - عين بوسوار": {"en": "Jbaa - Ain Bouswar", "dist": "النبطية"},
    "جبشيت": {"en": "Jebchit", "dist": "النبطية"},
    "قعقعية الجسر": {"en": "Kaakaiyet el Jisr", "dist": "النبطية"},
    "كفرفيلا": {"en": "Kfarfila", "dist": "النبطية"},
    "كفررمان": {"en": "Kfarremane", "dist": "النبطية"},
    "كفرصير": {"en": "Kfarsir", "dist": "النبطية"},
    "كفرتبنيت": {"en": "Kfartebnit", "dist": "النبطية"},
    "الكفور": {"en": "Kfour", "dist": "النبطية"},
    "القصيبة": {"en": "Kossaybeh", "dist": "النبطية"},
    "ميفدون": {"en": "Mayfadoun", "dist": "النبطية"},
    "النبطية الفوقا": {"en": "Nabatieh el Fawka", "dist": "النبطية"},
    "النبطية التحتا": {"en": "Nabatieh el Tahta", "dist": "النبطية"},
    "النميرية": {"en": "Namiriyeh", "dist": "النبطية"},
    "رومين": {"en": "Roumine", "dist": "النبطية"},
    "صربا": {"en": "Sarba", "dist": "النبطية"},
    "سيناي": {"en": "Siney", "dist": "النبطية"},
    "صير الغربية": {"en": "Sir el Gharbiyeh", "dist": "النبطية"},
    "يحمر الشقيف": {"en": "Yehmor el Chekif", "dist": "النبطية"},
    "زوطر الشرقية": {"en": "Zawtar el Charkiyeh", "dist": "النبطية"},
    "زوطر الغربية": {"en": "Zawtar el Gharbiyeh", "dist": "النبطية"},
    "زبدين": {"en": "Zebdine", "dist": "النبطية"},
    "زفتا": {"en": "Zefta", "dist": "النبطية"},
    # 2. Bint Jbeil District
    "عين إبل": {"en": "Ain Ebel", "dist": "بنت جبيل"},
    "عيناتا": {"en": "Aynata", "dist": "بنت جبيل"},
    "عيتا الشعب": {"en": "Aita el Chaeb", "dist": "بنت جبيل"},
    "عيتا الجبل": {"en": "Aita al Jabal", "dist": "بنت جبيل"},
    "عيترون": {"en": "Aitaroun", "dist": "بنت جبيل"},
    "بيت ليف": {"en": "Beit Lif", "dist": "بنت جبيل"},
    "بيت ياحون": {"en": "Beit Yahoun", "dist": "بنت جبيل"},
    "بنت جبيل": {"en": "Bint Jbeil", "dist": "بنت جبيل"},
    "برج قلاويه": {"en": "Burj Qalaouiyah", "dist": "بنت جبيل"},
    "برعشيت": {"en": "Baraachit", "dist": "بنت جبيل"},
    "شقرا": {"en": "Chakra", "dist": "بنت جبيل"},
    "دبل": {"en": "Debel", "dist": "بنت جبيل"},
    "دير انطار": {"en": "Deir Antar", "dist": "بنت جبيل"},
    "فرون": {"en": "Froun", "dist": "بنت جبيل"},
    "الغندورية": {"en": "Ghandouriyeh", "dist": "بنت جبيل"},
    "حانين": {"en": "Hanine", "dist": "بنت جبيل"},
    "حاريص": {"en": "Hariss", "dist": "بنت جبيل"},
    "حداثا": {"en": "Haddatha", "dist": "بنت جبيل"},
    "جميجمة": {"en": "Jmeyjmeh", "dist": "بنت جبيل"},
    "كفرا": {"en": "Kafra", "dist": "بنت جبيل"},
    "قالويه": {"en": "Kalawayh", "dist": "بنت جبيل"},
    "القوزح": {"en": "Kawzah", "dist": "بنت جبيل"},
    "كفر دونين": {"en": "Kfardounine", "dist": "بنت جبيل"},
    "خربة سلم": {"en": "Khirbet Selm", "dist": "بنت جبيل"},
    "كونين": {"en": "Kounine", "dist": "بنت جبيل"},
    "مارون الراس": {"en": "Maroun el Ras", "dist": "بنت جبيل"},
    "رامية": {"en": "Ramyah", "dist": "بنت جبيل"},
    "رشاف": {"en": "Rchaf", "dist": "بنت جبيل"},
    "رميش": {"en": "Rmeich", "dist": "بنت جبيل"},
    "صربين": {"en": "Serbine", "dist": "بنت جبيل"},
    "السلطانية": {"en": "Sultanieh", "dist": "بنت جبيل"},
    "تبنين": {"en": "Tibnin", "dist": "بنت جبيل"},
    "الطيري": {"en": "Tiri", "dist": "بنت جبيل"},
    "يارون": {"en": "Yaroun", "dist": "بنت جبيل"},
    "ياطر": {"en": "Yater", "dist": "بنت جبيل"},
    # 3. Marjeyoun District
    "العديسة": {"en": "Adayseh", "dist": "مرجعيون"},
    "عدشيت القصير": {"en": "Adchit", "dist": "مرجعيون"},
    "بني حيان": {"en": "Bani Hayyan", "dist": "مرجعيون"},
    "بلاط": {"en": "Blatt", "dist": "مرجعيون"},
    "بليدا": {"en": "Blida", "dist": "مرجعيون"},
    "دبين": {"en": "Debbine", "dist": "مرجعيون"},
    "دير ميماس": {"en": "Deir Mimass", "dist": "مرجعيون"},
    "دير سريان": {"en": "Deir Syriane", "dist": "مرجعيون"},
    "إبل السقي": {"en": "Ebel el Saky", "dist": "مرجعيون"},
    "حولا": {"en": "Houla", "dist": "مرجعيون"},
    "جديدة مرجعيون": {"en": "Jdeidet Marjeyoun", "dist": "مرجعيون"},
    "قبريخا": {"en": "Kabrikha", "dist": "مرجعيون"},
    "القنطرة": {"en": "Kantara", "dist": "مرجعيون"},
    "كفركلا": {"en": "Kfarkila", "dist": "مرجعيون"},
    "الخيام": {"en": "Khiam", "dist": "مرجعيون"},
    "القليعة": {"en": "Klayaa", "dist": "مرجعيون"},
    "مجدل سلم": {"en": "Majdel Selm", "dist": "مرجعيون"},
    "مركبا": {"en": "Markaba", "dist": "مرجعيون"},
    "ميس الجبل": {"en": "Mays el Jabal", "dist": "مرجعيون"},
    "رب ثلاثين": {"en": "Rab Thalathine", "dist": "مرجعيون"},
    "الصوانة": {"en": "Sawaneh", "dist": "مرجعيون"},
    "طلوسة": {"en": "Tallousseh", "dist": "مرجعيون"},
    "الطيبة": {"en": "Taybeh", "dist": "مرجعيون"},
    "الوزاني": {"en": "Wazzani", "dist": "مرجعيون"},
    # 4. Hasbaya District
    "عين قنيا": {"en": "Ain Qenya", "dist": "حاصبيا"},
    "شبعا": {"en": "Chebaa", "dist": "حاصبيا"},
    "شويا": {"en": "Chwayya", "dist": "حاصبيا"},
    "الفرديس": {"en": "Fardiss", "dist": "حاصبيا"},
    "حاصبيا": {"en": "Hasbaya", "dist": "حاصبيا"},
    "الهبارية": {"en": "Hbariyeh", "dist": "حاصبيا"},
    "كوكبا": {"en": "Kawkaba", "dist": "حاصبيا"},
    "كفرشوبا": {"en": "Kfarchouba", "dist": "حاصبيا"},
    "كفر حمام": {"en": "Kfarhamam", "dist": "حاصبيا"},
    "الكفير": {"en": "Kfeir", "dist": "حاصبيا"},
    "خلوات": {"en": "Khalwat", "dist": "حاصبيا"},
    "ماري": {"en": "Mari", "dist": "حاصبيا"},
    "مرج الزهور": {"en": "Marj el Zhour", "dist": "حاصبيا"},
    "ميمس": {"en": "Mimass", "dist": "حاصبيا"},
    "راشيا الفخار": {"en": "Rachaya el Fokhar", "dist": "حاصبيا"},
}


# -- 2. Helpers --------------------------------------------------------------

def load_json_file(path):
    with open(path, "r", encoding="utf-8") as f:
        content = f.read()
    # Fix common issue: double closing braces  },  },
    content = re.sub(r"\},\s*\},", "},", content)
    return json.loads(content)


def normalize_ar(text):
    """Normalize Arabic text for fuzzy matching."""
    if not isinstance(text, str):
        return ""
    text = text.strip()
    text = re.sub(r"[\u064B-\u065F\u0670]", "", text)  # remove diacritics
    text = re.sub(r"[إأآا]", "ا", text)                 # unify alef
    text = re.sub(r"ة", "ه", text)                       # taa marbuta
    text = re.sub(r"ى", "ي", text)                       # alef maqsoura
    text = re.sub(r"ـ", "", text)                         # tatweel
    text = re.sub(r"\s+", " ", text).strip()
    return text


# -- 3. Main logic -----------------------------------------------------------

def check_villages(villages_json_path, input_villages):
    print(f"\nLoading JSON database: {villages_json_path}")
    db = load_json_file(villages_json_path)
    print(f"  -> {len(db):,} villages loaded\n")

    db_ar_exact = set(db.keys())
    db_ar_norm  = {normalize_ar(k): k for k in db.keys()}
    db_en_lower = {v.get("en", "").lower().strip(): k for k, v in db.items()}

    found     = []
    not_found = []

    for ar_key, info in input_villages.items():
        en   = info.get("en", "")
        dist = info.get("dist", "")

        if ar_key in db_ar_exact:
            match = db[ar_key]
            found.append((ar_key, en, dist, "exact Arabic", ar_key, match.get("en", "")))

        elif normalize_ar(ar_key) in db_ar_norm:
            orig_key = db_ar_norm[normalize_ar(ar_key)]
            match = db[orig_key]
            found.append((ar_key, en, dist, "normalized Arabic", orig_key, match.get("en", "")))

        elif en.lower().strip() in db_en_lower:
            orig_key = db_en_lower[en.lower().strip()]
            match = db[orig_key]
            found.append((ar_key, en, dist, "exact English", orig_key, match.get("en", "")))

        else:
            not_found.append((ar_key, en, dist))

    # Print results
    print("=" * 65)
    print(f"FOUND IN JSON  ({len(found)})")
    print("=" * 65)
    for ar, en, dist, method, db_ar, db_en in found:
        print(f"  [{method}]  dist: {dist}")
        print(f"    Input : '{ar}' / '{en}'")
        print(f"    In DB : '{db_ar}' / '{db_en}'")

    print()
    print("=" * 65)
    print(f"NOT FOUND IN JSON  ({len(not_found)})")
    print("=" * 65)
    if not_found:
        for ar, en, dist in not_found:
            print(f"  - '{ar}'  /  '{en}'  (dist: {dist})")
    else:
        print("  All villages were found!")

    print()
    print(f"Summary: {len(found)} found, {len(not_found)} missing out of {len(input_villages)} checked.")
    return not_found


# -- 4. Entry point ----------------------------------------------------------

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python check_villages.py <villages_json_path> [input_json_path]")
        sys.exit(1)

    villages_json_path = sys.argv[1]

    if len(sys.argv) >= 3:
        input_villages = load_json_file(sys.argv[2])
    else:
        input_villages = INPUT_VILLAGES

    check_villages(villages_json_path, input_villages)