import os
import json
from google import genai
from google.genai import types
from dotenv import load_dotenv

load_dotenv()

# ── Configure Gemini ──────────────────────────────────────────────────────────
client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))
MODEL  = "gemini-2.0-flash"

# ── Attack keywords (Arabic) ──────────────────────────────────────────────────
ATTACK_WORDS = [
    "غارة", "غارات",       # airstrike / airstrikes
    "قصف",                 # shelling / bombing
    "استهداف", "استُهدف",  # targeting / was targeted
    "هجوم", "هجمات",       # attack / attacks
    "قذيفة", "قذائف",      # shell / shells
    "صاروخ", "صواريخ",     # missile / missiles
    "انفجار", "انفجارات",  # explosion / explosions
    "طائرة مسيّرة", "مسيّرة", # drone
    "اغتيال",              # assassination
    "ضربة", "ضربات",       # strike / strikes
    "سقوط",                # falling (as in shells falling on)
]

# Village extraction hints — if message has attack word + village indicator
VILLAGE_INDICATORS = [
    "بلدة", "قرية", "مدينة", "بلدية",  # town, village, city, municipality
    "منطقة", "محيط", "في ",            # area, vicinity, in
]

# ── Load villages.json once at startup ───────────────────────────────────────
VILLAGES_FILE = os.path.join(os.path.dirname(__file__), "..", "villages.json")

try:
    with open(VILLAGES_FILE, "r", encoding="utf-8") as f:
        raw_data = json.load(f)
    
    VILLAGES_DATA = {k.strip(): v for k, v in raw_data.items()}
    print(f"✓ Loaded {len(VILLAGES_DATA)} villages from villages.json")
except Exception as e:
    print(f"✗ Error loading villages.json: {e}")
    VILLAGES_DATA = {}


def lookup_village(village_name: str) -> dict | None:
    """Looks up an Arabic village name in villages.json."""
    if not village_name or not VILLAGES_DATA:
        return None

    village_name = village_name.strip()

    # 1. Exact match
    if village_name in VILLAGES_DATA:
        return VILLAGES_DATA[village_name]

    # 2. Normalize 'ال' prefix and try partial match
    def normalize(s):
        return s[2:] if s.startswith("ال") else s

    normalized_name = normalize(village_name)

    for key, data in VILLAGES_DATA.items():
        normalized_key = normalize(key)
        if normalized_name in normalized_key or normalized_key in normalized_name:
            return data

    return None


def rule_based_check(message_text: str) -> bool:
    """
    Fast rule-based check: is this likely an attack message?
    Returns True if any attack keyword is found.
    No API call needed.
    """
    return any(word in message_text for word in ATTACK_WORDS)


def rule_based_village(message_text: str) -> str | None:
    """
    Try to extract village name directly from message text
    by checking if any known village name appears in the message.
    Returns the Arabic village name or None.
    """
    for village_ar in VILLAGES_DATA.keys():
        if village_ar in message_text:
            return village_ar
    return None


def analyze_message(message_text: str, message_date: str) -> dict:
    """
    Hybrid analysis:
    1. Rule-based check for attack keywords (instant, no API)
    2. Rule-based village name lookup (instant, no API)
    3. Only call Gemini if attack detected but village not found by rules

    Returns:
        {"is_attack": True,  "village_ar": "...", "description": "..."}
        {"is_attack": False, "village_ar": None}
    """
    # ── Step 1: Rule-based attack detection ──────────────────────────────────
    is_attack = rule_based_check(message_text)

    if not is_attack:
        # Not an attack — skip Gemini entirely
        return {"is_attack": False, "village_ar": None}

    # ── Step 2: Rule-based village extraction ────────────────────────────────
    village_ar = rule_based_village(message_text)

    if village_ar:
        # Attack + village found without Gemini
        print(f"  ✓ Rules matched: attack=True, village={village_ar} [NO API CALL]")
        return {
            "is_attack":   True,
            "village_ar":  village_ar,
            "description": "",
        }

    # ── Step 3: Gemini for complex cases (attack detected, village unclear) ──
    print(f"  → Rules: attack=True but village unclear — calling Gemini...")
    return _gemini_extract_village(message_text, message_date)


def _gemini_extract_village(message_text: str, message_date: str) -> dict:
    """
    Called only when rules confirm attack but can't find village name.
    Asks Gemini only for the village name — not whether it's an attack.
    """
    prompt = f"""
You are an expert at reading Arabic news reports about Lebanon.
This message is confirmed to describe a military attack. Your only job is to find the village name.

What is the exact Arabic name of the Lebanese town or village that was attacked?

Rules:
- Return ONLY the Arabic village name, nothing else.
- If you cannot find a clear village name, return the word: null
- Do not add explanation, punctuation, or quotes.

Message:
"{message_text}"
"""

    try:
        response = client.models.generate_content(
            model=MODEL,
            contents=prompt,
            config=types.GenerateContentConfig(temperature=0.1),
        )

        village_ar = response.text.strip()

        # Clean up
        if village_ar.lower() in ("null", "none", "", "لا يوجد"):
            return {"is_attack": True, "village_ar": None, "description": ""}

        # Strip quotes if Gemini added them
        village_ar = village_ar.strip('"\'')

        print(f"  ✓ Gemini extracted village: {village_ar}")
        return {
            "is_attack":   True,
            "village_ar":  village_ar,
            "description": "",
        }

    except Exception as e:
        print(f"  ✗ Gemini error: {e}")
        return {"is_attack": True, "village_ar": None}


# ── Quick test ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    tests = [
        "غارة معادية تستهدف بلدة الخيام",
        "قصف مدفعي على بنت جبيل",
        "الجيش اللبناني يُصدر بيانًا حول الوضع",
        "انفجار ضخم في منطقة صور",
        "صفارات الإنذار تدوي في تل أبيب",
    ]

    for msg in tests:
        print(f"\nMessage: {msg}")
        result = analyze_message(msg, "2024-10-25")
        print(f"Result : {json.dumps(result, ensure_ascii=False)}")
        if result.get("village_ar"):
            data = lookup_village(result["village_ar"])
            print(f"Coords : {data}")
