"""
history.py — SHAHED Project
Simple, clean pipeline:
  1. Does message have an attack keyword?
  2. Find villages after "على" (primary target) + all other villages in message
  3. Verify each village exists in villages.json
  4. If ambiguous name, use region context to pick correct one
  5. Nothing found → skip
"""

import os
import re
import json
import asyncio
from datetime import timezone
from dotenv import load_dotenv
from telethon import TelegramClient
from supabase import create_client, Client

from ai.gemini import VILLAGES_DATA

load_dotenv()

# ── Credentials ───────────────────────────────────────────────────────────────
API_ID       = int(os.getenv("TELEGRAM_API_ID"))
API_HASH     = os.getenv("TELEGRAM_API_HASH")
PHONE        = os.getenv("TELEGRAM_PHONE")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

CHANNEL = "bintjbeilnews"

# ── Attack keywords ───────────────────────────────────────────────────────────
ATTACK_WORDS = [
    "غارة", "غارات", "قصف", "استهداف", "استهدفت", "استهدف",
    "ضربة", "ضربات", "مسيّرة", "مسيرة", "اغتيال", "غارتين ",
    "قذائف", "قذيفة", "صواريخ", "صاروخ", "انفجار", "انفجارات",
]

# ── Words that mean Lebanon is SOURCE not target → discard ────────────────────
DISCARD_WORDS = [
    "صافرات الإنذار", "صفارات الإنذار", "إعلام إسرائيلي",
    "الجبهة الداخلية الإسرائيلية", "القبة الحديدية",
    "باتجاه إسرائيل", "نحو حيفا", "نحو تل أبيب", "نحو عسقلان",
    "إطلاق من لبنان", "أطلقت من لبنان",
    "تل أبيب", "حيفا", "أسدود", "عسقلان", "نتانيا", "بئر السبع",
]

# ── Channel signature noise ───────────────────────────────────────────────────
CHANNEL_SIGNATURES = [
    "بنت جبيل نيوز", "bintjbeilnews", "@bintjbeilnews",
    "قناة بنت جبيل", "قناة موقع بنت جبيل", "موقع بنت جبيل",
    "https://whatsapp.com", "https://t.me",
]

# ── Region context: keyword → list of matching gov/dist values ────────────────
REGION_CONTEXT = {
    "البقاع":     ["البقاع", "زحلة", "راشيا", "حاصبيا"],
    "بعلبك":      ["بعلبك", "الهرمل"],
    "الهرمل":     ["الهرمل"],
    "الجنوب":     ["الجنوب", "النبطية", "بنت جبيل", "صور", "مرجعيون"],
    "جنوب لبنان": ["الجنوب", "النبطية", "بنت جبيل", "صور", "مرجعيون"],
    "النبطية":    ["النبطية"],
    "صور":        ["صور"],
    "مرجعيون":    ["مرجعيون"],
    "صيدا":       ["صيدا"],
    "الشمال":     ["الشمال", "طرابلس", "عكار", "زغرتا", "البترون"],
    "عكار":       ["عكار"],
    "طرابلس":     ["طرابلس"],
    "بيروت":      ["بيروت"],
    "الضاحية":    ["بيروت"],
    "الجبل":      ["عاليه", "الشوف", "بعبدا", "المتن"],
}

# ── Region fallback coordinates ───────────────────────────────────────────────
REGION_ANCHORS = {
    "بعلبك":      {"village_ar": "بعلبك",      "village_en": "Baalbek",      "lat": 34.0042, "lng": 36.2097},
    "الهرمل":     {"village_ar": "الهرمل",     "village_en": "Hermel",        "lat": 34.3906, "lng": 36.3842},
    "البقاع":     {"village_ar": "البقاع",     "village_en": "Bekaa",         "lat": 33.8462, "lng": 35.9016},
    "صور":        {"village_ar": "صور",        "village_en": "Tyre",          "lat": 33.2704, "lng": 35.2038},
    "النبطية":    {"village_ar": "النبطية",    "village_en": "Nabatieh",      "lat": 33.3779, "lng": 35.4836},
    "صيدا":       {"village_ar": "صيدا",       "village_en": "Sidon",         "lat": 33.5631, "lng": 35.3711},
    "مرجعيون":    {"village_ar": "مرجعيون",    "village_en": "Marjayoun",     "lat": 33.3631, "lng": 35.5922},
    "طرابلس":     {"village_ar": "طرابلس",     "village_en": "Tripoli",       "lat": 34.4367, "lng": 35.8498},
    "بيروت":      {"village_ar": "بيروت",      "village_en": "Beirut",        "lat": 33.8886, "lng": 35.4955},
    "الضاحية":    {"village_ar": "الضاحية",    "village_en": "Dahiyeh",       "lat": 33.8500, "lng": 35.4900},
    "الجنوب":     {"village_ar": "جنوب لبنان", "village_en": "South Lebanon", "lat": 33.2700, "lng": 35.3500},
    "جنوب لبنان": {"village_ar": "جنوب لبنان", "village_en": "South Lebanon", "lat": 33.2700, "lng": 35.3500},
    "الشمال":     {"village_ar": "شمال لبنان", "village_en": "North Lebanon", "lat": 34.2000, "lng": 35.9000},
    "عكار":       {"village_ar": "عكار",       "village_en": "Akkar",         "lat": 34.5333, "lng": 36.1000},
    "النبي شيت":  {"village_ar": "النبي شيت",  "village_en": "Nabi Sheet",    "lat": 33.9792, "lng": 35.9697},
}

# ── Build village regex (longest match first, >3 chars only) ──────────────────
_sorted_villages = sorted(
    [v for v in VILLAGES_DATA.keys() if len(v) > 3],
    key=len, reverse=True
)
VILLAGE_REGEX = re.compile(
    r'(?<![ء-ي])(' + '|'.join(re.escape(v) for v in _sorted_villages) + r')(?![ء-ي])'
)
print(f"✓ Village regex ready — {len(_sorted_villages)} villages")


# ══════════════════════════════════════════════════════════════════════════════
# PIPELINE FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════

def clean_message(text: str) -> str:
    """Remove channel signature noise."""
    for sig in CHANNEL_SIGNATURES:
        text = text.replace(sig, "")
    return text.strip()


def has_attack_keyword(text: str) -> bool:
    """Step 1: Does message contain an attack word?"""
    return any(word in text for word in ATTACK_WORDS)


def should_discard(text: str) -> bool:
    """Discard if message is Israeli news or describes outgoing fire."""
    return any(word in text for word in DISCARD_WORDS)


def get_region_from_text(text: str) -> str | None:
    """Detect region keyword mentioned in message (e.g. 'البقاع', 'الجنوب')."""
    for keyword in REGION_CONTEXT:
        if keyword in text:
            return keyword
    return None


def resolve_village(village_ar: str, region_keyword: str | None) -> dict | None:
    """
    Verify village exists in VILLAGES_DATA.
    If multiple entries share a similar name, use region_keyword to pick the right one.
    Returns full village dict or None.
    """
    # Exact match
    if village_ar in VILLAGES_DATA:
        return {"village_ar": village_ar, **VILLAGES_DATA[village_ar]}

    # Normalized match (strip ال prefix for comparison)
    def norm(s): return s[2:] if s.startswith("ال") else s

    candidates = [
        (k, v) for k, v in VILLAGES_DATA.items()
        if norm(k) == norm(village_ar)
    ]

    if not candidates:
        return None

    if len(candidates) == 1:
        k, v = candidates[0]
        return {"village_ar": k, **v}

    # Multiple candidates — use region context to pick correct one
    if region_keyword and region_keyword in REGION_CONTEXT:
        valid_govs = REGION_CONTEXT[region_keyword]
        for k, v in candidates:
            gov  = v.get("gov", "")
            dist = v.get("dist", "")
            if any(g in gov or g in dist for g in valid_govs):
                return {"village_ar": k, **v}

    # Fallback: return first candidate
    k, v = candidates[0]
    return {"village_ar": k, **v}


def extract_locations(text: str) -> list[dict]:
    """
    Step 2+3: Find all villages in message.

    Strategy:
    - Primary: look for villages in the window AFTER "على" (the attack target)
    - Secondary: scan full message for any other mentioned villages
    - All results verified against VILLAGES_DATA
    - Ambiguous names resolved using region context
    """
    region_keyword = get_region_from_text(text)
    found_names    = []

    # ── Primary: villages mentioned right after "على" ─────────────────────────
    # e.g. "غارة على عيتا الشعب" or "غارات على كونين والطيري وكوثرية الرز"
    for على_match in re.finditer(r'على\s+', text):
        window = text[على_match.end(): على_match.end() + 120]
        for v_match in VILLAGE_REGEX.finditer(window):
            found_names.append(v_match.group(1))

    # ── Secondary: all other villages anywhere in message ────────────────────
    for v_match in VILLAGE_REGEX.finditer(text):
        found_names.append(v_match.group(1))

    # Deduplicate preserving order
    seen = set()
    unique_names = []
    for name in found_names:
        if name not in seen:
            seen.add(name)
            unique_names.append(name)

    # Verify each against dataset
    results = []
    for name in unique_names:
        resolved = resolve_village(name, region_keyword)
        if resolved and resolved.get("lat") and resolved.get("lng"):
            results.append(resolved)
        elif resolved is None:
            print(f"    ~ '{name}' not in villages.json — skipped")

    return results


# ── Supabase helpers ──────────────────────────────────────────────────────────
def get_supabase() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def is_already_processed(supabase: Client, msg_id: int) -> bool:
    try:
        result = supabase.table("processed_messages").select("msg_id").eq("msg_id", msg_id).execute()
        return len(result.data) > 0
    except:
        return False


def mark_as_processed(supabase: Client, msg_id: int):
    try:
        supabase.table("processed_messages").insert({"msg_id": msg_id}).execute()
    except:
        pass


def save_attack(supabase: Client, loc: dict, original_msg: str, msg_date) -> bool:
    try:
        existing = (
            supabase.table("attacks")
            .select("id, attack_count")
            .eq("village_ar", loc["village_ar"])
            .execute()
        )
        if existing.data:
            new_count = existing.data[0]["attack_count"] + 1
            supabase.table("attacks").update({
                "attack_count": new_count,
                "original_msg": original_msg[:1000],
                "msg_date":     msg_date.isoformat() if msg_date else None,
            }).eq("id", existing.data[0]["id"]).execute()
            print(f"    ↑ {loc['village_ar']} → {new_count} attacks")
        else:
            supabase.table("attacks").insert({
                "village_ar":   loc["village_ar"],
                "village_en":   loc.get("en", ""),
                "lat":          loc.get("lat"),
                "lng":          loc.get("lng"),
                "attack_count": 1,
                "original_msg": original_msg[:1000],
                "msg_date":     msg_date.isoformat() if msg_date else None,
                "source":       "bintjbeilnews",
            }).execute()
            print(f"    ✓ NEW: {loc['village_ar']} ({loc.get('en','')}) [{loc.get('gov','')}]")
        return True
    except Exception as e:
        print(f"    ✗ Supabase error: {e}")
        return False


# ── Main scraper ──────────────────────────────────────────────────────────────
async def scrape_history():
    supabase = get_supabase()

    print("=" * 60)
    print("SHAHED — Historical Scrape")
    print(f"Channel : @{CHANNEL}")
    print("=" * 60)
    print("1. Attack keyword check")
    print("2. Discard Israeli/outgoing messages")
    print('3. Find villages after "على" + all others in message')
    print("4. Verify each against villages.json")
    print("5. Use region context for ambiguous names")
    print("=" * 60 + "\n")

    async with TelegramClient("shahed_session", API_ID, API_HASH) as client:
        await client.start(phone=PHONE)
        print("✓ Telegram connected\n")

        total        = 0
        skipped_seen = 0
        skipped_kw   = 0
        skipped_disc = 0
        skipped_none = 0
        saved        = 0

        async for message in client.iter_messages(CHANNEL, limit=None):
            if not message.text or not message.text.strip():
                continue

            total   += 1
            msg_id   = message.id
            msg_date = message.date.replace(tzinfo=timezone.utc) if message.date else None
            date_str = msg_date.strftime("%Y-%m-%d") if msg_date else "unknown"
            msg_text = clean_message(message.text.strip())

            print(f"[{total}] {date_str} | id={msg_id} | {msg_text[:70]}...")

            # Already processed?
            if is_already_processed(supabase, msg_id):
                skipped_seen += 1
                print(f"  → Already processed.")
                continue

            # Step 1: Attack keyword
            if not has_attack_keyword(msg_text):
                skipped_kw += 1
                print(f"  → No attack keyword.")
                mark_as_processed(supabase, msg_id)
                continue

            # Step 2: Discard Israeli/outgoing
            if should_discard(msg_text):
                skipped_disc += 1
                print(f"  → Discarded (Israeli/outgoing context).")
                mark_as_processed(supabase, msg_id)
                continue

            # Steps 3-5: Extract locations
            locations = extract_locations(msg_text)

            if not locations:
                # Region fallback
                region_kw = get_region_from_text(msg_text)
                if region_kw and region_kw in REGION_ANCHORS:
                    r = REGION_ANCHORS[region_kw]
                    print(f"  → Region fallback: {r['village_ar']}")
                    save_attack(supabase, r, msg_text, msg_date)
                    saved += 1
                else:
                    skipped_none += 1
                    print(f"  → No valid location found.")
                mark_as_processed(supabase, msg_id)
                continue

            print(f"  → {len(locations)} location(s): {[l['village_ar'] for l in locations]}")
            for loc in locations:
                save_attack(supabase, loc, msg_text, msg_date)
                saved += 1

            mark_as_processed(supabase, msg_id)

        print("\n" + "=" * 60)
        print("SCRAPE COMPLETE")
        print(f"  Total messages            : {total}")
        print(f"  Skipped (seen before)     : {skipped_seen}")
        print(f"  Skipped (no keyword)      : {skipped_kw}")
        print(f"  Discarded (Israeli/out)   : {skipped_disc}")
        print(f"  Discarded (no location)   : {skipped_none}")
        print(f"  Entries saved             : {saved}")
        print("=" * 60)


if __name__ == "__main__":
    asyncio.run(scrape_history())