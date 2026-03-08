"""
reprocess_missing.py — SHAHED Project
Finds villages that exist in villages.json but have NO entry in attacks table,
then re-scans ALL Telegram messages looking specifically for those missing villages.
Same strict pipeline as history.py — attack keywords, discard filter, على targeting.
"""

import os
import re
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

# ── Same attack/discard/signature config as history.py ───────────────────────
ATTACK_WORDS = [
    "غارة", "غارات", "قصف", "استهداف", "استهدفت", "استهدف",
    "ضربة", "ضربات", "مسيّرة", "مسيرة", "اغتيال", "غارتين",
    "قذائف", "قذيفة", "صواريخ", "صاروخ", "انفجار", "انفجارات",
]

DISCARD_WORDS = [
    "صافرات الإنذار", "صفارات الإنذار", "إعلام إسرائيلي",
    "الجبهة الداخلية الإسرائيلية", "القبة الحديدية",
    "باتجاه إسرائيل", "نحو حيفا", "نحو تل أبيب", "نحو عسقلان",
    "إطلاق من لبنان", "أطلقت من لبنان",
    "تل أبيب", "حيفا", "أسدود", "عسقلان", "نتانيا", "بئر السبع",
]

CHANNEL_SIGNATURES = [
    "بنت جبيل نيوز", "bintjbeilnews", "@bintjbeilnews",
    "قناة بنت جبيل", "قناة موقع بنت جبيل", "موقع بنت جبيل",
    "https://whatsapp.com", "https://t.me",
]

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


# ── جنوب لبنان constants ─────────────────────────────────────────────────────
SOUTH_LEBANON_TERMS = [
    "جنوب لبنان", "الجنوب اللبناني", "جنوب لبناني", "الجنوب",
]

SOUTH_LEBANON_ENTRY = {
    "village_ar": "جنوب لبنان",
    "village_en": "South Lebanon",
    "lat": 33.2700,
    "lng": 35.3500,
}


# ── Helpers (identical to history.py) ────────────────────────────────────────────
def clean_message(text: str) -> str:
    for sig in CHANNEL_SIGNATURES:
        text = text.replace(sig, "")
    return text.strip()


def has_attack_keyword(text: str) -> bool:
    return any(word in text for word in ATTACK_WORDS)


def should_discard(text: str) -> bool:
    if len(text) > 400:
        return True
    return any(word in text for word in DISCARD_WORDS)


def get_region_from_text(text: str) -> str | None:
    for keyword in REGION_CONTEXT:
        if keyword in text:
            return keyword
    return None


def resolve_village(village_ar: str, region_keyword: str | None) -> dict | None:
    if village_ar in VILLAGES_DATA:
        return {"village_ar": village_ar, **VILLAGES_DATA[village_ar]}

    def norm(s): return s[2:] if s.startswith("ال") else s

    candidates = [(k, v) for k, v in VILLAGES_DATA.items() if norm(k) == norm(village_ar)]

    if not candidates:
        return None
    if len(candidates) == 1:
        k, v = candidates[0]
        return {"village_ar": k, **v}

    if region_keyword and region_keyword in REGION_CONTEXT:
        valid_govs = REGION_CONTEXT[region_keyword]
        for k, v in candidates:
            if any(g in v.get("gov", "") or g in v.get("dist", "") for g in valid_govs):
                return {"village_ar": k, **v}

    k, v = candidates[0]
    return {"village_ar": k, **v}


def already_in_supabase(supabase: Client, village_ar: str) -> bool:
    """Check if village already has an entry in attacks table."""
    try:
        result = supabase.table("attacks").select("id").eq("village_ar", village_ar).execute()
        return len(result.data) > 0
    except:
        return False


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
            print(f"    ✓ NEW: {loc['village_ar']} ({loc.get('en', '')}) [{loc.get('gov', '')}]")
        return True
    except Exception as e:
        print(f"    ✗ Supabase error: {e}")
        return False


# ── Step 1: Find which villages are missing from attacks table ────────────────
def get_missing_villages(supabase: Client) -> dict:
    """
    Returns subset of VILLAGES_DATA whose village_ar names
    do NOT appear in the attacks table at all.
    """
    print("Fetching existing villages from attacks table...")
    existing = supabase.table("attacks").select("village_ar").execute()
    already_saved = {row["village_ar"] for row in existing.data}

    missing = {
        k: v for k, v in VILLAGES_DATA.items()
        if k not in already_saved
    }

    print(f"  villages.json total   : {len(VILLAGES_DATA)}")
    print(f"  already in attacks    : {len(already_saved)}")
    print(f"  missing (to scan for) : {len(missing)}")
    return missing


# ── Step 2: Build regex only for missing villages ─────────────────────────────
def build_missing_regex(missing_villages: dict) -> re.Pattern:
    sorted_names = sorted(
        [v for v in missing_villages.keys() if len(v) > 3],
        key=len, reverse=True
    )
    if not sorted_names:
        return None
    pattern = re.compile(
        r'(?<![ء-ي])(' + '|'.join(re.escape(v) for v in sorted_names) + r')(?![ء-ي])'
    )
    print(f"✓ Regex built for {len(sorted_names)} missing villages\n")
    return pattern


# ── Step 3: Extract missing villages from a message ──────────────────────────
def extract_missing_locations(text: str, missing_regex: re.Pattern,
                               missing_villages: dict,
                               supabase: Client = None) -> list[dict]:
    """
    Find missing villages in message.
    جنوب لبنان logic:
    - specific village found in missing_villages → use it
    - no specific village + جنوب لبنان mentioned + not in Supabase → use fallback
    - village already in Supabase → skip
    """
    region_keyword = get_region_from_text(text)
    found_names    = []
    mentions_south = any(term in text for term in SOUTH_LEBANON_TERMS)

    # Primary: villages after "على"
    for m in re.finditer(r'على\s+', text):
        window = text[m.end(): m.end() + 120]
        for v_match in missing_regex.finditer(window):
            found_names.append(v_match.group(1))

    # Secondary: anywhere in message
    for v_match in missing_regex.finditer(text):
        found_names.append(v_match.group(1))

    # Deduplicate
    seen, unique = set(), []
    for name in found_names:
        if name not in seen:
            seen.add(name)
            unique.append(name)

    results = []
    for name in unique:
        if name in SOUTH_LEBANON_TERMS or name == "جنوب لبنان":
            continue
        resolved = resolve_village(name, region_keyword)
        if not resolved or not resolved.get("lat") or not resolved.get("lng"):
            continue
        # Skip if already counted in Supabase
        if supabase and already_in_supabase(supabase, resolved["village_ar"]):
            print(f"    ~ '{resolved['village_ar']}' already in Supabase — skipped")
            continue
        results.append(resolved)

    # جنوب لبنان fallback: only if no specific village found
    if mentions_south and not results:
        if supabase and already_in_supabase(supabase, "جنوب لبنان"):
            print(f"    ~ جنوب لبنان already in Supabase — skipped")
        else:
            print(f"    → No specific village, counting as جنوب لبنان")
            results.append(SOUTH_LEBANON_ENTRY.copy())

    return results


# ── Main ──────────────────────────────────────────────────────────────────────
async def reprocess_missing():
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    print("=" * 60)
    print("SHAHED — Reprocess Missing Villages")
    print(f"Channel : @{CHANNEL}")
    print("=" * 60)
    print("Goal: find villages in villages.json not yet in attacks table")
    print("      re-scan ALL messages looking only for those villages")
    print("=" * 60 + "\n")

    # Step 1: find what's missing
    missing_villages = get_missing_villages(supabase)

    if not missing_villages:
        print("✓ No missing villages — attacks table is complete!")
        return

    # Step 2: build regex for missing only
    missing_regex = build_missing_regex(missing_villages)
    if not missing_regex:
        print("✗ No valid village names to search for (all <= 3 chars)")
        return

    # Step 3: scan all messages
    async with TelegramClient("shahed_reprocess_session", API_ID, API_HASH) as client:
        await client.start(phone=PHONE)
        print("✓ Telegram connected\n")

        total        = 0
        skipped_kw   = 0
        skipped_disc = 0
        no_missing   = 0
        saved        = 0

        async for message in client.iter_messages(CHANNEL, limit=None):
            if not message.text or not message.text.strip():
                continue

            total   += 1
            msg_id   = message.id
            msg_date = message.date.replace(tzinfo=timezone.utc) if message.date else None
            date_str = msg_date.strftime("%Y-%m-%d") if msg_date else "unknown"
            msg_text = clean_message(message.text.strip())

            # Same strict filters as history.py
            if not has_attack_keyword(msg_text):
                skipped_kw += 1
                continue

            if should_discard(msg_text):
                skipped_disc += 1
                continue

            # Only look for missing villages
            locations = extract_missing_locations(msg_text, missing_regex, missing_villages, supabase)

            if not locations:
                no_missing += 1
                continue

            print(f"[{total}] {date_str} | id={msg_id} | {msg_text[:60]}...")
            print(f"  → Found missing village(s): {[l['village_ar'] for l in locations]}")

            for loc in locations:
                save_attack(supabase, loc, msg_text, msg_date)
                saved += 1

                # Remove from missing set so we don't keep counting duplicates
                missing_villages.pop(loc["village_ar"], None)

            # Rebuild regex if missing set shrank significantly
            if saved % 20 == 0 and missing_villages:
                missing_regex = build_missing_regex(missing_villages)
                if not missing_regex:
                    print("✓ All missing villages found!")
                    break

        print("\n" + "=" * 60)
        print("REPROCESS COMPLETE")
        print(f"  Total messages scanned    : {total}")
        print(f"  Skipped (no keyword)      : {skipped_kw}")
        print(f"  Skipped (Israeli/out)     : {skipped_disc}")
        print(f"  No missing village found  : {no_missing}")
        print(f"  New entries saved         : {saved}")
        if missing_villages:
            print(f"  Still not found ({len(missing_villages)} villages) — not mentioned in channel")
        else:
            print(f"  All missing villages found ✓")
        print("=" * 60)


if __name__ == "__main__":
    asyncio.run(reprocess_missing())