"""
live.py — SHAHED Project
Real-time Telegram listener.

Flow:
  1. New message arrives
  2. Pre-filter: length + discard words + attack keywords (no API)
  3. Groq (Llama 3) analyzes message → extracts Lebanese village names
  4. Each name compared against villages.json → get coordinates
  5. Found in villages.json → save to Supabase (upsert)
  6. Not found in villages.json → log warning, skip
  7. No village found at all → try region fallback from REGION_ANCHORS
"""

import os
import re
import json
import asyncio
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from supabase import create_client, Client
from groq import Groq

from ai.gemini import VILLAGES_DATA

load_dotenv()

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level  = logging.INFO,
    format = "%(asctime)s [%(levelname)s] %(message)s",
    datefmt= "%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("shahed-live")

# ── Credentials ───────────────────────────────────────────────────────────────
API_ID           = int(os.getenv("TELEGRAM_API_ID"))
API_HASH         = os.getenv("TELEGRAM_API_HASH")
TELEGRAM_SESSION = os.getenv("TELEGRAM_SESSION", "")
SUPABASE_URL     = os.getenv("SUPABASE_URL")
SUPABASE_KEY     = os.getenv("SUPABASE_KEY")
GROQ_KEY         = os.getenv("GROQ_API_KEY")

CHANNEL = "bintjbeilnews"

# ── Clients ───────────────────────────────────────────────────────────────────
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
groq_client      = Groq(api_key=GROQ_KEY)
MODEL            = "llama-3.1-8b-instant"

# ── Pre-filter: attack keywords ───────────────────────────────────────────────
ATTACK_WORDS = [
    "غارة", "غارات", "قصف", "استهداف", "استهدفت", "استهدف",
    "يستهدف", "تستهدف", "استهدفت", "مستهدف",
    "ضربة", "ضربات", "مسيّرة", "مسيرة", "اغتيال", "غارتين",
    "قذائف", "قذيفة", "صواريخ", "صاروخ", "انفجار", "انفجارات",
    "حزام ناري", "قنابل", "تفجير", "دمّر", "دمر",
]

# ── Pre-filter: discard words ─────────────────────────────────────────────────
DISCARD_WORDS = [
    "صافرات الإنذار", "صفارات الإنذار", "إعلام إسرائيلي",
    "التلفزيون الإيراني", "إيران", "إيراني",
    "الجبهة الداخلية الإسرائيلية", "القبة الحديدية",
    "باتجاه إسرائيل", "نحو حيفا", "نحو تل أبيب", "نحو عسقلان",
    "إطلاق من لبنان", "أطلقت من لبنان",
    "إطلاق صواريخ", "رأس حربي",
    "تل أبيب", "حيفا", "أسدود", "عسقلان", "نتانيا", "بئر السبع",
    "اليمن", "الحوثي", "العراق", "سوريا",
]

# ── Channel signatures to strip ───────────────────────────────────────────────
CHANNEL_SIGNATURES = [
    "بنت جبيل نيوز", "bintjbeilnews", "@bintjbeilnews",
    "قناة بنت جبيل", "قناة موقع بنت جبيل", "موقع بنت جبيل",
    "https://whatsapp.com", "https://t.me",
]

# ── Region fallback anchors (all Lebanon) ─────────────────────────────────────
REGION_ANCHORS = {
    "بعلبك":      {"village_ar": "بعلبك",      "village_en": "Baalbek",       "lat": 34.0042, "lng": 36.2097},
    "الهرمل":     {"village_ar": "الهرمل",     "village_en": "Hermel",         "lat": 34.3906, "lng": 36.3842},
    "البقاع":     {"village_ar": "البقاع",     "village_en": "Bekaa",          "lat": 33.8462, "lng": 35.9016},
    "زحلة":       {"village_ar": "زحلة",       "village_en": "Zahle",          "lat": 33.8497, "lng": 35.9017},
    "صور":        {"village_ar": "صور",        "village_en": "Tyre",           "lat": 33.2704, "lng": 35.2038},
    "النبطية":    {"village_ar": "النبطية",    "village_en": "Nabatieh",       "lat": 33.3779, "lng": 35.4836},
    "صيدا":       {"village_ar": "صيدا",       "village_en": "Sidon",          "lat": 33.5631, "lng": 35.3711},
    "مرجعيون":    {"village_ar": "مرجعيون",    "village_en": "Marjayoun",      "lat": 33.3631, "lng": 35.5922},
    "طرابلس":     {"village_ar": "طرابلس",     "village_en": "Tripoli",        "lat": 34.4367, "lng": 35.8498},
    "بيروت":      {"village_ar": "بيروت",      "village_en": "Beirut",         "lat": 33.8886, "lng": 35.4955},
    "الضاحية":    {"village_ar": "الضاحية",    "village_en": "Dahiyeh",        "lat": 33.8500, "lng": 35.4900},
    "جنوب لبنان": {"village_ar": "جنوب لبنان", "village_en": "South Lebanon",  "lat": 33.2700, "lng": 35.3500},
    "الجنوب":     {"village_ar": "جنوب لبنان", "village_en": "South Lebanon",  "lat": 33.2700, "lng": 35.3500},
    "الشمال":     {"village_ar": "شمال لبنان", "village_en": "North Lebanon",  "lat": 34.2000, "lng": 35.9000},
    "النبي شيت":  {"village_ar": "النبي شيت",  "village_en": "Nabi Sheet",     "lat": 33.9792, "lng": 35.9697},
    "راشيا":      {"village_ar": "راشيا",      "village_en": "Rashaya",        "lat": 33.4997, "lng": 35.8414},
    "عكار":       {"village_ar": "عكار",       "village_en": "Akkar",          "lat": 34.5333, "lng": 36.1000},
}

# ── Groq system prompt ────────────────────────────────────────────────────────
VILLAGES_INDEX = json.dumps(
    {k: {"en": v.get("en", ""), "gov": v.get("gov", "")}
     for k, v in VILLAGES_DATA.items()},
    ensure_ascii=False
)[:6000]

SYSTEM_PROMPT = f"""You analyze Arabic Telegram messages about military events in Lebanon.

Your ONLY job: extract the Arabic names of Lebanese villages or regions that are the TARGET of an attack.

VERIFIED LEBANESE VILLAGES:
{VILLAGES_INDEX}

RULES:
1. Only return villages/regions inside Lebanon that are being ATTACKED.
2. Ignore Israeli cities (حيفا، تل أبيب، عسقلان، أسدود).
3. Ignore messages where Lebanon is the source of fire, not the target.
4. If multiple villages attacked in one message → return ALL of them.
5. Common words (صور، برج، عين، بيت) only valid if clearly a Lebanese village in context.
6. If no valid Lebanese attack location → return null.

RESPOND WITH JSON ARRAY ONLY. No explanation. No markdown.
Examples:
["عيتا الشعب"]
["كونين", "الطيري", "كوثرية الرز"]
null"""


# ══════════════════════════════════════════════════════════════════════════════
# STEP 1 — Pre-filter (zero API cost)
# ══════════════════════════════════════════════════════════════════════════════

def clean_message(text: str) -> str:
    for sig in CHANNEL_SIGNATURES:
        text = text.replace(sig, "")
    return text.strip()


def should_process(text: str) -> bool:
    if len(text) > 400:
        return False
    if any(word in text for word in DISCARD_WORDS):
        return False
    return any(word in text for word in ATTACK_WORDS)


# ══════════════════════════════════════════════════════════════════════════════
# STEP 2 — Groq extracts village names
# ══════════════════════════════════════════════════════════════════════════════

def ask_groq(message_text: str) -> list[str] | None:
    try:
        response = groq_client.chat.completions.create(
            model    = MODEL,
            messages = [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user",   "content": message_text},
            ],
            max_tokens  = 256,
            temperature = 0.1,
        )

        raw = response.choices[0].message.content.strip()
        raw = re.sub(r"```[a-z]*", "", raw).replace("```", "").strip()

        if raw.lower() == "null" or not raw:
            return None

        parsed = json.loads(raw)
        if isinstance(parsed, list) and parsed:
            return [n.strip() for n in parsed if isinstance(n, str) and n.strip()]

        return None

    except Exception as e:
        log.error(f"  ✗ Groq error: {e}")
        return None


# ══════════════════════════════════════════════════════════════════════════════
# STEP 3 — Compare against villages.json
# ══════════════════════════════════════════════════════════════════════════════

def lookup_in_villages_json(village_ar: str) -> dict | None:
    if village_ar in VILLAGES_DATA:
        d = VILLAGES_DATA[village_ar]
        return {"village_ar": village_ar, "village_en": d.get("en", ""), "lat": d["lat"], "lng": d["lng"]}

    def norm(s): return s[2:] if s.startswith("ال") else s
    norm_input = norm(village_ar)
    for key, d in VILLAGES_DATA.items():
        if norm(key) == norm_input:
            return {"village_ar": key, "village_en": d.get("en", ""), "lat": d["lat"], "lng": d["lng"]}

    return None


def lookup_in_region_anchors(village_ar: str) -> dict | None:
    if village_ar in REGION_ANCHORS:
        return REGION_ANCHORS[village_ar].copy()
    for keyword, rdata in REGION_ANCHORS.items():
        if keyword in village_ar or village_ar in keyword:
            return rdata.copy()
    return None


# ══════════════════════════════════════════════════════════════════════════════
# STEP 4 — Save to Supabase
# ══════════════════════════════════════════════════════════════════════════════

def save_attack(loc: dict, original_msg: str, msg_date) -> bool:
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
            log.info(f"  ↑ {loc['village_ar']} → {new_count} attacks")
        else:
            supabase.table("attacks").insert({
                "village_ar":   loc["village_ar"],
                "village_en":   loc.get("village_en", ""),
                "lat":          loc.get("lat"),
                "lng":          loc.get("lng"),
                "attack_count": 1,
                "original_msg": original_msg[:1000],
                "msg_date":     msg_date.isoformat() if msg_date else None,
                "source":       "bintjbeilnews",
            }).execute()
            log.info(f"  ✓ NEW: {loc['village_ar']} ({loc.get('village_en', '')})")
        return True
    except Exception as e:
        log.error(f"  ✗ Supabase error: {e}")
        return False


# ══════════════════════════════════════════════════════════════════════════════
# MAIN LISTENER
# ══════════════════════════════════════════════════════════════════════════════

async def main():
    log.info("=" * 55)
    log.info("SHAHED — Live Listener")
    log.info(f"Channel : @{CHANNEL}")
    log.info(f"Model   : {MODEL} via Groq (free)")
    log.info("=" * 55)

    # ── StringSession — no interactive login needed on Railway ────────────────
    session = StringSession(TELEGRAM_SESSION)

    async with TelegramClient(session, API_ID, API_HASH) as client:
        await client.start()
        await client.catch_up()

        entity = await client.get_entity(CHANNEL)
        log.info(f"✓ Connected to: {entity.title}")
        log.info("✓ Listening for new messages...\n")

        @client.on(events.NewMessage(chats=entity, func=lambda e: True))
        async def handler(event):
            if not event.message.text:
                return

            msg_text = clean_message(event.message.text.strip())
            msg_date = event.message.date.replace(tzinfo=timezone.utc) \
                       if event.message.date else datetime.now(timezone.utc)

            log.info(f"📨 {msg_text[:80]}...")

            if not should_process(msg_text):
                log.info("  → Filtered (no keyword / too long / Israeli context)")
                return

            log.info("  → Passes filter — asking Groq...")
            village_names = ask_groq(msg_text)

            if not village_names:
                log.warning("  ⚠ Groq found no Lebanese attack location.")
                return

            log.info(f"  → Groq extracted: {village_names}")

            saved_any = False
            for name in village_names:
                loc = lookup_in_villages_json(name)
                if loc:
                    log.info(f"  ✓ '{name}' found in villages.json → saving")
                    save_attack(loc, msg_text, msg_date)
                    saved_any = True
                    continue

                loc = lookup_in_region_anchors(name)
                if loc:
                    log.info(f"  ✓ '{name}' found in regions → saving as {loc['village_ar']}")
                    save_attack(loc, msg_text, msg_date)
                    saved_any = True
                    continue

                log.warning(f"  ⚠ '{name}' not in villages.json or regions — skipped")

            if not saved_any:
                log.warning("  ⚠ No location could be resolved to coordinates.")

        while True:
            await asyncio.sleep(60)
            log.info("⏳ Still listening...")


if __name__ == "__main__":
    asyncio.run(main())
