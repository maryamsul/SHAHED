"""
history.py — SHAHED Project
Crystal clear 3-step logic:
  Step 1: Does message contain attack keywords?
  Step 2: Does it mention a village that exists in villages.json?
  Step 3: Was this message already saved? If yes, skip it.
Safe to restart anytime — never double-counts.
"""

import os
import asyncio
from datetime import timezone
from dotenv import load_dotenv
from telethon import TelegramClient
from supabase import create_client, Client

from ai.gemini import lookup_village

load_dotenv()

# ── Credentials ───────────────────────────────────────────────────────────────
API_ID       = int(os.getenv("TELEGRAM_API_ID"))
API_HASH     = os.getenv("TELEGRAM_API_HASH")
PHONE        = os.getenv("TELEGRAM_PHONE")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

CHANNEL = "bintjbeilnews"

# ── Step 1: Attack keywords ───────────────────────────────────────────────────
# Message MUST contain at least one of these to be considered an attack
ATTACK_WORDS = [
    "غارة",
    "قصف",
    "استهداف",
    "مسيّرة",
    "مسيرة",
    "سلسلة الغارات",
    "غارة استهدفت",
]

# ── Channel signature noise — strip before checking ───────────────────────────
# بنت جبيل appears in every message as the channel name — remove it first
CHANNEL_SIGNATURES = [
    "بنت جبيل نيوز",
    "bintjbeilnews",
    "@bintjbeilnews",
    "قناة بنت جبيل",
    "قناة موقع بنت جبيل",
    "موقع بنت جبيل",
    "https://whatsapp.com",
    "https://t.me",
]


def clean_message(text: str) -> str:
    """Remove channel signature noise before analysis."""
    for sig in CHANNEL_SIGNATURES:
        text = text.replace(sig, "")
    return text.strip()


def has_attack_keyword(text: str) -> bool:
    """Step 1: Check if message contains any attack keyword."""
    return any(word in text for word in ATTACK_WORDS)


def find_village_in_message(text: str) -> tuple[str, dict] | tuple[None, None]:
    """
    Step 2: Find the FIRST village mentioned in the message by position.
    The attacked village always appears early — channel footers are at the bottom.
    Returns (village_ar, village_data) or (None, None).
    """
    from ai.gemini import VILLAGES_DATA

    earliest_pos    = len(text) + 1
    earliest_village = None
    earliest_data    = None

    for village_ar, data in VILLAGES_DATA.items():
        pos = text.find(village_ar)
        if pos != -1 and pos < earliest_pos:
            earliest_pos     = pos
            earliest_village = village_ar
            earliest_data    = data

    return earliest_village, earliest_data


# ── Supabase helpers ──────────────────────────────────────────────────────────
def get_supabase() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def is_already_processed(supabase: Client, msg_id: int) -> bool:
    """Step 3: Was this Telegram message ID already saved?"""
    try:
        result = (
            supabase.table("processed_messages")
            .select("msg_id")
            .eq("msg_id", msg_id)
            .execute()
        )
        return len(result.data) > 0
    except:
        return False


def mark_as_processed(supabase: Client, msg_id: int):
    """Mark message ID as done so we never process it again."""
    try:
        supabase.table("processed_messages").insert({"msg_id": msg_id}).execute()
    except:
        pass


def save_attack(supabase: Client, village_ar: str, village_data: dict,
                original_msg: str, msg_date, msg_id: int) -> bool:
    """
    Save or update attack in Supabase.
    - Village exists → increment attack_count
    - New village    → insert new row
    """
    try:
        existing = (
            supabase.table("attacks")
            .select("id, attack_count")
            .eq("village_ar", village_ar)
            .execute()
        )

        if existing.data:
            row_id    = existing.data[0]["id"]
            new_count = existing.data[0]["attack_count"] + 1
            supabase.table("attacks").update({
                "attack_count": new_count,
                "original_msg": original_msg[:1000],
                "msg_date":     msg_date.isoformat() if msg_date else None,
            }).eq("id", row_id).execute()
            print(f"  ↑ {village_ar} → {new_count} attacks total")
        else:
            supabase.table("attacks").insert({
                "village_en":   village_data.get("en", ""),
                "village_ar":   village_ar,
                "lat":          village_data.get("lat"),
                "lng":          village_data.get("lng"),
                "attack_count": 1,
                "original_msg": original_msg[:1000],
                "msg_date":     msg_date.isoformat() if msg_date else None,
                "source":       "bintjbeilnews",
            }).execute()
            print(f"  ✓ NEW village saved: {village_ar} ({village_data.get('en', '?')})")

        return True

    except Exception as e:
        print(f"  ✗ Supabase error: {e}")
        return False


# ── Main scraper ──────────────────────────────────────────────────────────────
async def scrape_history():
    supabase = get_supabase()

    print("=" * 60)
    print("SHAHED — Historical Scrape")
    print(f"Channel : @{CHANNEL}")
    print("=" * 60)
    print("Logic:")
    print("  1. Message has attack keyword?  → No  = skip")
    print("  2. Village found in .json?      → No  = skip")
    print("  3. msg_id already processed?    → Yes = skip")
    print("=" * 60 + "\n")

    async with TelegramClient("shahed_session", API_ID, API_HASH) as client:
        await client.start(phone=PHONE)
        print("✓ Telegram connected\n")

        total         = 0
        skipped_seen  = 0
        skipped_kw    = 0
        skipped_vil   = 0
        saved         = 0

        async for message in client.iter_messages(CHANNEL, limit=None):
            # Only process text messages
            if not message.text or not message.text.strip():
                continue

            total   += 1
            msg_id   = message.id
            msg_date = message.date.replace(tzinfo=timezone.utc) if message.date else None
            date_str = msg_date.strftime("%Y-%m-%d") if msg_date else "unknown"

            # Clean channel name noise from message
            msg_text = clean_message(message.text.strip())

            print(f"[{total}] {date_str} | id={msg_id} | {msg_text[:70]}...")

            # ── Step 3 first (cheapest check — just a DB lookup) ──────────────
            if is_already_processed(supabase, msg_id):
                skipped_seen += 1
                print(f"  → Already processed, skipping.")
                continue

            # ── Step 1: Attack keyword check ──────────────────────────────────
            if not has_attack_keyword(msg_text):
                skipped_kw += 1
                print(f"  → No attack keyword found, skipping.")
                mark_as_processed(supabase, msg_id)
                continue

            # ── Step 2: Village check ─────────────────────────────────────────
            village_ar, village_data = find_village_in_message(msg_text)

            if not village_ar:
                skipped_vil += 1
                print(f"  → Attack keyword found but no Lebanese village matched.")
                mark_as_processed(supabase, msg_id)
                continue

            # ── All checks passed — save to Supabase ──────────────────────────
            success = save_attack(
                supabase     = supabase,
                village_ar   = village_ar,
                village_data = village_data,
                original_msg = msg_text,
                msg_date     = msg_date,
                msg_id       = msg_id,
            )

            if success:
                saved += 1

            mark_as_processed(supabase, msg_id)

        # ── Summary ───────────────────────────────────────────────────────────
        print("\n" + "=" * 60)
        print("SCRAPE COMPLETE")
        print(f"  Total messages seen           : {total}")
        print(f"  Skipped (already processed)   : {skipped_seen}")
        print(f"  Skipped (no attack keyword)   : {skipped_kw}")
        print(f"  Skipped (village not in .json): {skipped_vil}")
        print(f"  Saved / updated in Supabase   : {saved}")
        print("=" * 60)


if __name__ == "__main__":
    asyncio.run(scrape_history())