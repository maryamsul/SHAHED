"""
main.py — SHAHED Project
FastAPI backend serving attack and testimony data to the frontend.
"""

import os
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from supabase import create_client

load_dotenv()

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))


@app.get("/attacks")
def get_attacks():
    result = supabase.table("attacks").select("*").execute()
    return result.data


@app.get("/testimonies/{village_ar}")
def get_testimonies(village_ar: str):
    result = (
        supabase.table("testimonies")
        .select("*")
        .eq("village_ar", village_ar)
        .order("created_at", desc=True)
        .execute()
    )
    return result.data


@app.post("/testimonies")
def post_testimony(payload: dict):
    result = supabase.table("testimonies").insert({
        "village_ar": payload.get("village_ar"),
        "message":    payload.get("message"),
    }).execute()
    return result.data


@app.get("/stats")
def get_stats():
    attacks     = supabase.table("attacks").select("id", count="exact").execute()
    testimonies = supabase.table("testimonies").select("id", count="exact").execute()
    return {
        "total_attacks":     attacks.count,
        "total_testimonies": testimonies.count,
    }