from fastapi import FastAPI, HTTPException, Depends, Request
from fastapi.security import APIKeyHeader
from sqlalchemy import create_engine, text
import os

# Rate limiting
from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from fastapi.responses import JSONResponse

# Caching
import redis
import json

app = FastAPI(title="India Geo API", version="1.0")

engine = create_engine("postgresql://neondb_owner:npg_7sv0JmSfOikH@ep-shy-truth-an1aj3mt-pooler.c-6.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require")
# ================================
# ⚡ REDIS (CACHE)
# ================================
REDIS_URL = os.getenv("REDIS_URL")

if REDIS_URL:
    redis_client = redis.from_url(REDIS_URL)
else:
    redis_client = None

# ================================
# ⚡ REDIS (SAFE INIT)
# ================================
REDIS_URL = os.getenv("REDIS_URL")

if REDIS_URL:
    redis_client = redis.from_url(REDIS_URL)
else:
    redis_client = None  # fallback for local

# ================================
# 🔑 API KEY SECURITY
# ================================
api_key_header = APIKeyHeader(name="x-api-key")

def verify_api_key(api_key: str = Depends(api_key_header)):
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT 1 FROM api_keys WHERE key = :key"),
            {"key": api_key}
        ).fetchone()

        if not result:
            raise HTTPException(status_code=403, detail="Invalid API key")

# ================================
# 🚦 RATE LIMITING
# ================================
limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter

@app.exception_handler(RateLimitExceeded)
def rate_limit_handler(request: Request, exc):
    return JSONResponse(
        status_code=429,
        content={"detail": "Rate limit exceeded"}
    )

# ================================
# 🔍 AUTOCOMPLETE
# ================================
@app.get("/autocomplete")
@limiter.limit("100/minute")
def autocomplete(
    request: Request,
    q: str,
    state: str = None,
    district: str = None,
    api_key: str = Depends(verify_api_key)
):
    cache_key = f"search:{q}:{state}:{district}"

    # 🔹 CACHE CHECK
    if redis_client:
        cached = redis_client.get(cache_key)
        if cached:
            return json.loads(cached)

    base_query = """
        SELECT 
            v.name as village,
            sd.name as sub_district,
            d.name as district,
            s.name as state
        FROM villages v
        JOIN sub_districts sd ON v.sub_district_id = sd.id
        JOIN districts d ON sd.district_id = d.id
        JOIN states s ON d.state_id = s.id
        WHERE v.name ILIKE :q
    """

    params = {"q": f"%{q}%"}

    if state:
        base_query += " AND s.name ILIKE :state"
        params["state"] = f"%{state}%"

    if district:
        base_query += " AND d.name ILIKE :district"
        params["district"] = f"%{district}%"

    base_query += " LIMIT 20"

    with engine.connect() as conn:
        result = conn.execute(text(base_query), params)

        data = [
            {
                "village": row.village,
                "sub_district": row.sub_district,
                "district": row.district,
                "state": row.state,
                "formatted": f"{row.village.title()}, {row.sub_district.title()}, {row.district.title()}, {row.state.title()}, India"
            }
            for row in result
        ]

    # 🔹 SAVE CACHE
    if redis_client:
        redis_client.setex(cache_key, 3600, json.dumps(data))

    return data

# ================================
# 🌍 STATES
# ================================
@app.get("/states")
@limiter.limit("50/minute")
def get_states(
    request: Request,
    api_key: str = Depends(verify_api_key)
):
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT id, name FROM states ORDER BY name")
        )
        return [dict(row._mapping) for row in result]

# ================================
# 🏙 DISTRICTS
# ================================
@app.get("/districts")
@limiter.limit("50/minute")
def get_districts(
    request: Request,
    state_id: int,
    api_key: str = Depends(verify_api_key)
):
    with engine.connect() as conn:
        result = conn.execute(
            text("""
                SELECT id, name 
                FROM districts 
                WHERE state_id = :id
                ORDER BY name
            """),
            {"id": state_id}
        )
        return [dict(row._mapping) for row in result]

# ================================
# 🏘 SUBDISTRICTS
# ================================
@app.get("/subdistricts")
@limiter.limit("50/minute")
def get_subdistricts(
    request: Request,
    district_id: int,
    api_key: str = Depends(verify_api_key)
):
    with engine.connect() as conn:
        result = conn.execute(
            text("""
                SELECT id, name 
                FROM sub_districts 
                WHERE district_id = :id
                ORDER BY name
            """),
            {"id": district_id}
        )
        return [dict(row._mapping) for row in result]

# ================================
# ❤️ HEALTH CHECK
# ================================
@app.get("/")
def root():
    return {"message": "India Geo API is running 🚀"}