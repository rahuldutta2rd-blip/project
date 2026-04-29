from fastapi import FastAPI, HTTPException, Depends
from fastapi.security import APIKeyHeader
from sqlalchemy import create_engine, text
import os

app = FastAPI(title="India Geo API", version="1.0")

engine = create_engine("postgresql://neondb_owner:npg_7sv0JmSfOikH@ep-shy-truth-an1aj3mt-pooler.c-6.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require")
# ================================
# 🔑 API KEY SECURITY
# ================================
api_key_header = APIKeyHeader(name="x-api-key")

def verify_api_key(api_key: str = Depends(api_key_header)):
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT * FROM api_keys WHERE key = :key"),
            {"key": api_key}
        ).fetchone()

        if not result:
            raise HTTPException(status_code=403, detail="Invalid API key")

# ================================
# 🔍 AUTOCOMPLETE API
# ================================
@app.get("/autocomplete")
def autocomplete(
    q: str,
    state: str = None,
    district: str = None,
    api_key: str = Depends(api_key_header)
):
    verify_api_key(api_key)

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

    return data


# ================================
# 🌍 GET STATES
# ================================
@app.get("/states")
def get_states(api_key: str = Depends(api_key_header)):
    verify_api_key(api_key)

    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT id, name FROM states ORDER BY name")
        )
        return [dict(row._mapping) for row in result]


# ================================
# 🏙 GET DISTRICTS
# ================================
@app.get("/districts")
def get_districts(state_id: int, api_key: str = Depends(api_key_header)):
    verify_api_key(api_key)

    query = text("""
        SELECT id, name 
        FROM districts 
        WHERE state_id = :id
        ORDER BY name
    """)

    with engine.connect() as conn:
        result = conn.execute(query, {"id": state_id})
        return [dict(row._mapping) for row in result]


# ================================
# 🏘 GET SUBDISTRICTS
# ================================
@app.get("/subdistricts")
def get_subdistricts(district_id: int, api_key: str = Depends(api_key_header)):
    verify_api_key(api_key)

    query = text("""
        SELECT id, name 
        FROM sub_districts 
        WHERE district_id = :id
        ORDER BY name
    """)

    with engine.connect() as conn:
        result = conn.execute(query, {"id": district_id})
        return [dict(row._mapping) for row in result]


# ================================
# ❤️ HEALTH CHECK
# ================================
@app.get("/")
def root():
    return {"message": "India Geo API is running 🚀"}