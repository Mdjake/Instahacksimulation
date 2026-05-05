"""
🚀 SUPER-SMART FastAPI + SQLiteCloud API v3.0.0
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Features:
  - EASY TO REMEMBER endpoints (/search, /find, /filter)
  - COMBINE ANY FILTERS with & (city, income, name, email, mobile)
  - SUPER FAST in-memory caching + filtering
  - SMART search that just works
  - Clean, simple URL patterns
"""

import os
import re
import time
import logging
import hashlib
from datetime import datetime
from typing import Optional, Any, List
from contextlib import asynccontextmanager
from collections import defaultdict

import sqlitecloud
from fastapi import FastAPI, HTTPException, Query, Request, Depends, Header
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, field_validator


# ─────────────────────────────────────────────
# Structured Logging
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("api")


# ─────────────────────────────────────────────
# Settings
# ─────────────────────────────────────────────
class Settings:
    CONN_STR: str        = os.getenv("SQLITE_CLOUD_CONN_STR", "")
    API_KEY: str         = os.getenv("API_KEY", "dev-secret-key")
    CACHE_TTL: int       = int(os.getenv("CACHE_TTL_SECONDS", "60"))
    RATE_LIMIT: int      = int(os.getenv("RATE_LIMIT_PER_MIN", "30"))
    APP_ENV: str         = os.getenv("APP_ENV", "development")
    POOL_SIZE: int       = int(os.getenv("POOL_SIZE", "5"))
    TRUSTED_HOSTS: list  = os.getenv("TRUSTED_HOSTS", "localhost,127.0.0.1").split(",")
    VERSION: str         = "3.0.0"


settings = Settings()


# ─────────────────────────────────────────────
# TTL Cache
# ─────────────────────────────────────────────
class TTLCache:
    def __init__(self, ttl: int = 60):
        self._store: dict[str, tuple[Any, float]] = {}
        self.ttl = ttl
        self.hits = 0
        self.misses = 0

    def _make_key(self, *args) -> str:
        raw = ":".join(str(a) for a in args)
        return hashlib.md5(raw.encode()).hexdigest()

    def get(self, *args) -> Optional[Any]:
        key = self._make_key(*args)
        if key in self._store:
            value, expires_at = self._store[key]
            if time.time() < expires_at:
                self.hits += 1
                return value
            del self._store[key]
        self.misses += 1
        return None

    def set(self, value: Any, *args):
        key = self._make_key(*args)
        self._store[key] = (value, time.time() + self.ttl)

    def invalidate(self, *args):
        key = self._make_key(*args)
        self._store.pop(key, None)

    def clear_all(self):
        self._store.clear()
        logger.info("Cache fully cleared.")

    def stats(self) -> dict:
        total = self.hits + self.misses
        return {
            "entries": len(self._store),
            "hits": self.hits,
            "misses": self.misses,
            "hit_rate": f"{(self.hits / total * 100):.1f}%" if total > 0 else "0%",
        }


cache = TTLCache(ttl=settings.CACHE_TTL)


# ─────────────────────────────────────────────
# Connection Pool
# ─────────────────────────────────────────────
class ConnectionPool:
    def __init__(self, conn_str: str, size: int = 5):
        self._conn_str = conn_str
        self._pool: list[sqlitecloud.Connection] = []
        self._size = size
        self._index = 0
        self._query_count = 0

    def init(self):
        if not self._conn_str:
            logger.warning("SQLITE_CLOUD_CONN_STR not set — pool not initialized.")
            return
        for i in range(self._size):
            try:
                conn = sqlitecloud.connect(self._conn_str)
                self._pool.append(conn)
                logger.info(f"Pool connection {i + 1}/{self._size} established.")
            except Exception as e:
                logger.error(f"Pool init error (conn {i + 1}): {e}")
        logger.info(f"Connection pool ready: {len(self._pool)}/{self._size} connections.")

    def acquire(self) -> sqlitecloud.Connection:
        if not self._pool:
            raise RuntimeError("Connection pool is empty. Check SQLITE_CLOUD_CONN_STR.")
        conn = self._pool[self._index % len(self._pool)]
        self._index += 1
        self._query_count += 1
        return conn

    def close_all(self):
        for conn in self._pool:
            try:
                conn.close()
            except Exception:
                pass
        self._pool.clear()
        logger.info("All pool connections closed.")

    def stats(self) -> dict:
        return {
            "pool_size": len(self._pool),
            "target_size": self._size,
            "total_queries_served": self._query_count,
        }


pool = ConnectionPool(settings.CONN_STR, size=settings.POOL_SIZE)


# ─────────────────────────────────────────────
# Rate Limiter
# ─────────────────────────────────────────────
class RateLimiter:
    def __init__(self, limit: int = 30, window: int = 60):
        self._requests: dict[str, list[float]] = defaultdict(list)
        self.limit = limit
        self.window = window

    def is_allowed(self, identifier: str) -> tuple[bool, int]:
        now = time.time()
        cutoff = now - self.window
        self._requests[identifier] = [t for t in self._requests[identifier] if t > cutoff]
        remaining = self.limit - len(self._requests[identifier])
        if remaining <= 0:
            return False, 0
        self._requests[identifier].append(now)
        return True, remaining - 1


rate_limiter = RateLimiter(limit=settings.RATE_LIMIT)


# ─────────────────────────────────────────────
# Validation
# ─────────────────────────────────────────────
MOBILE_REGEX = re.compile(r"^\+?[0-9\s\-]{7,15}$")


class TargetData(BaseModel):
    name: str
    mobile: str
    email: str
    city: str
    income: Optional[float] = None
    docs: Optional[str] = None

    @field_validator("mobile")
    @classmethod
    def validate_mobile(cls, v: str) -> str:
        if not MOBILE_REGEX.match(v.strip()):
            raise ValueError("Invalid mobile number format.")
        return v.strip()


# ─────────────────────────────────────────────
# Lifespan
# ─────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(f"🚀 Starting SUPER-SMART API v{settings.VERSION} [{settings.APP_ENV}]")
    pool.init()
    yield
    logger.info("🛑 Shutting down — draining connection pool.")
    pool.close_all()


# ─────────────────────────────────────────────
# App Instance
# ─────────────────────────────────────────────
app = FastAPI(
    title="🎯 SUPER-SMART Target Lookup API",
    description="Easy to use - combine ANY filters with &",
    version=settings.VERSION,
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "DELETE"],
    allow_headers=["*"],
)


# ─────────────────────────────────────────────
# Dependencies
# ─────────────────────────────────────────────
async def require_api_key(
    x_api_key: Optional[str] = Header(None, alias="X-API-Key"),
    api_key: Optional[str] = Query(None),
):
    key = x_api_key or api_key
    if not key or key != settings.API_KEY:
        logger.warning("Rejected request — invalid API key.")
        raise HTTPException(status_code=401, detail="Invalid or missing API key.")
    return key


async def check_rate_limit(request: Request) -> int:
    client_ip = request.client.host if request.client else "unknown"
    allowed, remaining = rate_limiter.is_allowed(client_ip)
    if not allowed:
        logger.warning(f"Rate limit exceeded for IP: {client_ip}")
        raise HTTPException(
            status_code=429,
            detail="Rate limit exceeded. Try again in a minute.",
            headers={"Retry-After": "60"},
        )
    return remaining


@app.middleware("http")
async def log_requests(request: Request, call_next):
    start = time.perf_counter()
    response = await call_next(request)
    elapsed_ms = (time.perf_counter() - start) * 1000
    logger.info(
        f"{request.method} {request.url.path} → {response.status_code} "
        f"({elapsed_ms:.1f}ms) | IP: {getattr(request.client, 'host', 'unknown')}"
    )
    response.headers["X-Response-Time"] = f"{elapsed_ms:.1f}ms"
    response.headers["X-API-Version"] = settings.VERSION
    return response


# ─────────────────────────────────────────────
# DB Helper
# ─────────────────────────────────────────────
def db_query(sql: str, params: tuple = ()) -> list[dict]:
    conn = pool.acquire()
    try:
        cursor = conn.execute(sql, params)
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"SQL Error | Query: {sql!r} | Params: {params} | {e}")
        raise


def get_all_targets() -> list[dict]:
    """Helper to get all targets with caching"""
    cached = cache.get("all_targets")
    if cached is not None:
        return cached
    rows = db_query("SELECT * FROM targets")
    cache.set(rows, "all_targets")
    return rows


# ─────────────────────────────────────────────
# 🎯 SMART SEARCH ENGINE - ONE ENDPOINT TO RULE THEM ALL
# ─────────────────────────────────────────────

@app.get("/search", tags=["🔍 Smart Search"], summary="MASTER SEARCH - Combine ANY filters with &")
async def smart_search(
    # ALL FILTERS IN ONE PLACE - Use any combination!
    city: Optional[str] = Query(None, description="Filter by city name"),
    mobile: Optional[str] = Query(None, description="Filter by mobile number"),
    name: Optional[str] = Query(None, description="Filter by name (partial match)"),
    email: Optional[str] = Query(None, description="Filter by email (partial match)"),
    min_income: Optional[float] = Query(None, ge=0, description="Minimum income"),
    max_income: Optional[float] = Query(None, ge=0, description="Maximum income"),
    income_min: Optional[float] = Query(None, ge=0, description="Alias for min_income"),
    income_max: Optional[float] = Query(None, ge=0, description="Alias for max_income"),
    
    # Pagination
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(20, ge=1, le=100, description="Results per page"),
    
    # Sorting
    sort_by: str = Query("income", description="Sort by: income, name, city, mobile"),
    sort_desc: bool = Query(False, description="Sort descending"),
    
    # Auth
    _: str = Depends(require_api_key),
    remaining: int = Depends(check_rate_limit),
):
    """
    🎯 MASTER SEARCH ENDPOINT - Most flexible, most powerful!
    
    Combine ANY filters with & - Examples:
    - /search?city=Mumbai&min_income=50000
    - /search?name=John&max_income=100000
    - /search?city=Delhi&name=Singh&min_income=30000
    - /search?mobile=9876543210
    - /search?email=gmail.com&city=Bangalore
    """
    
    # Handle aliases
    if income_min is not None and min_income is None:
        min_income = income_min
    if income_max is not None and max_income is None:
        max_income = income_max
    
    offset = (page - 1) * page_size
    
    # Create cache key from ALL filters
    cache_key = f"smart:{city}:{mobile}:{name}:{email}:{min_income}:{max_income}:{page}:{page_size}:{sort_by}:{sort_desc}"
    
    cached = cache.get(cache_key)
    if cached is not None:
        logger.info(f"Cache HIT → smart search")
        return cached
    
    try:
        all_data = get_all_targets()
        filtered = all_data
        
        # Apply filters (all optional, any combination)
        if city:
            filtered = [row for row in filtered if row.get("city", "").lower() == city.lower()]
        
        if mobile:
            filtered = [row for row in filtered if mobile in row.get("mobile", "")]
        
        if name:
            filtered = [row for row in filtered if name.lower() in row.get("name", "").lower()]
        
        if email:
            filtered = [row for row in filtered if email.lower() in row.get("email", "").lower()]
        
        if min_income is not None:
            filtered = [row for row in filtered if row.get("income") is not None and row["income"] >= min_income]
        
        if max_income is not None:
            filtered = [row for row in filtered if row.get("income") is not None and row["income"] <= max_income]
        
        # Sorting
        if sort_by == "income":
            filtered.sort(key=lambda x: x.get("income", 0) or 0, reverse=sort_desc)
        elif sort_by == "name":
            filtered.sort(key=lambda x: x.get("name", ""), reverse=sort_desc)
        elif sort_by == "city":
            filtered.sort(key=lambda x: x.get("city", ""), reverse=sort_desc)
        elif sort_by == "mobile":
            filtered.sort(key=lambda x: x.get("mobile", ""), reverse=sort_desc)
        
        total = len(filtered)
        paginated = filtered[offset:offset + page_size]
        
        # Build active filters description
        active_filters = {}
        if city: active_filters["city"] = city
        if mobile: active_filters["mobile"] = mobile
        if name: active_filters["name"] = name
        if email: active_filters["email"] = email
        if min_income is not None: active_filters["min_income"] = min_income
        if max_income is not None: active_filters["max_income"] = max_income
        
        result = {
            "success": True,
            "message": f"Found {total} matching targets",
            "data": paginated,
            "page": page,
            "page_size": page_size,
            "total": total,
            "has_next": (offset + page_size) < total,
            "filters_applied": active_filters,
            "sort": {"by": sort_by, "descending": sort_desc}
        }
        
        cache.set(result, cache_key)
        return result
        
    except Exception as e:
        logger.error(f"Smart search error: {e}")
        raise HTTPException(status_code=500, detail=f"Search error: {e}")


# ─────────────────────────────────────────────
# 📍 SIMPLE ENDPOINTS - Easy to remember!
# ─────────────────────────────────────────────

@app.get("/by-city", tags=["📍 Simple Queries"], summary="Find by city")
async def by_city(
    city: str = Query(..., description="City name"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    _: str = Depends(require_api_key),
    remaining: int = Depends(check_rate_limit),
):
    """Simple endpoint - just add ?city=Mumbai"""
    return await smart_search(city=city, page=page, page_size=page_size)


@app.get("/by-income", tags=["📍 Simple Queries"], summary="Find by income range")
async def by_income(
    min: float = Query(0, ge=0, description="Minimum income"),
    max: float = Query(..., ge=0, description="Maximum income"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    _: str = Depends(require_api_key),
    remaining: int = Depends(check_rate_limit),
):
    """Simple endpoint - add ?min=50000&max=100000"""
    return await smart_search(min_income=min, max_income=max, page=page, page_size=page_size)


@app.get("/by-name", tags=["📍 Simple Queries"], summary="Find by name")
async def by_name(
    name: str = Query(..., description="Name (partial match)"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    _: str = Depends(require_api_key),
    remaining: int = Depends(check_rate_limit),
):
    """Simple endpoint - add ?name=John"""
    return await smart_search(name=name, page=page, page_size=page_size)


@app.get("/rich", tags=["📍 Simple Queries"], summary="Rich people (income >= amount)")
async def rich(
    min_income: float = Query(50000, ge=0, description="Minimum income"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    _: str = Depends(require_api_key),
    remaining: int = Depends(check_rate_limit),
):
    """Find all targets with income >= specified amount (default 50k)"""
    return await smart_search(min_income=min_income, sort_desc=True, page=page, page_size=page_size)


# ─────────────────────────────────────────────
# Basic Routes
# ─────────────────────────────────────────────

@app.get("/", tags=["ℹ️ Info"])
def root():
    return {
        "api": "🎯 SUPER-SMART Target Lookup API",
        "version": settings.VERSION,
        "environment": settings.APP_ENV,
        "docs": "/docs",
        "quick_start": {
            "search": "/search?city=Mumbai&min_income=50000&api_key=YOUR_KEY",
            "by_city": "/by-city?city=Delhi&api_key=YOUR_KEY",
            "by_income": "/by-income?min=30000&max=80000&api_key=YOUR_KEY",
            "rich": "/rich?min_income=100000&api_key=YOUR_KEY"
        }
    }


@app.get("/health", tags=["ℹ️ Info"])
def health_check():
    db_ok = False
    try:
        db_query("SELECT 1")
        db_ok = True
    except Exception:
        pass
    return {
        "status": "healthy" if db_ok else "degraded",
        "environment": settings.APP_ENV,
        "version": settings.VERSION,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "db_connected": db_ok,
        "cache_stats": cache.stats(),
        "pool_stats": pool.stats(),
    }


@app.get("/lookup", tags=["📞 Mobile Lookup"])
async def lookup_target(
    mobile: str = Query(..., min_length=7, max_length=15),
    _: str = Depends(require_api_key),
    remaining: int = Depends(check_rate_limit),
):
    """Quick lookup by mobile number"""
    if not MOBILE_REGEX.match(mobile.strip()):
        raise HTTPException(status_code=422, detail="Invalid mobile number format.")
    
    mobile = mobile.strip()
    cached = cache.get("lookup", mobile)
    if cached is not None:
        return JSONResponse(
            content={"success": True, "data": cached, "cached": True},
            headers={"X-Rate-Limit-Remaining": str(remaining)},
        )
    
    try:
        rows = db_query("SELECT * FROM targets WHERE mobile = ?", (mobile,))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
    
    if not rows:
        return JSONResponse(
            status_code=404,
            content={"success": False, "message": "Target not found."},
            headers={"X-Rate-Limit-Remaining": str(remaining)},
        )
    
    row = rows[0]
    cache.set(row, "lookup", mobile)
    return JSONResponse(
        content={"success": True, "data": row, "cached": False},
        headers={"X-Rate-Limit-Remaining": str(remaining)},
    )


# ─────────────────────────────────────────────
# Admin Routes
# ─────────────────────────────────────────────

@app.delete("/cache/mobile", tags=["🔧 Admin"])
async def invalidate_cache(
    mobile: str = Query(..., description="Mobile number to evict"),
    _: str = Depends(require_api_key),
):
    cache.invalidate("lookup", mobile.strip())
    cache.invalidate("all_targets")
    return {"success": True, "message": f"Cache cleared for {mobile}."}


@app.delete("/cache/all", tags=["🔧 Admin"])
async def flush_cache(_: str = Depends(require_api_key)):
    cache.clear_all()
    return {"success": True, "message": "Entire cache flushed."}


@app.get("/metrics", tags=["🔧 Admin"])
async def get_metrics(_: str = Depends(require_api_key)):
    return {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "version": settings.VERSION,
        "environment": settings.APP_ENV,
        "cache": cache.stats(),
        "pool": pool.stats(),
        "rate_limiter": {
            "limit_per_minute": rate_limiter.limit,
            "window_seconds": rate_limiter.window,
        },
    }
