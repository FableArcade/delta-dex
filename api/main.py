"""FastAPI application for the Pokemon analytics pipeline."""

import sys
import traceback
from pathlib import Path
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from api.routers import health, leaderboard, sets, cards, sealed, model

FRONTEND_DIR = Path(__file__).resolve().parent.parent / "frontend"

app = FastAPI(
    title="PokeDelta API",
    description="Data-driven Pokemon TCG investment analytics — Delta Edition",
    version="2.0.0",
)


# Log unhandled exceptions to stderr + return the detail in the response body
# so the browser Network tab / curl can see what's going wrong. Development-only.
@app.exception_handler(Exception)
async def _unhandled_exception_handler(request: Request, exc: Exception):
    tb = traceback.format_exc()
    print(f"\n=== UNHANDLED EXCEPTION on {request.method} {request.url.path} ===\n{tb}\n",
          file=sys.stderr, flush=True)
    return JSONResponse(
        status_code=500,
        content={
            "error": type(exc).__name__,
            "detail": str(exc),
            "path": str(request.url.path),
        },
    )


# CORS - allow all origins for development
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount all routers under /api prefix
app.include_router(health.router, prefix="/api", tags=["health"])
app.include_router(leaderboard.router, prefix="/api", tags=["leaderboard"])
app.include_router(sets.router, prefix="/api", tags=["sets"])
app.include_router(cards.router, prefix="/api", tags=["cards"])
app.include_router(sealed.router, prefix="/api", tags=["sealed"])
app.include_router(model.router, prefix="/api", tags=["model"])


# Serve frontend static files
if FRONTEND_DIR.exists():
    from fastapi.responses import FileResponse

    @app.get("/")
    def serve_index():
        return FileResponse(FRONTEND_DIR / "index.html")

    @app.get("/sets.html")
    def serve_sets_grid():
        return FileResponse(FRONTEND_DIR / "sets.html")

    @app.get("/set.html")
    def serve_set_detail_direct():
        return FileResponse(FRONTEND_DIR / "set.html")

    @app.get("/sets/{set_code:path}")
    def serve_set_detail(set_code: str):
        return FileResponse(FRONTEND_DIR / "set.html")

    @app.get("/card.html")
    def serve_card_detail():
        return FileResponse(FRONTEND_DIR / "card.html")

    @app.get("/card_leaderboard.html")
    def serve_card_leaderboard():
        return FileResponse(FRONTEND_DIR / "card_leaderboard.html")

    @app.get("/sealed_leaderboard.html")
    def serve_sealed_leaderboard():
        return FileResponse(FRONTEND_DIR / "sealed_leaderboard.html")

    @app.get("/search.html")
    def serve_search():
        return FileResponse(FRONTEND_DIR / "search.html")

    @app.get("/calculator.html")
    def serve_calculator():
        return FileResponse(FRONTEND_DIR / "calculator.html")

    @app.get("/about.html")
    def serve_about():
        return FileResponse(FRONTEND_DIR / "about.html")

    @app.get("/report_card.html")
    def serve_report_card():
        return FileResponse(FRONTEND_DIR / "report_card.html")

    app.mount("/", StaticFiles(directory=str(FRONTEND_DIR)), name="frontend")
else:
    @app.get("/")
    def root():
        return {"service": "pokemon-analytics", "docs": "/docs"}
