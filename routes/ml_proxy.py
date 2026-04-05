"""ML service proxy routes: /api/ml/*."""

import os
from fastapi import APIRouter
from fastapi.responses import JSONResponse

router = APIRouter(prefix="/api/ml")

_ml_url = os.getenv("ML_API_URL", "")


@router.post("/train")
async def proxy_ml_train():
    import httpx
    if not _ml_url:
        return JSONResponse({"error": "ML_API_URL not configured"}, status_code=500)
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.post(f"{_ml_url}/api/train")
        return JSONResponse(r.json(), status_code=r.status_code)


@router.post("/train-only")
async def proxy_ml_train_only():
    import httpx
    if not _ml_url:
        return JSONResponse({"error": "ML_API_URL not configured"}, status_code=500)
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.post(f"{_ml_url}/api/train-only")
        return JSONResponse(r.json(), status_code=r.status_code)


@router.get("/training-status")
async def proxy_ml_status():
    import httpx
    if not _ml_url:
        return JSONResponse({"error": "ML_API_URL not configured"}, status_code=500)
    async with httpx.AsyncClient(timeout=5) as client:
        r = await client.get(f"{_ml_url}/api/training-status")
        return JSONResponse(r.json())


@router.get("/health")
async def proxy_ml_health():
    import httpx
    if not _ml_url:
        return JSONResponse({"error": "ML_API_URL not configured"}, status_code=500)
    async with httpx.AsyncClient(timeout=5) as client:
        r = await client.get(f"{_ml_url}/health")
        return JSONResponse(r.json())
