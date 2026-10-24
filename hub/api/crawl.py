"""Route module: crawl."""

import os
from typing import Any, Dict, Optional

import httpx
from fastapi import Depends, HTTPException

from hub.api.core import _verify_token


async def api_crawl_scrape(body: Dict[str, Any], _auth: None = Depends(_verify_token)):
    """Proxy: Scrape a single URL via Trapper."""
    base_url = os.environ.get("TRAPPER_URL", "http://localhost:3000").rstrip("/")
    api_key = os.environ.get("TRAPPER_API_KEY", "")
    url = body.get("url")
    if not url:
        raise HTTPException(status_code=400, detail="url is required")
    payload = {"url": url}
    payload.update({k: v for k, v in body.items() if k != "url"})
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    try:
        async with httpx.AsyncClient(timeout=120.0) as client:
            resp = await client.post(f"{base_url}/scrape", json=payload, headers=headers)
            if resp.status_code != 200:
                raise HTTPException(status_code=resp.status_code, detail=resp.text)
            return resp.json()
    except httpx.ConnectError:
        raise HTTPException(status_code=502, detail=f"Cannot connect to Trapper at {base_url}")


async def api_crawl_start(body: Dict[str, Any], _auth: None = Depends(_verify_token)):
    """Proxy: Start a crawl job via Trapper."""
    base_url = os.environ.get("TRAPPER_URL", "http://localhost:3000").rstrip("/")
    api_key = os.environ.get("TRAPPER_API_KEY", "")
    url = body.get("url")
    if not url:
        raise HTTPException(status_code=400, detail="url is required")
    payload = {"url": url}
    payload.update({k: v for k, v in body.items() if k != "url"})
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(f"{base_url}/crawl", json=payload, headers=headers)
            if resp.status_code != 201:
                raise HTTPException(status_code=resp.status_code, detail=resp.text)
            return resp.json()
    except httpx.ConnectError:
        raise HTTPException(status_code=502, detail=f"Cannot connect to Trapper at {base_url}")


async def api_crawl_status(job_id: str, _auth: None = Depends(_verify_token)):
    """Proxy: Check crawl job status via Trapper."""
    base_url = os.environ.get("TRAPPER_URL", "http://localhost:3000").rstrip("/")
    api_key = os.environ.get("TRAPPER_API_KEY", "")
    headers = {}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(f"{base_url}/crawl/{job_id}", headers=headers)
            if resp.status_code != 200:
                raise HTTPException(status_code=resp.status_code, detail=resp.text)
            return resp.json()
    except httpx.ConnectError:
        raise HTTPException(status_code=502, detail=f"Cannot connect to Trapper at {base_url}")


async def api_crawl_search(body: Dict[str, Any], _auth: None = Depends(_verify_token)):
    """Proxy: Search the web via Trapper."""
    base_url = os.environ.get("TRAPPER_URL", "http://localhost:3000").rstrip("/")
    api_key = os.environ.get("TRAPPER_API_KEY", "")
    query = body.get("query")
    if not query:
        raise HTTPException(status_code=400, detail="query is required")
    payload = {"query": query}
    payload.update({k: v for k, v in body.items() if k != "query"})
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    try:
        async with httpx.AsyncClient(timeout=120.0) as client:
            resp = await client.post(f"{base_url}/search", json=payload, headers=headers)
            if resp.status_code != 200:
                raise HTTPException(status_code=resp.status_code, detail=resp.text)
            return resp.json()
    except httpx.ConnectError:
        raise HTTPException(status_code=502, detail=f"Cannot connect to Trapper at {base_url}")


async def api_crawl_map(body: Dict[str, Any], _auth: None = Depends(_verify_token)):
    """Proxy: Map URLs on a site via Trapper."""
    base_url = os.environ.get("TRAPPER_URL", "http://localhost:3000").rstrip("/")
    api_key = os.environ.get("TRAPPER_API_KEY", "")
    url = body.get("url")
    if not url:
        raise HTTPException(status_code=400, detail="url is required")
    payload = {"url": url}
    payload.update({k: v for k, v in body.items() if k != "url"})
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.post(f"{base_url}/map", json=payload, headers=headers)
            if resp.status_code != 200:
                raise HTTPException(status_code=resp.status_code, detail=resp.text)
            return resp.json()
    except httpx.ConnectError:
        raise HTTPException(status_code=502, detail=f"Cannot connect to Trapper at {base_url}")


async def api_crawl_batch(body: Dict[str, Any], _auth: None = Depends(_verify_token)):
    """Proxy: Start a batch scrape job via Trapper."""
    base_url = os.environ.get("TRAPPER_URL", "http://localhost:3000").rstrip("/")
    api_key = os.environ.get("TRAPPER_API_KEY", "")
    urls = body.get("urls")
    if not urls or not isinstance(urls, list):
        raise HTTPException(status_code=400, detail="urls list is required")
    payload = {"urls": urls}
    payload.update({k: v for k, v in body.items() if k != "urls"})
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(f"{base_url}/batch_scrape", json=payload, headers=headers)
            if resp.status_code != 201:
                raise HTTPException(status_code=resp.status_code, detail=resp.text)
            return resp.json()
    except httpx.ConnectError:
        raise HTTPException(status_code=502, detail=f"Cannot connect to Trapper at {base_url}")


async def api_crawl_batch_status(job_id: str, _auth: None = Depends(_verify_token)):
    """Proxy: Check batch scrape job status via Trapper."""
    base_url = os.environ.get("TRAPPER_URL", "http://localhost:3000").rstrip("/")
    api_key = os.environ.get("TRAPPER_API_KEY", "")
    headers = {}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(f"{base_url}/batch_scrape/{job_id}", headers=headers)
            if resp.status_code != 200:
                raise HTTPException(status_code=resp.status_code, detail=resp.text)
            return resp.json()
    except httpx.ConnectError:
        raise HTTPException(status_code=502, detail=f"Cannot connect to Trapper at {base_url}")


async def api_crawl_batch_cancel(job_id: str, _auth: None = Depends(_verify_token)):
    """Proxy: Cancel a batch scrape job via Trapper."""
    base_url = os.environ.get("TRAPPER_URL", "http://localhost:3000").rstrip("/")
    api_key = os.environ.get("TRAPPER_API_KEY", "")
    headers = {}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.delete(f"{base_url}/batch_scrape/{job_id}", headers=headers)
            if resp.status_code != 200:
                raise HTTPException(status_code=resp.status_code, detail=resp.text)
            return resp.json()
    except httpx.ConnectError:
        raise HTTPException(status_code=502, detail=f"Cannot connect to Trapper at {base_url}")


async def api_crawl_cancel(job_id: str, _auth: None = Depends(_verify_token)):
    """Proxy: Cancel a crawl job via Trapper."""
    base_url = os.environ.get("TRAPPER_URL", "http://localhost:3000").rstrip("/")
    api_key = os.environ.get("TRAPPER_API_KEY", "")
    headers = {}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.delete(f"{base_url}/crawl/{job_id}", headers=headers)
            if resp.status_code != 200:
                raise HTTPException(status_code=resp.status_code, detail=resp.text)
            return resp.json()
    except httpx.ConnectError:
        raise HTTPException(status_code=502, detail=f"Cannot connect to Trapper at {base_url}")


async def api_crawl_interact(body: Dict[str, Any], _auth: None = Depends(_verify_token)):
    """Proxy: Interact with a page via Trapper."""
    base_url = os.environ.get("TRAPPER_URL", "http://localhost:3000").rstrip("/")
    api_key = os.environ.get("TRAPPER_API_KEY", "")
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    payload = dict(body)
    try:
        async with httpx.AsyncClient(timeout=120.0) as client:
            resp = await client.post(f"{base_url}/interact", json=payload, headers=headers)
            if resp.status_code != 200:
                raise HTTPException(status_code=resp.status_code, detail=resp.text)
            return resp.json()
    except httpx.ConnectError:
        raise HTTPException(status_code=502, detail=f"Cannot connect to Trapper at {base_url}")


async def api_crawl_extract(body: Dict[str, Any], _auth: None = Depends(_verify_token)):
    """Proxy: Start an extract job via Trapper."""
    base_url = os.environ.get("TRAPPER_URL", "http://localhost:3000").rstrip("/")
    api_key = os.environ.get("TRAPPER_API_KEY", "")
    urls = body.get("urls")
    prompt = body.get("prompt")
    if not urls or not prompt:
        raise HTTPException(status_code=400, detail="urls and prompt are required")
    payload = {"urls": urls, "prompt": prompt}
    payload.update({k: v for k, v in body.items() if k not in ("urls", "prompt")})
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(f"{base_url}/extract", json=payload, headers=headers)
            if resp.status_code != 201:
                raise HTTPException(status_code=resp.status_code, detail=resp.text)
            return resp.json()
    except httpx.ConnectError:
        raise HTTPException(status_code=502, detail=f"Cannot connect to Trapper at {base_url}")


async def api_crawl_extract_status(job_id: str, _auth: None = Depends(_verify_token)):
    """Proxy: Check extract job status via Trapper."""
    base_url = os.environ.get("TRAPPER_URL", "http://localhost:3000").rstrip("/")
    api_key = os.environ.get("TRAPPER_API_KEY", "")
    headers = {}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(f"{base_url}/extract/{job_id}", headers=headers)
            if resp.status_code != 200:
                raise HTTPException(status_code=resp.status_code, detail=resp.text)
            return resp.json()
    except httpx.ConnectError:
        raise HTTPException(status_code=502, detail=f"Cannot connect to Trapper at {base_url}")


async def api_crawl_extract_cancel(job_id: str, _auth: None = Depends(_verify_token)):
    """Proxy: Cancel an extract job via Trapper."""
    base_url = os.environ.get("TRAPPER_URL", "http://localhost:3000").rstrip("/")
    api_key = os.environ.get("TRAPPER_API_KEY", "")
    headers = {}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.delete(f"{base_url}/extract/{job_id}", headers=headers)
            if resp.status_code != 200:
                raise HTTPException(status_code=resp.status_code, detail=resp.text)
            return resp.json()
    except httpx.ConnectError:
        raise HTTPException(status_code=502, detail=f"Cannot connect to Trapper at {base_url}")


async def api_crawl_agent(body: Dict[str, Any], _auth: None = Depends(_verify_token)):
    """Proxy: Start an agent research job via Trapper."""
    base_url = os.environ.get("TRAPPER_URL", "http://localhost:3000").rstrip("/")
    api_key = os.environ.get("TRAPPER_API_KEY", "")
    prompt = body.get("prompt")
    if not prompt:
        raise HTTPException(status_code=400, detail="prompt is required")
    payload = {"prompt": prompt}
    payload.update({k: v for k, v in body.items() if k != "prompt"})
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(f"{base_url}/agent", json=payload, headers=headers)
            if resp.status_code != 201:
                raise HTTPException(status_code=resp.status_code, detail=resp.text)
            return resp.json()
    except httpx.ConnectError:
        raise HTTPException(status_code=502, detail=f"Cannot connect to Trapper at {base_url}")


async def api_crawl_agent_status(job_id: str, _auth: None = Depends(_verify_token)):
    """Proxy: Check agent job status via Trapper."""
    base_url = os.environ.get("TRAPPER_URL", "http://localhost:3000").rstrip("/")
    api_key = os.environ.get("TRAPPER_API_KEY", "")
    headers = {}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(f"{base_url}/agent/{job_id}", headers=headers)
            if resp.status_code != 200:
                raise HTTPException(status_code=resp.status_code, detail=resp.text)
            return resp.json()
    except httpx.ConnectError:
        raise HTTPException(status_code=502, detail=f"Cannot connect to Trapper at {base_url}")


async def api_crawl_agent_cancel(job_id: str, _auth: None = Depends(_verify_token)):
    """Proxy: Cancel an agent job via Trapper."""
    base_url = os.environ.get("TRAPPER_URL", "http://localhost:3000").rstrip("/")
    api_key = os.environ.get("TRAPPER_API_KEY", "")
    headers = {}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.delete(f"{base_url}/agent/{job_id}", headers=headers)
            if resp.status_code != 200:
                raise HTTPException(status_code=resp.status_code, detail=resp.text)
            return resp.json()
    except httpx.ConnectError:
        raise HTTPException(status_code=502, detail=f"Cannot connect to Trapper at {base_url}")


# ─── Slinker proxy endpoints ───────────────────────────────────────────────


async def api_crawl_health(_auth: None = Depends(_verify_token)):
    """Proxy: Trapper health check."""
    base_url = os.environ.get("TRAPPER_URL", "http://localhost:3000").rstrip("/")
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(f"{base_url}/health")
            if resp.status_code != 200:
                raise HTTPException(status_code=resp.status_code, detail=resp.text)
            return resp.json()
    except httpx.ConnectError:
        raise HTTPException(status_code=502, detail=f"Cannot connect to Trapper at {base_url}")


# ─── Cross-Service Pipeline ─────────────────────────────────────────────────



def register(app):
    """Register crawl routes with the FastAPI app."""
    app.post("/api/crawl/scrape")(api_crawl_scrape)
    app.post("/api/crawl/crawl")(api_crawl_start)
    app.get("/api/crawl/{job_id}")(api_crawl_status)
    app.post("/api/crawl/search")(api_crawl_search)
    app.post("/api/crawl/map")(api_crawl_map)
    app.post("/api/crawl/batch")(api_crawl_batch)
    app.get("/api/crawl/batch/{job_id}")(api_crawl_batch_status)
    app.delete("/api/crawl/batch/{job_id}")(api_crawl_batch_cancel)
    app.delete("/api/crawl/{job_id}")(api_crawl_cancel)
    app.post("/api/crawl/interact")(api_crawl_interact)
    app.post("/api/crawl/extract")(api_crawl_extract)
    app.get("/api/crawl/extract/{job_id}")(api_crawl_extract_status)
    app.delete("/api/crawl/extract/{job_id}")(api_crawl_extract_cancel)
    app.post("/api/crawl/agent")(api_crawl_agent)
    app.get("/api/crawl/agent/{job_id}")(api_crawl_agent_status)
    app.delete("/api/crawl/agent/{job_id}")(api_crawl_agent_cancel)
    app.get("/api/crawl/health")(api_crawl_health)
