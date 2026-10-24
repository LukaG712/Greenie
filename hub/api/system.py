"""System stats and logs API routes for Greenie.

Provides system resource statistics and aggregated log access for the
operational dashboard.
"""

import logging
import os
import platform
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/system", tags=["System"])

# In-memory log buffer (in production, this would connect to Sneaker)
_log_buffer: List[Dict[str, Any]] = []
MAX_LOG_ENTRIES = 1000


def _add_log(level: str, service: str, message: str):
    """Add a log entry to the buffer."""
    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "level": level,
        "service": service,
        "message": message,
    }
    _log_buffer.append(entry)
    if len(_log_buffer) > MAX_LOG_ENTRIES:
        _log_buffer.pop(0)


@router.get("/stats")
async def get_system_stats():
    """Get system resource statistics."""
    try:
        # CPU usage
        cpu_percent = 0.0
        if os.path.exists("/proc/stat"):
            with open("/proc/stat") as f:
                line = f.readline()
            values = list(map(int, line.split()[1:]))
            total = sum(values)
            idle = values[3]
            # Simple approximation
            cpu_percent = (1 - idle / total) * 100 if total > 0 else 0

        # Memory usage
        memory_percent = 0.0
        if os.path.exists("/proc/meminfo"):
            with open("/proc/meminfo") as f:
                content = f.read()
            for line in content.splitlines():
                if line.startswith("MemTotal:"):
                    total = int(line.split()[1])
                elif line.startswith("MemAvailable:"):
                    available = int(line.split()[1])
            memory_percent = ((total - available) / total) * 100 if total > 0 else 0

        # Disk usage
        disk_percent = 0.0
        try:
            statvfs = os.statvfs("/")
            disk_percent = ((statvfs.f_blocks - statvfs.f_bfree) / statvfs.f_blocks) * 100
        except Exception:
            pass

        # Uptime
        uptime = 0.0
        if os.path.exists("/proc/uptime"):
            with open("/proc/uptime") as f:
                uptime = float(f.readline().split()[0])

        # Fallback: use platform info
        if cpu_percent == 0 and memory_percent == 0:
            import psutil
            cpu_percent = psutil.cpu_percent(interval=0.1)
            memory = psutil.virtual_memory()
            memory_percent = memory.percent
            disk = psutil.disk_usage("/")
            disk_percent = disk.percent

        return {
            "cpuPercent": cpu_percent,
            "memoryPercent": memory_percent,
            "diskPercent": disk_percent,
            "uptime": uptime,
            "hostname": platform.node(),
            "platform": platform.platform(),
            "pythonVersion": platform.python_version(),
        }
    except ImportError:
        # psutil not available, return basic info
        return {
            "cpuPercent": 0,
            "memoryPercent": 0,
            "diskPercent": 0,
            "uptime": time.time() % 86400,  # approximate
            "hostname": platform.node(),
            "platform": platform.platform(),
            "pythonVersion": platform.python_version(),
        }


@router.get("/logs")
async def get_logs(
    limit: int = Query(100, ge=1, le=1000),
    service: Optional[str] = None,
    level: Optional[str] = None,
    q: Optional[str] = None,
):
    """Get recent log entries with filtering."""
    logs = _log_buffer.copy()

    # Filter by service
    if service:
        logs = [l for l in logs if l.get("service", "").lower() == service.lower()]

    # Filter by level
    if level:
        logs = [l for l in logs if l.get("level", "").lower() == level.lower()]

    # Filter by search query
    if q:
        q_lower = q.lower()
        logs = [l for l in logs if q_lower in l.get("message", "").lower()]

    # Sort by timestamp descending and limit
    logs = sorted(logs, key=lambda x: x.get("timestamp", ""), reverse=True)[:limit]

    return {"logs": logs, "total": len(logs)}


@router.post("/logs")
async def add_log(data: Dict[str, Any]):
    """Add a log entry (for other services to push logs)."""
    level = data.get("level", "info")
    service = data.get("service", "unknown")
    message = data.get("message", "")
    if not message:
        raise HTTPException(status_code=400, detail="message is required")
    _add_log(level, service, message)
    return {"status": "ok"}


def register(app):
    """Register system routes with the FastAPI app."""
    app.include_router(router)
