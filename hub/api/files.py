"""Route module: files."""

import logging
import os
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import Depends, HTTPException, Request
from starlette.responses import FileResponse

from hub import feature_store
from hub.api.core import _verify_token

logger = logging.getLogger(__name__)

# File store
_file_store: Dict[str, Dict[str, Any]] = {}
_file_store_lock = threading.Lock()

# Upload directory
_UPLOAD_DIR = Path(os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'uploads'))
_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

# Max file size (100MB)
_MAX_FILE_SIZE = 100 * 1024 * 1024


def _audit(action: str, user: str, resource_type: str, resource_id: str, details: str) -> None:
    """Record an audit log entry."""
    pass  # Placeholder


async def api_upload_file(request: Request, _auth: None = Depends(_verify_token)):
    """Upload a file to the storage.

    Accepts multipart/form-data with a 'file' field.
    Optional fields: 'run_id' (associate with a run), 'description'.
    """
    import io

    # Parse multipart manually since we can't use UploadFile in this signature
    body = await request.body()
    content_type = request.headers.get("content-type", "")

    if "multipart/form-data" not in content_type:
        raise HTTPException(status_code=400, detail="Content-Type must be multipart/form-data")

    # Simple multipart parsing
    boundary = content_type.split("boundary=")[-1].strip()
    boundaries = [f"--{boundary}".encode()]

    # Find file content between boundaries
    parts = body.split(b"--{boundary}".format(boundary=boundary).encode())

    file_data = None
    filename = "upload"
    run_id = None
    description = ""

    for part in parts:
        if b"Content-Disposition" in part:
            # Extract filename
            if b'filename=' in part:
                fname_start = part.find(b"filename=") + 8
                fname_end = part.find(b'"', fname_start)
                if fname_end > fname_start:
                    filename = part[fname_start:fname_end].decode("utf-8", errors="replace")

            # Extract run_id
            if b"run_id" in part:
                rid_start = part.find(b'run_id"')
                if rid_start > 0:
                    rid_value_start = part.find(b"\r\n\r\n", rid_start)
                    if rid_value_start > 0:
                        rid_value_end = part.find(b"\r\n", rid_value_start + 4)
                        if rid_value_end > 0:
                            run_id = part[rid_value_start + 4:rid_value_end].decode("utf-8", errors="replace").strip()
                        else:
                            run_id = part[rid_value_start + 4:].decode("utf-8", errors="replace").strip()

            # Extract description
            if b"description" in part:
                desc_start = part.find(b'description"')
                if desc_start > 0:
                    desc_value_start = part.find(b"\r\n\r\n", desc_start)
                    if desc_value_start > 0:
                        desc_value_end = part.find(b"\r\n", desc_value_start + 4)
                        if desc_value_end > 0:
                            description = part[desc_value_start + 4:desc_value_end].decode("utf-8", errors="replace").strip()
                        else:
                            description = part[desc_value_start + 4:].decode("utf-8", errors="replace").strip()

            # File content is after the blank line following headers
            header_end = part.find(b"\r\n\r\n")
            if header_end >= 0 and b"filename=" in part:
                file_data = part[header_end + 4:]
                # Remove trailing boundary markers
                if file_data.endswith(b"\r\n"):
                    file_data = file_data[:-2]

    if file_data is None:
        raise HTTPException(status_code=400, detail="No file found in request")

    if len(file_data) > _MAX_FILE_SIZE:
        raise HTTPException(status_code=413, detail=f"File too large (max {_MAX_FILE_SIZE // (1024*1024)}MB)")

    file_id = f"file-{uuid.uuid4().hex[:8]}"
    file_path = _UPLOAD_DIR / f"{file_id}_{filename}"
    file_path.write_bytes(file_data)

    file_meta = {
        "id": file_id,
        "filename": filename,
        "size": len(file_data),
        "content_type": request.headers.get("content-type", "application/octet-stream"),
        "run_id": run_id,
        "description": description,
        "uploaded_at": datetime.now(timezone.utc).isoformat(),
    }

    with _file_store_lock:
        _file_store[file_id] = file_meta

    # Persist to database
    try:
        feature_store.save_file_metadata(file_meta)
    except Exception as e:
        logger.warning(f"Failed to save file metadata: {e}")

    _audit("file.uploaded", _auth or "anonymous", "file", f"Uploaded {filename}", "")
    return file_meta


async def api_list_files(
    run_id: Optional[str] = None,
    _auth: None = Depends(_verify_token),
):
    """List all uploaded files, optionally filtered by run_id."""
    with _file_store_lock:
        files = list(_file_store.values())

    if run_id:
        files = [f for f in files if f.get("run_id") == run_id]

    return {"files": files}


async def api_get_file(file_id: str, _auth: None = Depends(_verify_token)):
    """Get file metadata."""
    with _file_store_lock:
        file_meta = _file_store.get(file_id)
    if not file_meta:
        raise HTTPException(status_code=404, detail="File not found")
    return file_meta


async def api_download_file(file_id: str, _auth: None = Depends(_verify_token)):
    """Download an uploaded file."""
    with _file_store_lock:
        file_meta = _file_store.get(file_id)
    if not file_meta:
        raise HTTPException(status_code=404, detail="File not found")

    file_path = _UPLOAD_DIR / f"{file_id}_{file_meta['filename']}"
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="File not found on disk")

    _audit("file.downloaded", _auth or "anonymous", "file", f"Downloaded {file_meta['filename']}", "")
    return FileResponse(
        str(file_path),
        filename=file_meta["filename"],
        media_type=file_meta.get("content_type", "application/octet-stream"),
    )


async def api_delete_file(file_id: str, _auth: None = Depends(_verify_token)):
    """Delete an uploaded file."""
    with _file_store_lock:
        file_meta = _file_store.get(file_id)
        if not file_meta:
            raise HTTPException(status_code=404, detail="File not found")
        del _file_store[file_id]

    # Persist to database
    try:
        feature_store.delete_file_metadata(file_id)
    except Exception as e:
        logger.warning(f"Failed to delete file metadata: {e}")

    file_path = _UPLOAD_DIR / f"{file_id}_{file_meta['filename']}"
    if file_path.exists():
        file_path.unlink()

    _audit("file.deleted", _auth or "anonymous", "file", f"Deleted {file_meta['filename']}", "")
    return {"status": "ok", "message": "File deleted"}


def register(app):
    """Register files routes with the FastAPI app."""
    app.post("/api/files/upload", status_code=201)(api_upload_file)
    app.get("/api/files")(api_list_files)
    app.get("/api/files/{file_id}")(api_get_file)
    app.get("/api/files/{file_id}/download")(api_download_file)
    app.delete("/api/files/{file_id}")(api_delete_file)
