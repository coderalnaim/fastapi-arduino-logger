from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse, StreamingResponse, HTMLResponse
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Any
import csv
import io
import json
import shutil

app = FastAPI()

# ---- State ----
BASE_DIR = Path(__file__).parent
SESSIONS_DIR = BASE_DIR / "sessions"
SESSIONS_DIR.mkdir(exist_ok=True)

logging_enabled: bool = False
start_epoch: Optional[int] = None
current_session_id: Optional[str] = None
current_session_dir: Optional[Path] = None

# For each device_id -> { "path": Path, "fields": [field1, field2, ...], "initialized": bool }
device_logs: Dict[str, Dict[str, Any]] = {}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def new_session_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def init_device_csv(device_id: str, sample_fields: List[str]):
    """
    Initialize CSV for a device in current session, with header:
    server_time_utc,device_id,<sample_fields...>
    """
    global device_logs, current_session_dir
    if current_session_dir is None:
        raise RuntimeError("No active session directory")

    csv_path = current_session_dir / f"{device_id}.csv"
    header = ["server_time_utc", "device_id"] + sample_fields

    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)

    device_logs[device_id] = {
        "path": csv_path,
        "fields": sample_fields,
        "initialized": True
    }


def append_device_rows(device_id: str, rows: List[Dict[str, Any]]):
    """
    Append rows to device CSV. rows are dicts with keys matching device_logs[device_id]['fields'].
    """
    meta = device_logs.get(device_id)
    if meta is None or not meta["initialized"]:
        raise RuntimeError(f"Device {device_id} not initialized")

    csv_path = meta["path"]
    fields = meta["fields"]

    with csv_path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        for row in rows:
            out = [utc_now_iso(), device_id]
            for key in fields:
                out.append(row.get(key, ""))
            writer.writerow(out)


@app.get("/")
async def root():
    # Simple pointer to dashboard (if served separately) or a tiny info page
    html = """
    <html>
      <head><title>IoT Logger API</title></head>
      <body>
        <h1>IoT Logger API</h1>
        <p>Use /dashboard for Start/Stop UI, or POST /api/start and /api/stop.</p>
      </body>
    </html>
    """
    return HTMLResponse(html)


@app.get("/dashboard")
async def dashboard():
    """
    Simple HTML UI with Start/Stop buttons.
    """
    html = (BASE_DIR / "dashboard.html").read_text(encoding="utf-8")
    return HTMLResponse(html)


@app.post("/api/start")
async def api_start():
    """
    Called by the web UI when user clicks START.
    Creates a new session, sets logging_enabled = True, and start_epoch = now().
    """
    global logging_enabled, start_epoch, current_session_id, current_session_dir, device_logs

    if logging_enabled:
        # Already logging, just return current status
        return {"status": "already_running", "start_epoch": start_epoch, "session_id": current_session_id}

    logging_enabled = True
    dt_now = datetime.now(timezone.utc)
    start_epoch = int(dt_now.timestamp())
    current_session_id = new_session_id()
    current_session_dir = SESSIONS_DIR / current_session_id
    current_session_dir.mkdir(exist_ok=True)
    device_logs = {}

    return {
        "status": "started",
        "start_epoch": start_epoch,
        "session_id": current_session_id,
        "start_time_utc": dt_now.isoformat().replace("+00:00", "Z"),
    }


@app.post("/api/stop")
async def api_stop():
    """
    Called by the web UI when user clicks STOP.
    Disables logging and returns a ZIP of all CSV files from this session.
    """
    global logging_enabled, start_epoch, current_session_id, current_session_dir, device_logs

    if not current_session_id or current_session_dir is None:
        raise HTTPException(status_code=400, detail="No active session to stop")

    logging_enabled = False

    # Zip all CSVs in current session dir
    mem_zip = io.BytesIO()
    with shutil.make_archive(base_name=None, format="zip", root_dir=current_session_dir, base_dir=".") as _:
        pass  # can't use shutil.make_archive directly to memory, so we do manual zip

    # Manual zip creation instead of shutil.make_archive to memory:
    import zipfile
    mem_zip = io.BytesIO()
    with zipfile.ZipFile(mem_zip, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for p in current_session_dir.glob("*.csv"):
            zf.write(p, arcname=p.name)
    mem_zip.seek(0)

    filename = f"session_{current_session_id}.zip"

    # Reset session references (but keep files on disk)
    logging_enabled = False
    start_epoch = None
    device_logs = {}

    return StreamingResponse(
        mem_zip,
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )


@app.get("/api/config")
async def api_config(device_id: str):
    """
    Polled by Arduino.
    Returns whether logging is enabled, and the start_epoch if available.
    """
    return {
        "logging": logging_enabled,
        "start_epoch": start_epoch
    }


@app.post("/api/bulk_samples")
async def api_bulk_samples(request: Request):
    """
    Arduino sends chunks:
    {
      "device_id": "tof_01",
      "samples": [
        {
          "timestamp_utc": "...",
          "sensor_time_ms": ...,
          "distance_m": ...,
          "status": ...,
          "signal": ...,
          "precision_cm": ...
        },
        ...
      ]
    }

    If logging_enabled is False, data is ignored (but returns 200).
    """
    if current_session_dir is None:
        # No session started yet, ignore
        return JSONResponse({"status": "ignored", "reason": "no_active_session"}, status_code=200)

    body = await request.body()
    try:
        data = json.loads(body)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    device_id = data.get("device_id")
    samples = data.get("samples")

    if not isinstance(device_id, str) or not isinstance(samples, list):
        raise HTTPException(status_code=400, detail="device_id (str) and samples (list) required")

    if not logging_enabled:
        # Currently not logging, just ignore the payload but respond OK
        return {"status": "ignored", "reason": "logging_disabled"}

    if len(samples) == 0:
        return {"status": "ok", "written_rows": 0}

    # Determine fields from first sample
    first_sample = samples[0]
    if not isinstance(first_sample, dict):
        raise HTTPException(status_code=400, detail="samples must contain objects")

    # Sorted keys for consistent CSV header
    sample_fields = sorted(first_sample.keys())

    # Init CSV for device if needed
    if device_id not in device_logs or not device_logs[device_id]["initialized"]:
        init_device_csv(device_id, sample_fields)
    else:
        # Ensure field set is same; if not, you might want to handle it more gracefully
        existing_fields = device_logs[device_id]["fields"]
        if existing_fields != sample_fields:
            # For simplicity, we enforce same schema per device in one session
            raise HTTPException(
                status_code=400,
                detail=f"Field mismatch for device {device_id}. Expected {existing_fields}, got {sample_fields}"
            )

    # Append rows
    append_device_rows(device_id, samples)

    return {"status": "ok", "written_rows": len(samples)}
