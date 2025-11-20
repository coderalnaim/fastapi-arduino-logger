from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse, StreamingResponse, HTMLResponse
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Any, Optional
import csv, io, json, zipfile, shutil

app = FastAPI()

# ================== GLOBAL STATE ==================
BASE_DIR = Path(__file__).parent
SESSIONS_DIR = BASE_DIR / "sessions"
SESSIONS_DIR.mkdir(exist_ok=True)

logging_enabled: bool = False
start_epoch: Optional[int] = None
current_session_id: Optional[str] = None
current_session_dir: Optional[Path] = None

# device_id → metadata: {path, fields, initialized}
device_logs: Dict[str, Dict[str, Any]] = {}


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def new_session_id():
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


# ========== Device CSV Initialization ==========
def init_device_csv(device_id: str, sample_fields: List[str]):
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
    meta = device_logs[device_id]
    csv_path = meta["path"]
    fields = meta["fields"]

    with csv_path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        for row in rows:
            out = [utc_now_iso(), device_id]
            for key in fields:
                out.append(row.get(key, ""))
            writer.writerow(out)


# ========== Minimal Root/Dashboard ==========
@app.get("/")
async def root():
    html = "<h2>IoT Logger API</h2><p>Go to <a href='/dashboard'>Dashboard</a></p>"
    return HTMLResponse(html)


@app.get("/dashboard")
async def dashboard():
    return HTMLResponse((BASE_DIR / "dashboard.html").read_text())


# ========== START LOGGING ==========
@app.post("/api/start")
async def api_start():
    global logging_enabled, start_epoch, current_session_id, current_session_dir, device_logs

    if logging_enabled:
        return {"status": "already_running", "start_epoch": start_epoch}

    logging_enabled = True
    dt_now = datetime.now(timezone.utc)
    start_epoch = int(dt_now.timestamp())

    current_session_id = new_session_id()
    current_session_dir = SESSIONS_DIR / current_session_id
    current_session_dir.mkdir(exist_ok=True)

    device_logs = {}

    return {
        "status": "started",
        "session_id": current_session_id,
        "start_epoch": start_epoch,
        "start_time_utc": dt_now.isoformat().replace("+00:00", "Z"),
    }


# ========== STOP LOGGING & DOWNLOAD ZIP ==========
@app.post("/api/stop")
async def api_stop():
    global logging_enabled, start_epoch, current_session_id, current_session_dir, device_logs

    if not current_session_dir:
        raise HTTPException(status_code=400, detail="No active session")

    # Save session info before reset
    session_id = current_session_id
    session_dir = current_session_dir

    # Reset server state for next session
    logging_enabled = False
    start_epoch = None
    device_logs = {}
    current_session_id = None
    current_session_dir = None

    # Create ZIP in memory
    mem = io.BytesIO()
    with zipfile.ZipFile(mem, "w", zipfile.ZIP_DEFLATED) as zf:
        for p in session_dir.glob("*.csv"):
            zf.write(p, arcname=p.name)

    mem.seek(0)
    return StreamingResponse(
        mem,
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="session_{session_id}.zip"'}
    )


# ========== ARDUINO POLLS THIS ==========
@app.get("/api/config")
async def api_config(device_id: str):
    return {
        "logging": logging_enabled,
        "start_epoch": start_epoch
    }


# ========== BULK UPLOAD FROM ARDUINO ==========
@app.post("/api/bulk_samples")
async def api_bulk_samples(request: Request):
    global logging_enabled, current_session_dir

    if not current_session_dir:
        return {"status": "ignored", "reason": "no_session"}

    body = await request.body()
    try:
        data = json.loads(body)
    except Exception:
        raise HTTPException(status_code=400, detail="Bad JSON")

    device_id = data.get("device_id")
    samples = data.get("samples")

    if not device_id or not isinstance(samples, list):
        raise HTTPException(400, "device_id and samples required")

    if not logging_enabled:
        return {"status": "ignored", "reason": "logging_disabled"}

    # Setup CSV for this device
    if device_id not in device_logs:
        fields = sorted(samples[0].keys())
        init_device_csv(device_id, fields)

    append_device_rows(device_id, samples)

    return {"status": "ok", "written": len(samples)}
