from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse
from datetime import datetime, timezone
from pathlib import Path
import csv
import io
import json
import zipfile

app = FastAPI()

# ---------- Paths ----------
BASE_DIR = Path(__file__).parent
SESSIONS_DIR = BASE_DIR / "sessions"
SESSIONS_DIR.mkdir(exist_ok=True)

# ---------- Global state ----------
logging_enabled = False
start_epoch = None
current_session_id = None
current_session_dir: Path | None = None  # type: ignore[assignment]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def new_session_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


# ---------- Helpers ----------
def get_device_csv_path(device_id: str) -> Path:
    assert current_session_dir is not None
    return current_session_dir / f"{device_id}.csv"


def ensure_device_csv(device_id: str, fieldnames: list[str]) -> None:
    """
    If CSV for this device does not exist yet, create it and write header:
    server_time_utc,device_id,<fieldnames...>
    """
    csv_path = get_device_csv_path(device_id)
    if csv_path.exists():
        return

    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        header = ["server_time_utc", "device_id"] + fieldnames
        writer.writerow(header)


def append_samples(device_id: str, samples: list[dict], fieldnames: list[str]) -> None:
    """
    Append all samples of this device to its CSV file.
    """
    csv_path = get_device_csv_path(device_id)
    with csv_path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        for s in samples:
            row = [utc_now_iso(), device_id]
            for key in fieldnames:
                row.append(s.get(key, ""))
            writer.writerow(row)


# ---------- Routes ----------
@app.get("/")
async def root():
    return HTMLResponse("<h2>IoT Logger</h2><p>Go to <a href='/dashboard'>/dashboard</a></p>")


@app.get("/dashboard")
async def dashboard():
    html = (BASE_DIR / "dashboard.html").read_text(encoding="utf-8")
    return HTMLResponse(html)


@app.post("/api/start")
async def api_start():
    """
    Start a new logging session:
    - sets logging_enabled = True
    - records start_epoch
    - creates a new /sessions/<session_id> directory
    """
    global logging_enabled, start_epoch, current_session_id, current_session_dir

    if logging_enabled:
        # Already running, return current info
        return {
            "status": "already_running",
            "session_id": current_session_id,
            "start_epoch": start_epoch,
        }

    logging_enabled = True
    now = datetime.now(timezone.utc)
    start_epoch = int(now.timestamp())
    current_session_id = new_session_id()
    current_session_dir = SESSIONS_DIR / current_session_id
    current_session_dir.mkdir(exist_ok=True)

    return {
        "status": "started",
        "session_id": current_session_id,
        "start_epoch": start_epoch,
        "start_time_utc": now.isoformat().replace("+00:00", "Z"),
    }


@app.post("/api/stop")
async def api_stop():
    """
    Stop logging and return a ZIP file with all device CSVs for this session.
    """
    global logging_enabled, start_epoch, current_session_id, current_session_dir

    if current_session_dir is None or current_session_id is None:
        raise HTTPException(status_code=400, detail="No active session to stop")

    # Keep local copy of session_dir before we clear global state
    session_dir = current_session_dir
    session_id = current_session_id

    # Reset state for next run
    logging_enabled = False
    start_epoch = None
    current_session_dir = None
    current_session_id = None

    # Create ZIP in memory
    mem = io.BytesIO()
    with zipfile.ZipFile(mem, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for csv_file in session_dir.glob("*.csv"):
            zf.write(csv_file, arcname=csv_file.name)
    mem.seek(0)

    return StreamingResponse(
        mem,
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename=\"session_{session_id}.zip\""}
    )


@app.get("/api/config")
async def api_config(device_id: str):
    """
    Polled by Arduino.
    Returns:
    - logging: whether logging is currently enabled on server
    - start_epoch: the UTC epoch when the current session started
    (Arduino uses this as its baseEpoch for timestamps.)
    """
    return {
        "logging": logging_enabled,
        "start_epoch": start_epoch,
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

    If logging is disabled or no active session, data is ignored.
    """
    global logging_enabled, current_session_dir

    if not logging_enabled or current_session_dir is None:
        # Ignore but reply OK so Arduino isn't confused
        return {"status": "ignored", "reason": "not_logging"}

    body = await request.body()
    try:
        data = json.loads(body)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    device_id = data.get("device_id")
    samples = data.get("samples")

    if not isinstance(device_id, str) or not isinstance(samples, list):
        raise HTTPException(status_code=400, detail="device_id (str) and samples (list) required")

    if len(samples) == 0:
        return {"status": "ok", "written": 0}

    # Fieldnames are the keys of the first sample, sorted for consistency
    first = samples[0]
    if not isinstance(first, dict):
        raise HTTPException(status_code=400, detail="samples must contain objects")

    fieldnames = sorted(first.keys())

    # Ensure CSV exists with proper header
    ensure_device_csv(device_id, fieldnames)

    # Append all samples
    append_samples(device_id, samples, fieldnames)

    return {"status": "ok", "written": len(samples)}
