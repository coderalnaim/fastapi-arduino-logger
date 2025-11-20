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
start_epoch = 0
current_session_id = None
current_session_dir: Path | None = None  # active session directory


def utc_now_iso() -> str:
    """UTC timestamp with microsecond precision."""
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def new_session_id() -> str:
    """Folder-friendly timestamp ID."""
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


# ---------- CSV Helpers ----------
def get_device_csv_path(device_id: str) -> Path:
    if current_session_dir is None:
        return SESSIONS_DIR / f"orphan_{device_id}.csv" # Fallback (shouldn't happen with logic below)
    return current_session_dir / f"{device_id}.csv"


def ensure_device_csv(device_id: str, fieldnames: list[str]) -> None:
    csv_path = get_device_csv_path(device_id)
    if csv_path.exists():
        return

    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["server_time_utc", "device_id"] + fieldnames)


def append_samples(device_id: str, samples: list[dict], fieldnames: list[str]) -> None:
    csv_path = get_device_csv_path(device_id)
    with csv_path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        for s in samples:
            row = [utc_now_iso(), device_id]
            for key in fieldnames:
                row.append(s.get(key, ""))
            writer.writerow(row)


# ---------- ROUTES ----------
@app.get("/")
async def root():
    return HTMLResponse("<h2>IoT Logger</h2><p>Go to <a href='/dashboard'>Dashboard</a></p>")


@app.get("/dashboard")
async def dashboard():
    # Simple embedded dashboard for testing
    html = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>IoT Dashboard</title>
        <style>
            body { font-family: sans-serif; padding: 20px; text-align: center; }
            button { padding: 15px 30px; font-size: 18px; margin: 10px; cursor: pointer; }
            .status { margin-top: 20px; padding: 10px; border: 1px solid #ccc; display: inline-block; }
            .running { background-color: #d4edda; color: #155724; }
            .stopped { background-color: #f8d7da; color: #721c24; }
        </style>
    </head>
    <body>
        <h1>IoT Logger Control</h1>
        <div>
            <button onclick="startSession()">START Logging</button>
            <button onclick="stopSession()">STOP & Download</button>
        </div>
        <div id="statusBox" class="status stopped">Status: Stopped</div>
        <div id="info"></div>

        <script>
            async function updateStatus() {
                // We can poll /api/config to see current server state
                try {
                    let res = await fetch('/api/config?device_id=browser');
                    let data = await res.json();
                    let box = document.getElementById('statusBox');
                    if (data.logging) {
                        box.className = "status running";
                        box.innerText = "Status: LOGGING (Started: " + data.start_epoch + ")";
                    } else {
                        box.className = "status stopped";
                        box.innerText = "Status: STOPPED";
                    }
                } catch(e) { console.error(e); }
            }

            async function startSession() {
                let res = await fetch('/api/start', {method: 'POST'});
                let data = await res.json();
                document.getElementById('info').innerText = JSON.stringify(data);
                updateStatus();
            }

            async function stopSession() {
                window.location.href = '/api/stop'; // Triggers download
                setTimeout(updateStatus, 1000); // Update UI after download starts
            }

            setInterval(updateStatus, 2000);
            updateStatus();
        </script>
    </body>
    </html>
    """
    return HTMLResponse(html)


# ---------------- START SESSION ----------------
@app.post("/api/start")
async def api_start():
    global logging_enabled, start_epoch, current_session_id, current_session_dir

    if logging_enabled:
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
    
    print(f"✅ Session STARTED: {current_session_id}")

    return {
        "status": "started",
        "session_id": current_session_id,
        "start_epoch": start_epoch,
        "start_time_utc": now.isoformat().replace("+00:00", "Z"),
    }


# ---------------- STOP SESSION ----------------
@app.get("/api/stop") # Changed to GET so browser can trigger download easily
async def api_stop():
    global logging_enabled, start_epoch, current_session_id, current_session_dir

    if not current_session_dir or not current_session_id:
        return {"status": "error", "message": "No active session"}

    session_dir = current_session_dir
    session_id = current_session_id

    # Reset state immediately so Arduino stops logging
    logging_enabled = False
    start_epoch = 0
    current_session_id = None
    current_session_dir = None
    
    print(f"🛑 Session STOPPED: {session_id}")

    # Build ZIP in memory
    mem = io.BytesIO()
    with zipfile.ZipFile(mem, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        files = list(session_dir.glob("*.csv"))
        if not files:
            # Add a dummy file if empty so zip is valid
            zf.writestr("empty.txt", "No data collected.")
        for csv_file in files:
            zf.write(csv_file, arcname=csv_file.name)
    mem.seek(0)

    return StreamingResponse(
        mem,
        media_type="application/zip",
        headers={
            "Content-Disposition": f'attachment; filename="session_{session_id}.zip"'
        }
    )


# ---------------- CONFIG FOR ARDUINO ----------------
@app.get("/api/config")
async def api_config(device_id: str):
    """
    Arduino polls this to know IF it should log and WHAT the time is.
    """
    return {
        "logging": logging_enabled,
        "start_epoch": start_epoch,
    }


# ---------------- BULK UPLOAD FROM ARDUINO ----------------
@app.post("/api/bulk_samples")
async def api_bulk_samples(request: Request):
    global logging_enabled, current_session_dir

    # CRITICAL: Logic to tell Arduino to stop
    if not logging_enabled or current_session_dir is None:
        print("⚠️ Received data but logging is OFF. Telling Arduino to ignore.")
        return {"status": "ignored", "reason": "not_logging"}

    # Parse JSON
    body = await request.body()
    try:
        data = json.loads(body)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    device_id = data.get("device_id")
    samples = data.get("samples")

    if not device_id or not isinstance(samples, list):
         return {"status": "error", "reason": "invalid_format"}

    if len(samples) == 0:
        return {"status": "ok", "written": 0}

    # Determine fieldnames from first sample
    first = samples[0]
    fieldnames = sorted(first.keys())

    # Create CSV if needed
    ensure_device_csv(device_id, fieldnames)

    # Append rows
    append_samples(device_id, samples, fieldnames)
    
    print(f"📝 Saved {len(samples)} samples from {device_id}")

    return {"status": "ok", "written": len(samples)}
