from fastapi import FastAPI, HTTPException, Body
from fastapi.responses import HTMLResponse, StreamingResponse
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
import csv
import io
import zipfile

app = FastAPI(title="Bulk Sensor Logger (2-minute chunks)")

# Directory for temporary CSVs
LOG_DIR = Path("session_logs")
LOG_DIR.mkdir(exist_ok=True)

# Global session state
logging_enabled: bool = False
session_id: Optional[str] = None
device_files: Dict[str, Path] = {}       # device_id -> csv path
device_headers: Dict[str, List[str]] = {}  # device_id -> header list


def start_session():
    """Start a new global logging session."""
    global logging_enabled, session_id, device_files, device_headers

    logging_enabled = True
    session_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    device_files = {}
    device_headers = {}


def stop_session_and_zip() -> bytes:
    """
    Stop logging, bundle all device CSVs into an in-memory ZIP,
    delete the CSVs on disk, and return the ZIP bytes.
    """
    global logging_enabled, session_id, device_files, device_headers

    logging_enabled = False

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for dev_id, path in device_files.items():
            if path.exists():
                zf.write(path, arcname=path.name)

    buf.seek(0)

    # Clean up files on disk
    for path in device_files.values():
        try:
            if path.exists():
                path.unlink()
        except Exception:
            pass

    device_files = {}
    device_headers = {}
    session_id = None

    return buf.getvalue()


@app.get("/")
def root():
    return {
        "message": "Bulk Sensor Logger (2-minute chunks)",
        "description": "Click /dashboard to control logging and download CSV ZIP.",
        "endpoints": {
            "GET  /dashboard": "Start / Stop & download buttons",
            "POST /api/start": "Start global logging session",
            "POST /api/stop": "Stop logging & download ZIP",
            "POST /api/bulk_samples": "Arduino sends 2-minute JSON chunks here",
            "GET  /api/status": "Current logging state and device files",
        },
    }


@app.get("/api/status")
def api_status():
    return {
        "logging_enabled": logging_enabled,
        "session_id": session_id,
        "devices": {
            dev: {
                "csv_file": str(path),
                "header": device_headers.get(dev, []),
            }
            for dev, path in device_files.items()
        },
    }


@app.post("/api/start")
def api_start():
    """
    Start a new logging session. If already running, just return current state.
    """
    if logging_enabled:
        return {"logging_enabled": True, "session_id": session_id, "note": "already running"}

    start_session()
    return {"logging_enabled": True, "session_id": session_id}


@app.post("/api/stop")
def api_stop():
    """
    Stop logging, create a ZIP with all session CSVs, delete them on disk,
    and return the ZIP to the client.
    """
    if not session_id:
        raise HTTPException(400, "No active session")

    zip_bytes = stop_session_and_zip()
    filename = f"sensor_logs_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.zip"

    return StreamingResponse(
        io.BytesIO(zip_bytes),
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.post("/api/bulk_samples")
def bulk_samples(payload: dict = Body(...)):
    """
    Arduino sends data every 2 minutes in a bulk JSON payload:

    {
      "device_id": "tof_01",
      "samples": [
        {
          "timestamp_utc": "2025-11-19T18:02:57.842Z",
          "sensor_time_ms": 2736032,
          "distance_m": 0.046,
          "status": 1,
          "signal": 415,
          "precision_cm": 2
        },
        ...
      ]
    }

    If logging is enabled, all samples are appended to the device's CSV.
    If logging is disabled, samples are ignored (but request still returns 200).
    """

    if "device_id" not in payload:
        raise HTTPException(400, "Missing 'device_id'")
    if "samples" not in payload or not isinstance(payload["samples"], list):
        raise HTTPException(400, "Missing or invalid 'samples' (must be a list)")

    dev_id = str(payload["device_id"])
    samples = payload["samples"]

    if not samples:
        return {"stored": False, "reason": "empty_samples"}

    # If global logging is OFF, ignore but don't error
    if not logging_enabled or session_id is None:
        return {"stored": False, "reason": "logging_disabled"}

    # Create CSV for this device if first time in this session
    if dev_id not in device_files:
        csv_path = LOG_DIR / f"{session_id}_{dev_id}.csv"
        device_files[dev_id] = csv_path

        # Header uses keys from the FIRST sample
        first_sample = samples[0]
        header = ["server_time_utc"] + list(first_sample.keys())
        device_headers[dev_id] = header

        with csv_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(header)

    csv_path = device_files[dev_id]
    header = device_headers[dev_id]

    # Append all samples
    with csv_path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        for s in samples:
            server_time = datetime.utcnow().isoformat() + "Z"
            row = [server_time] + [s.get(col, "") for col in header[1:]]
            writer.writerow(row)

    return {"stored": True, "device_id": dev_id, "samples_stored": len(samples)}


# ------------ Simple HTML dashboard ------------
@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    html = """
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Sensor Logger Dashboard</title>
  <style>
    body { font-family: system-ui, sans-serif; max-width: 800px; margin: 40px auto; }
    button { padding: 8px 14px; margin-right: 8px; cursor: pointer; }
    pre { background: #f5f5f5; padding: 8px; border-radius: 4px; max-height: 280px; overflow: auto; }
  </style>
</head>
<body>
  <h1>Sensor Logger Dashboard</h1>
  <p>
    Arduino sends 10 Hz data in 2-minute bulk chunks to <code>/api/bulk_samples</code>.<br/>
    The server only logs data between Start and Stop.
  </p>

  <button onclick="startSession()">▶ Start logging</button>
  <button onclick="stopSession()">⏹ Stop & download ZIP</button>

  <h3>Status</h3>
  <pre id="statusBox">Loading...</pre>

<script>
async function startSession() {
  const res = await fetch('/api/start', { method: 'POST' });
  const data = await res.json();
  alert('Logging started. Session: ' + data.session_id);
  refreshStatus();
}

async function stopSession() {
  const res = await fetch('/api/stop', { method: 'POST' });
  if (!res.ok) {
    const txt = await res.text();
    alert('Stop failed: ' + txt);
    return;
  }
  const blob = await res.blob();
  const url = window.URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = 'sensor_logs.zip';
  document.body.appendChild(a);
  a.click();
  a.remove();
  window.URL.revokeObjectURL(url);
  refreshStatus();
}

async function refreshStatus() {
  const res = await fetch('/api/status');
  const data = await res.json();
  document.getElementById('statusBox').textContent = JSON.stringify(data, null, 2);
}
setInterval(refreshStatus, 2000);
refreshStatus();
</script>
</body>
</html>
"""
    return HTMLResponse(content=html)
