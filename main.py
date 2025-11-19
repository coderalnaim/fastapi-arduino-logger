from fastapi import FastAPI, HTTPException, Body
from fastapi.responses import HTMLResponse, StreamingResponse
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
import csv
import io
import zipfile
import re

app = FastAPI(title="Universal Sensor Logging API")

# Directory for temporary CSVs
LOG_DIR = Path("session_logs")
LOG_DIR.mkdir(exist_ok=True)

# Global session state
logging_enabled: bool = False
session_id: Optional[str] = None
device_files: Dict[str, Path] = {}      # device_id -> csv path
device_headers: Dict[str, List[str]] = {}  # device_id -> header list

# Very simple ISO8601-with-Z check
ISO_REGEX = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$")


def start_session():
  global logging_enabled, session_id, device_files, device_headers
  logging_enabled = True
  session_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
  device_files = {}
  device_headers = {}


def stop_session_and_zip() -> bytes:
  """
  Stop logging, build a ZIP of all device CSVs, delete them, and return ZIP bytes.
  """
  global logging_enabled, session_id, device_files, device_headers
  logging_enabled = False

  buf = io.BytesIO()
  with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
    for dev_id, path in device_files.items():
      if path.exists():
        zf.write(path, arcname=path.name)

  buf.seek(0)

  # Cleanup local CSV files
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
    "message": "Universal Sensor Logging API",
    "description": "Click /dashboard to start/stop a global logging session.",
    "endpoints": {
      "GET  /dashboard": "Start/Stop buttons + live status",
      "POST /api/start": "Start global logging session",
      "POST /api/stop": "Stop logging and download ZIP of CSVs",
      "POST /api/measurement": "Devices send JSON here (10 Hz ok)",
      "GET  /api/status": "Current logging state and known devices",
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
  Start a new global logging session.
  Existing temp files (if any) are forgotten and a new session_id is created.
  """
  if logging_enabled:
    return {"logging_enabled": True, "session_id": session_id, "note": "already running"}

  start_session()
  return {"logging_enabled": True, "session_id": session_id}


@app.post("/api/stop")
def api_stop():
  """
  Stop logging, bundle all CSVs into a ZIP, delete them, and return ZIP for download.
  """
  if not logging_enabled and not device_files:
    raise HTTPException(400, "No active session or files to download")

  zip_bytes = stop_session_and_zip()
  filename = f"sensor_logs_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.zip"

  return StreamingResponse(
    io.BytesIO(zip_bytes),
    media_type="application/zip",
    headers={"Content-Disposition": f'attachment; filename="{filename}"'},
  )


@app.post("/api/measurement")
def api_measurement(payload: dict = Body(...)):
  """
  Devices send JSON here (10 Hz).

  Required fields in JSON:
    - device_id (string)
    - timestamp_utc (ISO8601, e.g. 2025-10-28T12:34:56.123Z)
    - sensor_time_ms (int)

  Any other keys are logged as additional columns.
  """
  # Basic required fields
  if "device_id" not in payload:
    raise HTTPException(400, "Missing 'device_id'")
  if "timestamp_utc" not in payload:
    raise HTTPException(400, "Missing 'timestamp_utc'")
  if "sensor_time_ms" not in payload:
    raise HTTPException(400, "Missing 'sensor_time_ms'")

  device_id = str(payload["device_id"])
  timestamp_utc = str(payload["timestamp_utc"])

  # Optional validation of timestamp format
  if not ISO_REGEX.match(timestamp_utc):
    raise HTTPException(400, "timestamp_utc must be ISO8601 like 2025-10-28T12:34:56.123Z")

  # If global logging is OFF, ignore
  if not logging_enabled or session_id is None:
    return {"stored": False, "reason": "logging_disabled"}

  # Create CSV for this device if first time in this session
  if device_id not in device_files:
    csv_path = LOG_DIR / f"{session_id}_{device_id}.csv"
    device_files[device_id] = csv_path

    # Header: server_time_utc + all keys in payload (in current order)
    header = ["server_time_utc"] + list(payload.keys())
    device_headers[device_id] = header

    with csv_path.open("w", newline="", encoding="utf-8") as f:
      writer = csv.writer(f)
      writer.writerow(header)

  csv_path = device_files[device_id]
  header = device_headers[device_id]

  # Build row in same order as header
  server_time = datetime.utcnow().isoformat() + "Z"
  row = [server_time] + [payload.get(col, "") for col in header[1:]]

  with csv_path.open("a", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(row)

  return {"stored": True}


# ------------- Simple dashboard (Start / Stop buttons) -------------
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
    UNO / sensors always send data to <code>/api/measurement</code>.<br/>
    The server only <strong>stores</strong> it between Start and Stop.
  </p>

  <button onclick="startSession()">▶ Start logging</button>
  <button onclick="stopSession()">⏹ Stop & download ZIP</button>

  <h3>Status</h3>
  <pre id="statusBox">Loading...</pre>

<script>
async function startSession() {
  const res = await fetch('/api/start', { method: 'POST' });
  const data = await res.json();
  alert('Logging started, session: ' + data.session_id);
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
