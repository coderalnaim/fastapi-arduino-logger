from fastapi import FastAPI
from pydantic import BaseModel
from datetime import datetime
from pathlib import Path
import csv

app = FastAPI(title="TOF / RTK Sensor Logging API")

# Folder where CSV logs are stored
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)


class Measurement(BaseModel):
    device_id: str
    system_time_ms: int
    distance_m: float
    status: int
    signal: int
    precision_cm: int


class ControlRequest(BaseModel):
    device_id: str
    enabled: bool


# In-memory state:
#   device_id -> {"enabled": bool, "file_path": Path | None}
device_state = {}


def create_new_log_file(device_id: str) -> Path:
    """Create a new CSV file for a recording session of this device."""
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    file_path = LOG_DIR / f"{device_id}_{timestamp}.csv"

    with file_path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "server_time_utc",
                "device_id",
                "system_time_ms",
                "distance_m",
                "status",
                "signal",
                "precision_cm",
            ]
        )
    return file_path


@app.get("/")
def root():
    """Simple info page."""
    return {
        "message": "TOF / RTK Sensor Logging API",
        "endpoints": {
            "POST /api/control": "Enable/disable logging per device_id",
            "POST /api/measurement": "Send sensor reading JSON (10 Hz allowed)",
            "GET  /api/status": "See which devices are logging and log file names",
        },
    }


@app.get("/api/status")
def get_status():
    """Return current logging state for all devices."""
    result = {}
    for dev_id, state in device_state.items():
        result[dev_id] = {
            "enabled": state.get("enabled", False),
            "file_path": str(state.get("file_path")) if state.get("file_path") else None,
        }
    return result


@app.post("/api/control")
def set_logging(req: ControlRequest):
    """
    Enable or disable logging for a given device_id.

    - When enabled=True and previously disabled:
        -> create new CSV file and start logging to it.
    - When enabled=False:
        -> stop logging but keep file on disk.
    """
    state = device_state.get(req.device_id, {"enabled": False, "file_path": None})

    if req.enabled and not state["enabled"]:
        # Turning ON: open a new CSV file for this session
        file_path = create_new_log_file(req.device_id)
        state["file_path"] = file_path
        state["enabled"] = True
    elif not req.enabled and state["enabled"]:
        # Turning OFF
        state["enabled"] = False

    device_state[req.device_id] = state

    return {
        "device_id": req.device_id,
        "enabled": state["enabled"],
        "file_path": str(state["file_path"]) if state["file_path"] else None,
    }


@app.post("/api/measurement")
def add_measurement(m: Measurement):
    """
    Receive one measurement.
    Devices can send at 10 Hz all the time.
    Data is only stored when logging is enabled for that device_id.
    """
    state = device_state.get(m.device_id, {"enabled": False, "file_path": None})

    # If logging not enabled for this device → ignore but don't error
    if not state["enabled"] or state["file_path"] is None:
        return {"stored": False, "reason": "logging_disabled"}

    server_time = datetime.utcnow().isoformat()

    row = [
        server_time,
        m.device_id,
        m.system_time_ms,
        m.distance_m,
        m.status,
        m.signal,
        m.precision_cm,
    ]

    with state["file_path"].open("a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(row)

    return {"stored": True}
