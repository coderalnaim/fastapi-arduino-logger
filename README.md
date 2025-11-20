# IoT Data Logger: Server & Client Guide

This project consists of a **FastAPI Python Server** that acts as a central controller and storage for IoT devices, and an **Arduino Client** implementation guide.

## 📂 Reference Code

**⚠️ Note:** A complete, working reference implementation for an **Arduino Uno R4 WiFi** connected to a **Time-of-Flight (ToF) sensor** is included in this project (see the provided `.ino` file). If the implementation details below seem abstract, please examine that file to see the logic in action.

-----

## 🖥️ Part 1: Python Server

The server provides a web dashboard to Start/Stop data recording sessions. When a session is active, it accepts JSON data from devices and saves them as CSV files. When stopped, it zips the data for download.

### Prerequisites

- Python 3.9+
- Git

### Installation

1. **Clone the repository**
   ```bash
   git clone git@github.com:coderalnaim/fastapi-arduino-logger.git
   ```

2. **Enter the project folder**
   ```bash
   cd fastapi-arduino-logger
   ```

3. **Install dependencies from `requirements.txt`**
   ```bash
   pip install -r requirements.txt
   ```

### Running the Server

To allow the Arduino to connect to your PC, you must run the server on `0.0.0.0` (listening on all network interfaces), not just localhost.

```bash
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

### Using the Dashboard

1.  Open your browser and go to `http://localhost:8000/dashboard`.
2.  **Click "Start Logging":** The server creates a session ID and a timestamp (`start_epoch`). It is now listening for data.
3.  **Recording:** Your Arduino devices will detect the start signal and begin streaming data.
4.  **Click "Stop & Download":** The server stops the session, zips all generated CSV files, and downloads them to your computer.

### ⚡ Quick API Reference

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `GET` | `/api/config?device_id=X` | Returns logging status (`true`/`false`) and `start_epoch`. |
| `POST` | `/api/bulk_samples` | Accepts a JSON list of samples to save to CSV. |
| `POST` | `/api/start` | Starts a new recording session. |
| `GET` | `/api/stop` | Stops recording and downloads the ZIP. |

-----

## 🤖 Part 2: Arduino Implementation Guide

To successfully communicate with this server, your Arduino code must follow a specific **"Poll & Push"** architecture. The Arduino is passive; it only records data when the Server tells it to.

### 1. The Logic Flow

Do not hardcode the recording start time in the Arduino. Instead:

1.  **Poll** the server config endpoint (`/api/config`) every few seconds.
2.  **Check** if `logging` is true and if a `start_epoch` (Unix timestamp) exists.
3.  **Sync Time:** Use the server's `start_epoch` as your baseline time.
4.  **Buffer & Send:** Collect samples in RAM and send them in batches (Bulk Upload) to save network overhead.

### 2. Essential Configuration

You need to point your Arduino to your PC's Local IP address.

```cpp
#include <WiFiS3.h> // Use WiFiS3 for Uno R4, WiFiNINA for Nano 33 IoT

const char* WIFI_SSID = "Your_WiFi_Name";
const char* WIFI_PASS = "Your_WiFi_Password";

// FIND YOUR PC IP: Run 'ipconfig' (Windows) or 'ifconfig' (Mac/Linux)
const char* API_HOST = "192.168.X.X"; 
const int   API_PORT = 8000;

const char* DEVICE_ID = "tof_sensor_01"; // Unique name for this device
```

### 3. Handling Time (The `start_epoch`)

The server sends a Unix Timestamp (e.g., `1700000000`) when you click "Start".

* **Variable:** Store this in `unsigned long baseEpoch`.
* **Calculation:** The timestamp for a specific data point is:
    `Timestamp = baseEpoch + (millis() - startMillis) / 1000`

This ensures all devices are synchronized to the Server's time, not their internal clocks.

### 4. Data Buffering (Batching)

Sending an HTTP request for every single sensor reading is too slow. You must buffer data.

**Define your Struct (Example based on provided ToF code):**

```cpp
typedef struct {
  char timestamp[24]; // ISO string "YYYY-MM-DDTHH:MM:SSZ"
  uint32_t sensor_time_ms;
  float distance_m;   // Distance in meters
  uint16_t signal;    // Signal strength
  int status;
} Sample;

const int MAX_SAMPLES = 10; // Adjust based on available RAM
Sample samples[MAX_SAMPLES];
```

### 5. Constructing the JSON Payload

The server expects a JSON object containing a `device_id` and a list of `samples`. Since standard Arduino JSON libraries can use too much memory, it is often better to build the string manually:

**Expected JSON Format:**

```json
{
  "device_id": "tof_sensor_01",
  "samples": [
    { "timestamp_utc": "2023-10-27T10:00:01Z", "distance_m": 1.25, "signal": 200 },
    { "timestamp_utc": "2023-10-27T10:00:02Z", "distance_m": 1.28, "signal": 195 }
  ]
}
```

**Arduino Code Snippet for Sending:**

```cpp
void sendBulk() {
  if (!client.connect(API_HOST, API_PORT)) return;

  // Start JSON
  String body = "{\"device_id\":\"" + String(DEVICE_ID) + "\",\"samples\":[";

  // Loop through buffer and append samples
  for (int i = 0; i < sample_count; i++) {
    if (i > 0) body += ","; 
    body += "{\"timestamp_utc\":\"" + String(samples[i].timestamp) + "\",";
    body += "\"distance_m\":" + String(samples[i].distance_m, 3) + ",";
    body += "\"signal\":" + String(samples[i].signal) + "}";
  }
  body += "]}"; 

  // Send HTTP POST
  client.println("POST /api/bulk_samples HTTP/1.1");
  client.println("Host: " + String(API_HOST));
  client.println("Content-Type: application/json");
  client.print("Content-Length: ");
  client.println(body.length());
  client.println(); 
  client.print(body); 

  sample_count = 0;
  client.stop();
}
```

### 6. The Main Loop Strategy

Your `loop()` should look like this to ensure stability:

1.  **Read Sensors:** Always read sensors to keep them warm/active.
2.  **Poll Config (Every 2s):** Ask server: "Are we recording?"
    * If Server says `0` (Stop): Set `haveBaseEpoch = false`.
    * If Server says `> 0` (Start): Set `haveBaseEpoch = true` and save the time.
3.  **Check State:** If `!haveBaseEpoch`, return (do nothing else).
4.  **Sample Timer (e.g., 10Hz):** If it's time, add data to `samples[]` array.
5.  **Upload Timer (e.g., 1s):** If buffer is full OR 1 second passed, call `sendBulk()`.

### Troubleshooting

* **Connection Failed:** Check your Firewall. You might need to allow Python/Uvicorn through Windows Firewall on Private/Public networks.
* **WiFi Issues:** Ensure the Arduino is connected to a 2.4GHz WiFi network (many Arduinos do not support 5GHz).
* **JSON Errors:** Ensure you aren't sending trailing commas in the JSON list.

* **Memory Issues:** If the Arduino crashes, reduce `MAX_SAMPLES` or use `F()` macro for static strings if using AVR boards.

