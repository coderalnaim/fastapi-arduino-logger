#include <WiFiS3.h>

// ---------- WIFI ----------
char WIFI_SSID[] = "test";
char WIFI_PASS[] = "12345678";

// ---------- API ----------
const char* API_HOST = "10.221.x.x";   // <-- your PC's LAN IP
const int   API_PORT = 8000;
const char* DEVICE_ID = "tof_01";

// Separate clients for config + upload
WiFiClient configClient;
WiFiClient uploadClient;

// ---------- TIME (from server start_epoch) ----------
unsigned long baseEpoch   = 0;  // seconds since 1970, given by server
unsigned long startMillis = 0;  // millis() when experiment started
bool          haveBaseEpoch = false;

// ---------- CONFIG POLLING ----------
unsigned long lastConfigPoll = 0;
const unsigned long CONFIG_POLL_INTERVAL_MS = 2000;

// ---------- TOF SENSOR ----------
#define TOF_FRAME_HEADER  0x57
#define TOF_FUNCTION_MARK 0x00

typedef struct {
  char     timestamp[24];   // "YYYY-MM-DDTHH:MM:SSZ"
  uint32_t sensor_time_ms;
  uint16_t distance_mm;
  uint16_t signal;
  uint8_t  status;
  uint8_t  precision_cm;
} Sample;

typedef struct {
  uint8_t  id;
  uint32_t sensor_time_ms;
  float    distance_m;
  uint8_t  status;
  uint16_t signal;
  uint8_t  precision_cm;
} TOF_Data;

TOF_Data tof;

uint8_t rx_buf[32];
uint8_t rx_idx = 0;

// ---------- BUFFER: now only 10 samples max ----------
const int MAX_SAMPLES = 10;
Sample samples[MAX_SAMPLES];
int sample_count = 0;

unsigned long lastSampleMs   = 0;
const unsigned long SAMPLE_INTERVAL_MS = 100;   // 10Hz

unsigned long chunkStartMs   = 0;
// send roughly every second
const unsigned long CHUNK_INTERVAL_MS  = 1000; // 1s

// =============================================================
// HELPER: millis-based UTC timestamp using baseEpoch
// =============================================================
String makeTimestamp() {
  if (!haveBaseEpoch) {
    return "1970-01-01T00:00:00Z";
  }

  unsigned long elapsed = (millis() - startMillis) / 1000;
  unsigned long epoch   = baseEpoch + elapsed;

  int sec = epoch % 60; epoch /= 60;
  int min = epoch % 60; epoch /= 60;
  int hr  = epoch % 24; epoch /= 24;

  int days = epoch;
  int year = 1970;

  while (true) {
    int daysInYear = (year % 4 == 0) ? 366 : 365;
    if (days < daysInYear) break;
    days -= daysInYear;
    year++;
  }

  const int mdays[] = {31,28,31,30,31,30,31,31,30,31,30,31};
  int month = 0;
  for (int i = 0; i < 12; i++) {
    int dim = mdays[i];
    if (i == 1 && (year % 4 == 0)) dim++;
    if (days < dim) { month = i + 1; break; }
    days -= dim;
  }
  int day = days + 1;

  char buf[24];
  sprintf(buf, "%04d-%02d-%02dT%02d:%02d:%02dZ",
          year, month, day, hr, min, sec);
  return String(buf);
}

// =============================================================
// WIFI
// =============================================================
void connectWiFi() {
  Serial.print("[WiFi] Connecting");
  WiFi.begin(WIFI_SSID, WIFI_PASS);

  unsigned long startAttempt = millis();
  while (WiFi.status() != WL_CONNECTED &&
         millis() - startAttempt < 15000) {
    Serial.print(".");
    delay(500);
  }

  if (WiFi.status() == WL_CONNECTED) {
    Serial.println("\n[WiFi] Connected.");
    Serial.print("[WiFi] IP: ");
    Serial.println(WiFi.localIP());
  } else {
    Serial.println("\n[WiFi] FAILED. Will keep trying in background.");
  }
}

// =============================================================
// TOF PARSER
// =============================================================
void parseTOF() {
  tof.id = rx_buf[3];

  tof.sensor_time_ms =
    ((uint32_t)rx_buf[7] << 24) |
    ((uint32_t)rx_buf[6] << 16) |
    ((uint32_t)rx_buf[5] << 8)  |
    (uint32_t)rx_buf[4];

  int32_t raw =
    ((uint32_t)rx_buf[10] << 24) |
    ((uint32_t)rx_buf[9]  << 16) |
    ((uint32_t)rx_buf[8]  << 8);
  raw /= 256;

  tof.distance_m   = raw / 1000.0f;
  tof.status       = rx_buf[11];
  tof.signal       = ((uint16_t)rx_buf[13] << 8) | rx_buf[12];
  tof.precision_cm = rx_buf[14];
}

void readTOF() {
  while (Serial1.available()) {
    uint8_t b = Serial1.read();

    if (rx_idx == 0 && b != TOF_FRAME_HEADER) {
      // keep discarding until header appears
      continue;
    }

    rx_buf[rx_idx++] = b;

    if (rx_idx >= 16) {
      rx_idx = 0;
      uint8_t sum = 0;
      for (int i = 0; i < 15; i++) sum += rx_buf[i];
      if (sum == rx_buf[15]) {
        parseTOF();
      }
    }
  }
}

// =============================================================
// BULK UPLOAD (now small payloads)
// =============================================================
void sendBulk() {
  if (sample_count == 0) return;

  Serial.print("[UPLOAD] Attempt, sample_count=");
  Serial.println(sample_count);

  if (WiFi.status() != WL_CONNECTED) {
    Serial.println("[UPLOAD] No WiFi, trying reconnect...");
    connectWiFi();
    if (WiFi.status() != WL_CONNECTED) {
      Serial.println("[UPLOAD] Still no WiFi, KEEPING buffer.");
      return; // keep samples in RAM
    }
  }

  if (uploadClient.connected()) {
    uploadClient.stop();
  }

  Serial.println("[UPLOAD] Connecting to server...");
  if (!uploadClient.connect(API_HOST, API_PORT)) {
    Serial.println("[UPLOAD] connect() FAILED, KEEPING buffer.");
    return; // do not clear samples
  }

  // Build JSON body
  String body;
  body.reserve(1024);  // small now
  body  = "{";
  body += "\"device_id\":\""; body += DEVICE_ID; body += "\",";
  body += "\"samples\":[";

  for (int i = 0; i < sample_count; i++) {
    if (i > 0) body += ",";
    body += "{";
    body += "\"timestamp_utc\":\""; body += samples[i].timestamp; body += "\",";
    body += "\"sensor_time_ms\":";  body += samples[i].sensor_time_ms; body += ",";
    body += "\"distance_m\":";      body += String(samples[i].distance_mm / 1000.0f, 3); body += ",";
    body += "\"status\":";          body += samples[i].status; body += ",";
    body += "\"signal\":";          body += samples[i].signal; body += ",";
    body += "\"precision_cm\":";    body += samples[i].precision_cm;
    body += "}";
  }

  body += "]}";

  Serial.print("[UPLOAD] body length = ");
  Serial.println(body.length());

  String req =
    String("POST /api/bulk_samples HTTP/1.1\r\n") +
    "Host: " + API_HOST + "\r\n" +
    "Content-Type: application/json\r\n" +
    "Connection: close\r\n" +
    "Content-Length: " + body.length() + "\r\n\r\n" +
    body + "\r\n";

  uploadClient.print(req);
  Serial.println("[UPLOAD] HTTP request sent, waiting for response...");

  // Read first response line (if any)
  unsigned long t0 = millis();
  String responseLine;
  while (uploadClient.connected() && millis() - t0 < 1000) {
    while (uploadClient.available()) {
      char c = uploadClient.read();
      if (c == '\n') {
        Serial.print("[UPLOAD] First response line: ");
        Serial.println(responseLine);
        t0 = millis();
        goto doneReading;
      } else if (c != '\r') {
        responseLine += c;
      }
      t0 = millis();
    }
  }

doneReading:
  uploadClient.stop();

  Serial.println("[UPLOAD] Done, clearing buffer.");
  sample_count = 0;
  chunkStartMs = millis();
}

// =============================================================
// CONFIG POLL: used to track start/stop
// =============================================================
void pollConfig() {
  if (WiFi.status() != WL_CONNECTED) {
    connectWiFi();
    if (WiFi.status() != WL_CONNECTED) {
      Serial.println("[CONFIG] No WiFi, skip.");
      return;
    }
  }

  if (configClient.connected()) {
    configClient.stop();
  }

  if (!configClient.connect(API_HOST, API_PORT)) {
    Serial.println("[CONFIG] connect failed");
    return;
  }

  String req =
    String("GET /api/config?device_id=") + DEVICE_ID +
    " HTTP/1.1\r\nHost: " + API_HOST + "\r\nConnection: close\r\n\r\n";

  configClient.print(req);

  String resp;
  unsigned long t0 = millis();
  while (configClient.connected() || configClient.available()) {
    if (configClient.available()) {
      char c = configClient.read();
      resp += c;
      t0 = millis();
    }
    if (millis() - t0 > 2000) break;
  }
  configClient.stop();

  int jsonStart = resp.indexOf('{');
  int jsonEnd   = resp.lastIndexOf('}');
  if (jsonStart < 0 || jsonEnd <= jsonStart) {
    Serial.println("[CONFIG] No JSON found");
    return;
  }

  String json = resp.substring(jsonStart, jsonEnd + 1);
  Serial.print("[CONFIG] JSON: ");
  Serial.println(json);

  // Parse start_epoch
  int idx = json.indexOf("\"start_epoch\":");
  if (idx < 0) {
    Serial.println("[CONFIG] start_epoch not present.");
    return;
  }

  int colon = json.indexOf(":", idx);
  if (colon < 0) {
    Serial.println("[CONFIG] Malformed start_epoch");
    return;
  }

  int i = colon + 1;
  while (i < (int)json.length() && (json[i] == ' ' || json[i] == '\"')) i++;
  String numStr;
  while (i < (int)json.length() && isDigit(json[i])) {
    numStr += json[i++];
  }

  if (numStr.length() == 0) {
    Serial.println("[CONFIG] start_epoch empty");
    return;
  }

  unsigned long epochVal = numStr.toInt();

  // ========== STOP ==========
  if (epochVal == 0) {
    if (haveBaseEpoch) {
      Serial.println("[CONFIG] Server STOPPED logging. Flushing buffer and stopping.");
      sendBulk();         // flush last tiny chunk
      haveBaseEpoch = false;
      // sample_count already cleared if upload ok
    }
    return;
  }

  // ========== START / RE-START ==========
  if (!haveBaseEpoch || baseEpoch != epochVal) {
    baseEpoch   = epochVal;
    startMillis = millis();
    haveBaseEpoch = true;

    sample_count = 0;
    chunkStartMs = millis();
    lastSampleMs = millis();

    Serial.print("[CONFIG] Got new start_epoch = ");
    Serial.println(baseEpoch);
  }
}

// =============================================================
// SETUP
// =============================================================
void setup() {
  Serial.begin(115200);
  Serial1.begin(115200);

  connectWiFi();

  Serial.println("[SETUP] Waiting for start_epoch from server.");
  lastConfigPoll = millis();
  lastSampleMs   = millis();
  chunkStartMs   = millis();
}

// =============================================================
// LOOP
// =============================================================
void loop() {
  unsigned long now = millis();

  // 1) Always keep sensor data updated
  readTOF();

  // 2) POLL CONFIG ALWAYS to react to start/stop
  if (now - lastConfigPoll >= CONFIG_POLL_INTERVAL_MS) {
    lastConfigPoll = now;
    pollConfig();
  }

  // If we don't have a baseEpoch, we do NOT sample or upload
  if (!haveBaseEpoch) {
    return;
  }

  // 3) 10 Hz sampling into buffer
  if (now - lastSampleMs >= SAMPLE_INTERVAL_MS) {
    lastSampleMs = now;

    if (sample_count < MAX_SAMPLES) {
      String ts = makeTimestamp();
      ts.toCharArray(samples[sample_count].timestamp, 24);

      samples[sample_count].sensor_time_ms = tof.sensor_time_ms;
      samples[sample_count].distance_mm    = (uint16_t)(tof.distance_m * 1000.0f + 0.5f);
      samples[sample_count].status         = tof.status;
      samples[sample_count].signal         = tof.signal;
      samples[sample_count].precision_cm   = tof.precision_cm;

      sample_count++;
    }
  }

  // 4) Send chunk every CHUNK_INTERVAL_MS or when buffer full
  if ((now - chunkStartMs >= CHUNK_INTERVAL_MS) || (sample_count >= MAX_SAMPLES)) {
    sendBulk();
  }
}
