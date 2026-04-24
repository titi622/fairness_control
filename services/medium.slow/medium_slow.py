import os
import time
from flask import Flask, jsonify, request

COLD_SLEEP_SEC = float(os.getenv("COLD_SLEEP_SEC", "3"))
REQ_SLEEP_SEC = float(os.getenv("REQ_SLEEP_SEC", "3"))  # ✅ 요청마다 지연

t0 = time.time()
time.sleep(COLD_SLEEP_SEC)  # 컨테이너 프로세스 시작 시 1회만 지연
startup_ms = (time.time() - t0) * 1000.0

app = Flask(__name__)

@app.get("/")
def root():

    if REQ_SLEEP_SEC > 0:
        time.sleep(REQ_SLEEP_SEC)
    # 헬스/관측용으로 몇 가지 반환
    return jsonify({
        "ok": True,
        "startup_ms": round(startup_ms, 1),
        "cold_sleep_sec": COLD_SLEEP_SEC,
        "pid": os.getpid(),
        "ts": int(time.time())
    })

@app.get("/healthz")
def healthz():
    return "ok", 200
