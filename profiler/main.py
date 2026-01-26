import sqlite3
import time

from typing import Optional, Dict, Any
import modules.exetime as exetime
import modules.reqcnt as reqcnt

DB_PATH = "../trace_store.db"

# ----------------------------
# DB 생성
# ----------------------------
conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

cur.executescript("""
CREATE TABLE IF NOT EXISTS service_profile (
  service           TEXT    NOT NULL,
  creation_time     INTEGER NOT NULL,

  t_warm            REAL,
  t_cold            REAL,
  t_execute         REAL,

  weight            INTEGER,
  qos               REAL,

  max_container     INTEGER,
  min_container     INTEGER,
  active_container  INTEGER,

  request_cnt       INTEGER,

  PRIMARY KEY (service)
);

CREATE INDEX IF NOT EXISTS idx_service_profile
  ON service_profile (service);
""")
conn.commit()

def now_us() -> int:
    return int(time.time() * 1_000_000)

creation_time_us = now_us()

# ----------------------------
# init profile
# ----------------------------
cur.execute("""
    INSERT OR IGNORE INTO service_profile
    (service, creation_time, weight, t_warm, t_cold)
    VALUES (?, ?, ?, ?,?)
""", ("hello",creation_time_us,1,1623.653,3.308))
conn.commit()


cur.execute("""
SELECT * FROM service_profile;
""")
print(cur.fetchall())

t_execute_map = exetime.compute(
    db_path=DB_PATH,
    window_sec=60,
)

print(t_execute_map)

request_cnt = reqcnt.compute(
    db_path=DB_PATH,
    window_sec=300,
    split_sec=60
)

print(request_cnt)

conn.close()