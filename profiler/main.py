import sqlite3
import time

import modules.exetime as exetime
import modules.reqcnt as reqcnt
import modules.qos as qos
import modules.minmax as minmax

DB_PATH = "/home/ubuntu/fairness_control/trace_store.db"
INTERVAL_SEC = 20


def now_us() -> int:
    return int(time.time() * 1_000_000)


def init_db(conn: sqlite3.Connection) -> None:
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


def seed_profile(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    creation_time_us = now_us()

    # 예시: hello 서비스 1개 시드
    cur.execute("""
        INSERT OR REPLACE INTO service_profile
        (service, creation_time, weight, t_warm, t_cold)
        VALUES (?, ?, ?, ?, ?)
    """, ("hello", creation_time_us, 1, 3.308, 1623.653))
    conn.commit()


def run_once(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()

    # (옵션) 현재 테이블 확인
    cur.execute("SELECT * FROM service_profile;")
    print("[before]", cur.fetchall())

    # 1) t_execute 계산
    t_execute_map = exetime.compute(
        db_path=DB_PATH,
        window_sec=60,
    )
    print("[t_execute_map]", t_execute_map)

    # 2) request_cnt 계산
    request_cnt_map = reqcnt.compute(
        db_path=DB_PATH,
        window_sec=60,
        split_sec=20,
    )
    print("[request_cnt_map]", request_cnt_map)

    # 3) service_profile 업데이트
    update_sql = """
        UPDATE service_profile
        SET
            t_execute = ?,
            request_cnt = ?
        WHERE
            service = ?;
    """

    services = set(t_execute_map.keys()) | set(request_cnt_map.keys())
    for svc in services:
        t_execute = t_execute_map.get(svc)
        request_cnt = request_cnt_map.get(svc)

        # None → 0 치환
        t_execute = t_execute if t_execute is not None else 0.0
        request_cnt = request_cnt if request_cnt is not None else 0

        cur.execute(update_sql, (t_execute, request_cnt, svc))

    conn.commit()

    cur.execute("SELECT * FROM service_profile;")
    print("[after t_execute/request_cnt]", cur.fetchall())

    # 4) QoS 업데이트
    qos.update_qos(DB_PATH)
    cur.execute("SELECT * FROM service_profile;")
    print("[after qos]", cur.fetchall())

    # 5) min/max 컨테이너 업데이트
    minmax.update_minmax(
        db_path=DB_PATH,
        window_sec=60,
        split_sec=20,
    )
    cur.execute("SELECT * FROM service_profile;")
    print("[after minmax]", cur.fetchall())


def main() -> None:
    conn = sqlite3.connect(DB_PATH, timeout=5)
    try:
        init_db(conn)
        seed_profile(conn)

        print(f"Start loop: every {INTERVAL_SEC}s (Ctrl+C to stop)")

        while True:
            run_once(conn)

            time.sleep(INTERVAL_SEC)

    except KeyboardInterrupt:
        print("\nKeyboardInterrupt received. Closing DB connection...")

    finally:
        try:
            conn.close()
            print("DB connection closed. Bye.")
        except Exception as e:
            print(f"Error while closing DB: {e}")


if __name__ == "__main__":
    main()
