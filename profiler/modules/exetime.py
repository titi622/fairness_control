import sqlite3
import time
from typing import Dict, Optional


def now_us() -> int:
    return int(time.time() * 1_000_000)


def compute(
    db_path: str,
    window_sec: int = 60,
) -> Dict[str, Optional[float]]:

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    try:
        # 1) service 목록
        cur.execute("SELECT DISTINCT service FROM service_profile;")
        services = [r[0] for r in cur.fetchall()]

        # 2) window 계산
        window_end_us = now_us()
        window_start_us = window_end_us - window_sec * 1_000_000

        # 3) 평균 실행시간 계산
        cur.execute(
            """
            SELECT
                service,
                AVG(duration_ms) AS avg_execute_ms
            FROM traces
            WHERE start_time_us BETWEEN ? AND ?
            GROUP BY service;
            """,
            (window_start_us, window_end_us),
        )

        avg_map = {svc: avg for (svc, avg) in cur.fetchall()}

        # 4) 결과 정리 (service_profile 기준)
        result: Dict[str, Optional[float]] = {}
        for svc in services:
            avg = avg_map.get(svc)  # 없으면 None
            result[svc] = avg

        return result

    finally:
        conn.close()
