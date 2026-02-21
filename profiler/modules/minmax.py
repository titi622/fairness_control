import sqlite3
import time
from typing import Dict, Optional


def update_minmax(
    db_path: str,
    window_sec: int,
    split_sec: int,
) -> int:

    conn = sqlite3.connect(db_path, timeout=5)
    cur = conn.cursor()

    try:
        # 0) compute window size
        window = round(window_sec / split_sec)
        # 1) 모든 서비스의 평균 qos (Q_all)
        cur.execute("""
            SELECT AVG(qos)
            FROM service_profile
            WHERE t_execute IS NOT NULL AND t_execute > 0
            AND qos IS NOT NULL;
        """)
        row = cur.fetchone()
        avg_qos_all = row[0] if row and row[0] is not None else 0.0

        # 분모 보호
        if avg_qos_all <= 0:
            print("Error: qos is 0")
            return 0

        # 2) 서비스별 필요한 값 읽기
        cur.execute("""
            SELECT
                service,
                request_cnt,
                t_warm,
                t_cold,
                weight
            FROM service_profile
        """)

        results: Dict[str, int] = {}

        for (
            service,
            request_cnt,
            t_warm,
            t_cold,
            weight,
        ) in cur.fetchall():

            # 분자
            numerator = 0.5 * request_cnt * t_warm

            # 분모
            denominator = (
                avg_qos_all* 100 * weight
                - (t_cold / window)
                - (t_warm / 2.0)
            )

            if denominator <= 0:
                print(f"Error: denominator <= 0 for service={service} (den={denominator})")
                return 0        
            else:
                max_container = math.ceil(numerator / denominator)

            results[service] = max_container
        
        # 3) DB 업데이트 (서비스별로 max_container 저장)
        cur.executemany(
            """
            UPDATE service_profile
            SET max_container = ?
            WHERE service = ?
            """,
            [(mc, svc) for svc, mc in results.items()]
        )
        conn.commit()

        return 1

    finally:
        conn.close()