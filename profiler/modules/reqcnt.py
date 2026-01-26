import sqlite3
import time
from typing import Dict, Optional


def now_us() -> int:
    return int(time.time() * 1_000_000)


def compute(
    db_path: str,
    window_sec: int,
    split_sec: int,
) -> Dict[str, int]:
    """
    윈도우(window_sec) 동안 split_sec 단위로 요청 수를 bucketize 해서,
    bucket별 요청 수의 최대값(max)을 서비스별로 반환한다.

    예: window=300s, split=60s -> 5개 버킷(1분 단위) 중 최대 요청 수

    Returns:
      { service: max_requests_in_any_split_bucket }  # 데이터 없으면 0
    """

    if window_sec <= 0:
        raise ValueError("window_sec must be > 0")
    if split_sec <= 0:
        raise ValueError("split_sec must be > 0")
    if split_sec > window_sec:
        # split이 window보다 크면 사실상 버킷이 1개이므로 max=count와 동일
        # 허용은 가능하나, 의도와 다를 수 있어 명확히 처리
        split_sec = window_sec

    window_end_us = now_us()
    window_start_us = window_end_us - window_sec * 1_000_000
    split_us = split_sec * 1_000_000

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    try:
        # 1) service 목록: service_profile 기준 (요구사항 유지)
        cur.execute("SELECT DISTINCT service FROM service_profile;")
        services = [r[0] for r in cur.fetchall()]

        # 2) 서비스별, 버킷별 count를 구한 뒤, 그 max를 서비스별로 집계
        # bucket_id = (event_time_us - window_start_us) / split_us  (정수 나눗셈)
        # cur.execute(
        #     """
        #     SELECT
        #         service,
        #         ((start_time_us - ?) / ?) AS bucket_id,
        #         COUNT(*) AS cnt
        #     FROM traces
        #     WHERE start_time_us BETWEEN ? AND ?
        #     GROUP BY service, bucket_id
        #     """,
        #     (window_start_us, split_us, window_start_us, window_end_us),
        # )

        # rows = cur.fetchall()
        # for r in rows:
        #     print(r)


        cur.execute(
            """
            WITH bucket_counts AS (
                SELECT
                    service,
                    ((start_time_us - ?) / ?) AS bucket_id,
                    COUNT(*) AS cnt
                FROM traces
                WHERE start_time_us BETWEEN ? AND ?
                GROUP BY service, bucket_id
            )
            SELECT
                service,
                MAX(cnt) AS max_cnt
            FROM bucket_counts
            GROUP BY service;
            """,
            (window_start_us, split_us, window_start_us, window_end_us),
        )

        max_map = {svc: int(mx) for (svc, mx) in cur.fetchall()}

        # 3) service_profile 기준으로 결과 정리 (데이터 없는 서비스는 0)
        result: Dict[str, int] = {}
        for svc in services:
            result[svc] = max_map.get(svc, 0)

        return result

    finally:
        conn.close()