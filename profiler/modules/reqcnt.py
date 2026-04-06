import sqlite3
import time
from typing import Dict


def now_us() -> int:
    # us 단위로 맞춤
    return time.time_ns() // 1_000


def select_twarm_us(conn: sqlite3.Connection) -> Dict[str, int]:
    """
    service_profile에서 서비스별 t_warm을 읽어 us로 반환.
    (t_warm은 ms)
    """
    cur = conn.cursor()
    cur.execute("SELECT service, t_warm FROM service_profile;")

    out: Dict[str, int] = {}
    for svc, t_warm in cur.fetchall():
        if svc is None or t_warm is None:
            continue
        out[str(svc)] = int(float(t_warm) * 1_000)  # ms -> us

    return out


def compute(db_path: str) -> Dict[str, int]:
    """
    현재시간 기준 [now-120s, now) 범위의 요청을 분석해서,
    서비스별 t_warm 이내에 들어온 요청의 '최대 동시 요청 수'를 반환.
    동시성은 (service, revision) 단위로 계산한 뒤 service로 max 집계.
    """
    window_end_us = now_us()
    window_start_us = window_end_us - 300 * 1_000_000  # 2 minutes

    conn = sqlite3.connect(db_path)
    try:
        twarm_us = select_twarm_us(conn)
        if not twarm_us:
            return {}

        max_warm_us = max(twarm_us.values(), default=0)

        # warm 윈도우 때문에 window_end + max_warm 까지 읽어야 함
        fetch_start = window_start_us
        fetch_end = window_end_us + max_warm_us

        cur = conn.cursor()

        # service 목록: service_profile 기준 
        cur.execute("SELECT DISTINCT service FROM service_profile;")
        services = [r[0] for r in cur.fetchall()]

        # service_profile 기준 서비스별 최대 concurrent를 전부 SQL로 계산
        cur.execute(
            """
            WITH
            window AS (
              SELECT
                ? AS start_us,
                ? AS end_us,
                ? AS max_warm_us
            ),
            t1 AS (
              SELECT
                tr.trace_id,
                tr.service,
                tr.start_time_us,
                CAST(sp.t_warm AS INTEGER) * 1000 AS warm_us  -- ms -> us
              FROM traces tr
              JOIN service_profile sp
                ON sp.service = tr.service
              JOIN window w
              WHERE tr.start_time_us >= w.start_us
                AND tr.start_time_us <  w.end_us
            ),
            t2 AS (
              SELECT
                tr.trace_id,
                tr.service,
                tr.start_time_us
              FROM traces tr
              JOIN window w
              WHERE tr.start_time_us >= w.start_us
                AND tr.start_time_us <  w.end_us + w.max_warm_us
            ),
            per_req AS (
              SELECT
                a.service,
                a.trace_id,
                COUNT(b.trace_id) AS concurrent_cnt
              FROM t1 a
              JOIN t2 b
                ON b.service = a.service
               AND b.start_time_us BETWEEN a.start_time_us
                                      AND a.start_time_us + a.warm_us
              GROUP BY a.service, a.trace_id
            )
            SELECT
              sp.service,
              COALESCE(MAX(pr.concurrent_cnt), 0) AS max_concurrent_cnt
            FROM service_profile sp
            LEFT JOIN per_req pr
              ON pr.service = sp.service
            GROUP BY sp.service
            ORDER BY max_concurrent_cnt DESC, sp.service;
            """,
            (fetch_start, window_end_us, max_warm_us),
        )

        max_map = {svc: int(mx) for (svc, mx) in cur.fetchall()}

        # 3) service_profile 기준으로 결과 정리 (데이터 없는 서비스는 0)
        result: Dict[str, int] = {}
        for svc in services:
            result[svc] = max_map.get(svc, 0)

        return result


    finally:
        conn.close()


# def compute(
#     db_path: str,
#     window_sec: int,
#     split_sec: int,
# ) -> Dict[str, int]:
#     """
#     윈도우(window_sec) 동안 split_sec 단위로 요청 수를 bucketize 해서,
#     bucket별 요청 수의 최대값(max)을 서비스별로 반환한다.

#     예: window=300s, split=60s -> 5개 버킷(1분 단위) 중 최대 요청 수

#     Returns:
#       { service: max_requests_in_any_split_bucket }  # 데이터 없으면 0
#     """

#     if window_sec <= 0:
#         raise ValueError("window_sec must be > 0")
#     if split_sec <= 0:
#         raise ValueError("split_sec must be > 0")
#     if split_sec > window_sec:
#         # split이 window보다 크면 사실상 버킷이 1개이므로 max=count와 동일
#         # 허용은 가능하나, 의도와 다를 수 있어 명확히 처리
#         split_sec = window_sec

#     window_end_us = now_us()
#     window_start_us = window_end_us - window_sec * 1_000_000
#     split_us = split_sec * 1_000_000

#     conn = sqlite3.connect(db_path)
#     cur = conn.cursor()

#     try:
#         # 1) service 목록: service_profile 기준 (요구사항 유지)
#         cur.execute("SELECT DISTINCT service FROM service_profile;")
#         services = [r[0] for r in cur.fetchall()]

#         # 2) 서비스별, 버킷별 count를 구한 뒤, 그 max를 서비스별로 집계
#         # bucket_id = (event_time_us - window_start_us) / split_us  (정수 나눗셈)
#         # cur.execute(
#         #     """
#         #     SELECT
#         #         service,
#         #         ((start_time_us - ?) / ?) AS bucket_id,
#         #         COUNT(*) AS cnt
#         #     FROM traces
#         #     WHERE start_time_us BETWEEN ? AND ?
#         #     GROUP BY service, bucket_id
#         #     """,
#         #     (window_start_us, split_us, window_start_us, window_end_us),
#         # )

#         # rows = cur.fetchall()
#         # for r in rows:
#         #     print(r)


#         cur.execute(
#             """
#             WITH bucket_counts AS (
#                 SELECT
#                     service,
#                     ((start_time_us - ?) / ?) AS bucket_id,
#                     COUNT(*) AS cnt
#                 FROM traces
#                 WHERE start_time_us BETWEEN ? AND ?
#                 GROUP BY service, bucket_id
#             )
#             SELECT
#                 service,
#                 MAX(cnt) AS max_cnt
#             FROM bucket_counts
#             GROUP BY service;
#             """,
#             (window_start_us, split_us, window_start_us, window_end_us),
#         )

#         max_map = {svc: int(mx) for (svc, mx) in cur.fetchall()}

#         # 3) service_profile 기준으로 결과 정리 (데이터 없는 서비스는 0)
#         result: Dict[str, int] = {}
#         for svc in services:
#             result[svc] = max_map.get(svc, 0)

#         return result

#     finally:
#         conn.close()