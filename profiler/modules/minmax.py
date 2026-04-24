import sqlite3
import time
from typing import Dict, Optional
import math


def update_minmax(
    db_path: str,
    window_sec: int,
    split_sec: int, # 이제 별 필요없음 (안씀)
) -> int:

    conn = sqlite3.connect(db_path, timeout=5)
    cur = conn.cursor()

    SERVICE_RESOURCES = {
        "small-fast":  {"cpu_m": 50,  "mem_bytes": 128 * 1024 * 1024},
        "small-fast2": {"cpu_m": 50,  "mem_bytes": 128 * 1024 * 1024},
        "medium-fast": {"cpu_m": 100, "mem_bytes": 256 * 1024 * 1024},
        "medium-slow": {"cpu_m": 100, "mem_bytes": 256 * 1024 * 1024},
        "large":       {"cpu_m": 300, "mem_bytes": 512 * 1024 * 1024},
    }

    try:
        # 0) compute window size
        # window = round(window_sec / split_sec)
        # 1) 모든 서비스의 평균 qos (Q_all)
        cur.execute("""
            SELECT avg(qos)
            FROM service_profile
            WHERE t_execute IS NOT NULL AND t_execute > 0
            AND qos IS NOT NULL;
        """)
        row = cur.fetchone()
        avg_qos_all = row[0] if row and row[0] is not None else 0.0
        print(f"AVERAGE QOS : {avg_qos_all}")
        # 분모 보호
        if avg_qos_all <= 0:
            print("Error: qos is 0") # QOS 평균이 0 이라면 어떤 서비스도 시작 되지 않았지 때문에 모든 맥스값은 0이다
            cur.execute(
            """
            UPDATE service_profile
            SET max_container = 0, min_container = 0
            """
            )
            conn.commit()
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
        service_rows = cur.fetchall()

        cur.execute("""
            SELECT
                sum(cpu_free_m),
                sum(mem_free_bytes)
            FROM node_resource_status
        """)

        free_resources = cur.fetchone()
        cpu_free = free_resources[0] if free_resources[0] is not None else 0
        mem_free = free_resources[1] if free_resources[1] is not None else 0

        # results: Dict[str, int] = {}
        results: Dict[str, dict] = {}
        min_container = 0
        max_container = 0

        # min 값 업데이트
        for (
            service,
            request_cnt,
            t_warm,
            t_cold,
            weight,
        ) in service_rows:

            # 분자
            numerator = request_cnt 

            # 분모
            denominator = (
                2/ min(1,(avg_qos_all * weight))
                - 1
            )

            if denominator <= 0:
                print(f"Error: denominator <= 0 for service={service} (den={denominator})")
                min_container = numerator   
            else:
                min_container = math.ceil(numerator / denominator)

            results.setdefault(service, {})
            results[service]["min"] = min_container
        
        # max 값 업데이트
        for service, spec in SERVICE_RESOURCES.items():
            cpu_limit = cpu_free // spec["cpu_m"]
            mem_limit = mem_free // spec["mem_bytes"]   # 🔹 bytes 기준

            max_count = min(cpu_limit, mem_limit)
            max_container = results[service]["min"] + max_count

            results.setdefault(service, {})
            results[service]["max"] = int(max_container)
        
        # 3) DB 업데이트 (서비스별로 max_container 저장)
        # for svc, data in results.items():
        #     print(svc, data)
        cur.executemany(
            """
            UPDATE service_profile
            SET max_container = ?, min_container = ?
            WHERE service = ?
            """,
            [
                (data["max"], data["min"], svc)
                for svc, data in results.items()
            ]
        )
        conn.commit()

        return 1

    finally:
        conn.close()