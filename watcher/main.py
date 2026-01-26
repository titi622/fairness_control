import time
import logging
import sqlite3
from typing import Dict, List

from collector.prometheus import PrometheusCollector
from collector.jaeger import JaegerCollector


# ----------------------------
# DB 생성
# ----------------------------
conn = sqlite3.connect("../trace_store.db")
cur = conn.cursor()

cur.executescript("""
CREATE TABLE IF NOT EXISTS pod_snapshots (
  creation_time_us INTEGER NOT NULL,
  service          TEXT    NOT NULL,
  revision         TEXT    NOT NULL,
  pod_count        INTEGER NOT NULL,
  PRIMARY KEY (creation_time_us, service, revision)
);

CREATE TABLE IF NOT EXISTS traces (
  trace_id          TEXT    PRIMARY KEY,
  creation_time_us  INTEGER NOT NULL,
  service           TEXT    NOT NULL,
  revision          TEXT    NOT NULL,
  start_time_us     INTEGER,
  duration_ms       REAL
);

CREATE INDEX IF NOT EXISTS idx_pods_time
  ON pod_snapshots(creation_time_us);
CREATE INDEX IF NOT EXISTS idx_pods_srv_rev_time
  ON pod_snapshots(service, revision, creation_time_us);

CREATE INDEX IF NOT EXISTS idx_traces_time
  ON traces(creation_time_us);
CREATE INDEX IF NOT EXISTS idx_traces_srv_rev_time
  ON traces(service, revision, creation_time_us);
""")
conn.commit()


# ----------------------------
# Logging 설정
# ----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

def now_us() -> int:
    return int(time.time() * 1_000_000)

def main():
    PROMETHEUS_URL = "http://localhost:9090"
    JAEGER_URL = "http://localhost:16686"

    prom = PrometheusCollector(PROMETHEUS_URL)
    jaeger = JaegerCollector(JAEGER_URL)


    logging.info("Knative profiler 시작 (Prometheus / Jaeger 분리 모드)")

    jaeger_store: Dict[str, Dict] = {}

    try:
        while True:
            creation_time_us = now_us()
            # =====================================================
            # 1) Prometheus 출력 (상태 스냅샷)
            # =====================================================
            prom_results = prom.get_service_info()

            print("\n=== Prometheus: Knative Service Status ===")
            if not prom_results:
                print("No services found.")
            else:
                print(f"{'SERVICE':<20} | {'REVISION':<30} | {'POD COUNT':<10}")
                print("-" * 70)
                for m in prom_results:
                    print(
                        f"{m['service']:<20} | "
                        f"{m['revision']:<30} | "
                        f"{m['pod_count']:<10}"
                    )
                    cur.execute("""
                        INSERT OR IGNORE INTO pod_snapshots
                        (creation_time_us, service, revision, pod_count)
                        VALUES (?, ?, ?, ?)
                    """, (creation_time_us, m["service"], m["revision"], int(m["pod_count"])))
                    conn.commit()

            # =====================================================
            # 2) Jaeger 출력 (요청 단위 trace 정보)
            # =====================================================
            print("\n=== Jaeger: Request-level Durations ===")

            # Jaeger에서 현재 관측 가능한 service 목록
            try:
                jaeger_services: List[str] = jaeger.list_services()
                print(jaeger_services)
            except Exception as e:
                logging.error(f"Jaeger service 목록 조회 실패: {e}")
                time.sleep(10)
                continue

            try:
                traces = jaeger.get_traces(
                    service="activator",
                    lookback_sec=60,
                    limit=500
                )
            except Exception as e:
                logging.warning(f"[{service}] trace 조회 실패: {e}")
                continue

            jager_results = jaeger.extract_request_info(traces)
            if jager_results:
                for r in jager_results:
                    cur.execute("""
                        INSERT OR IGNORE INTO traces
                        (trace_id, creation_time_us, service, revision, start_time_us, duration_ms)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (
                        r["trace_id"],
                        creation_time_us,        # 수집 시점
                        r["service"],
                        r["revision"],
                        r["start_us"],           # 이벤트 시점
                        r["duration_ms"],
                    ))
                    conn.commit()

            # =====================================================
            # 3) 주기 대기
            # =====================================================
            # cur.execute("""
            #     SELECT
            #     creation_time_us,
            #     service,
            #     revision,
            #     pod_count
            #     FROM pod_snapshots
            #     ORDER BY creation_time_us DESC
            #     LIMIT 30
            #     """)

            # pod_rows = cur.fetchall()
            # for row in pod_rows:
            #     print(row)

            # cur.execute("""
            #     SELECT
            #     trace_id,
            #     creation_time_us,
            #     service,
            #     revision,
            #     start_time_us,
            #     duration_ms
            #     FROM traces
            #     ORDER BY creation_time_us DESC
            #     LIMIT 30
            #     """)

            # trace_rows = cur.fetchall()
            # for row in trace_rows:
            #     print(row)

            time.sleep(20)

    except KeyboardInterrupt:
        logging.info("종료 신호 수신, 요약 출력")

        conn.close()

        print("\n=== Jaeger Summary ===")
        for svc, data in jaeger_store.items():
            print(
                f"SERVICE={svc}, total_requests={len(data['requests'])}"
            )


if __name__ == "__main__":
    main()
