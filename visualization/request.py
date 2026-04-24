import sqlite3
import pandas as pd

DB_PATH = "/home/ubuntu/fairness_control/trace_store.db"

SCENARIOS = {
    "scenario1": {
        "services": ["small-fast", "small-fast2"],
        "Proposed": {
            "start": "2026-04-23 17:43:00",
            "end":   "2026-04-23 17:54:01",
            "requests": {"small-fast": 536, "small-fast2": 32},
        },
        "LRU": {
            "start": "2026-04-23 20:40:19",
            "end":   "2026-04-23 20:51:07",
            "requests": {"small-fast": 469, "small-fast2": 28},
        },
    },

    "scenario2": {
        "services": ["small-fast", "small-fast2"],
        "Proposed": {
            "start": "2026-04-23 17:20:35",
            "end":   "2026-04-23 17:31:56",
            "requests": {"small-fast": 804, "small-fast2": 268},
        },
        "LRU": {
            "start": "2026-04-23 22:38:33",
            "end":   "2026-04-23 22:53:32",
            "requests": {"small-fast": 804, "small-fast2": 268},
        },
    },

    "scenario3": {
        "services": ["small-fast", "medium-fast"],
        "Proposed": {
            "start": "2026-04-23 11:25:20",
            "end":   "2026-04-23 11:37:07",
            "requests": {"small-fast": 536, "medium-fast": 328},
        },
        "LRU": {
            "start": "2026-04-23 23:02:07",
            "end":   "2026-04-23 23:22:05",
            "requests": {"small-fast": 536, "medium-fast": 328},
        },
    },

    "scenario4": {
        "services": ["medium-fast", "medium-slow"],
        "Proposed": {
            "start": "2026-04-22 11:42:36",
            "end":   "2026-04-22 11:54:07",
            "requests": {"medium-fast": 328, "medium-slow": 328},
        },
        "LRU": {
            "start": "2026-04-23 23:35:24",
            "end":   "2026-04-23 23:55:01",
            "requests": {"medium-fast": 328, "medium-slow": 328},
        },
    },

    "scenario5": {
        "services": ["small-fast", "medium-slow", "large"],
        "Proposed": {
            "start": "2026-04-23 13:39:35",
            "end":   "2026-04-23 14:00:49",
            "requests": {"small-fast": 603, "medium-slow": 369, "large": 126},
        },
        "LRU": {
            "start": "2026-04-24 08:12:21",
            "end":   "2026-04-24 08:47:34",
            "requests": {"small-fast": 536, "medium-slow": 328, "large": 112},
        },
    },

    "scenario6": {
        "services": ["small-fast", "small-fast2"],
        "Proposed": {
            "start": "2026-04-22 12:43:58",
            "end":   "2026-04-22 12:55:14",
            "requests": {"small-fast": 536, "small-fast2": 536},
        },
        "LRU": {
            "start": "2026-04-24 10:47:22",
            "end":   "2026-04-24 11:07:39",
            "requests": {"small-fast": 536, "small-fast2": 536},
        },
    },
}


def count_cold_starts(conn, start, end, services):
    service_placeholders = "', '".join(services)

    query = f"""
    SELECT creation_time_us, service, pod_count
    FROM pod_snapshots
    WHERE datetime(creation_time_us / 1000000, 'unixepoch', '+9 hours')
          BETWEEN '{start}' AND '{end}'
          AND service IN ('{service_placeholders}')
    ORDER BY service, creation_time_us ASC;
    """

    df = pd.read_sql_query(query, conn)

    result = {}

    for service in services:
        s_df = df[df["service"] == service].sort_values("creation_time_us")

        cold_count = 0
        prev = None

        for _, row in s_df.iterrows():
            curr = int(row["pod_count"])

            if prev is not None and curr > prev:
                cold_count += curr - prev

            prev = curr

        result[service] = cold_count

    return result


conn = sqlite3.connect(DB_PATH)

for scenario_name, scenario in SCENARIOS.items():
    print(f"\n===== {scenario_name} =====")

    for mode in ["Proposed", "LRU"]:
        cold_counts = count_cold_starts(
            conn=conn,
            start=scenario[mode]["start"],
            end=scenario[mode]["end"],
            services=scenario["services"]
        )

        print(f"\n[{mode}]")

        for service in scenario["services"]:
            total_req = scenario[mode]["requests"][service]
            cold = cold_counts.get(service, 0)
            rate = (cold / total_req) * 100 if total_req > 0 else 0

            print(
                f"{service}: "
                f"total_requests={total_req}, "
                f"cold_starts={cold}, "
                f"cold_start_rate={rate:.2f}%"
            )

conn.close()