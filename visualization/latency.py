import sqlite3
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

plt.rcParams.update({'font.size': 20})

SCENARIOS = {
    "scenario1": {
        "services": ["small-fast", "small-fast2"],
        "Proposed": {
            "start": "2026-04-23 17:43:00",
            "end":   "2026-04-23 17:54:01",
        },
        "LRU": {
            "start": "2026-04-23 20:40:19",
            "end":   "2026-04-23 20:51:07",
        },
    },

    "scenario2": {
        "services": ["small-fast", "small-fast2"],
        "Proposed": {
            "start": "2026-04-23 17:20:35",
            "end":   "2026-04-23 17:31:56",
        },
        "LRU": {
            "start": "2026-04-23 22:38:33",
            "end":   "2026-04-23 22:53:32",
        },
    },

    "scenario3": {
        "services": ["small-fast", "medium-fast"],
        "Proposed": {
            "start": "2026-04-23 11:25:20",
            "end":   "2026-04-23 11:37:07",
        },
        "LRU": {
            "start": "2026-04-23 23:02:07",
            "end":   "2026-04-23 23:22:05",
        },
    },

    "scenario4": {
        "services": ["medium-fast", "medium-slow"],
        "Proposed": {
            "start": "2026-04-22 11:42:36",
            "end":   "2026-04-22 11:54:07",
        },
        "LRU": {
            "start": "2026-04-23 23:35:24",
            "end":   "2026-04-23 23:55:01",
        },
    },

    "scenario5": {
        "services": ["small-fast", "medium-slow", "large"],
        "Proposed": {
            "start": "2026-04-23 13:39:35",
            "end":   "2026-04-23 14:00:49",
        },
        "LRU": {
            "start": "2026-04-24 08:12:21",
            "end":   "2026-04-24 08:47:34",
        },
    },

    "scenario6": {
        "services": ["small-fast", "small-fast2"],
        "Proposed": {
            "start": "2026-04-22 12:43:58",
            "end":   "2026-04-22 12:55:14",
        },
        "LRU": {
            "start": "2026-04-24 10:47:22",
            "end":   "2026-04-24 11:07:39",
        },
    },
}

def remove_outliers_iqr(s):
    q1 = s.quantile(0.25)
    q3 = s.quantile(0.75)
    iqr = q3 - q1

    lower = q1 - 1.5 * iqr
    upper = q3 + 1.5 * iqr

    return s[(s >= lower) & (s <= upper)]

def plot_box_by_service(start_time, end_time, start_time2, end_time2, services, output_path):
    conn = sqlite3.connect('/home/ubuntu/fairness_control/trace_store.db')
    service_placeholders = "', '".join(services)

    # query_template = """
    # SELECT service, t_execute
    # FROM profile_hst
    # WHERE datetime(creation_time / 1000000, 'unixepoch', '+9 hours')
    #       BETWEEN '{start}' AND '{end}'
    #       AND service IN ('{services}')
    # ORDER BY service, creation_time ASC;
    # """

    query_template = """
    SELECT service, duration_ms as t_execute
    FROM traces
    WHERE datetime(start_time_us / 1000000, 'unixepoch', '+9 hours')
        BETWEEN '{start}' AND '{end}'
        AND service IN ('{services}')
    """

    df_proposed = pd.read_sql_query(
        query_template.format(
            start=start_time,
            end=end_time,
            services=service_placeholders
        ),
        conn
    )

    df_lru = pd.read_sql_query(
        query_template.format(
            start=start_time2,
            end=end_time2,
            services=service_placeholders
        ),
        conn
    )
    conn.close()

    box_data = []
    labels = []

    for service in services:
        proposed_data = df_proposed[df_proposed['service'] == service]['t_execute'].dropna()
        lru_data = df_lru[df_lru['service'] == service]['t_execute'].dropna()

        # proposed_data = remove_outliers_iqr(proposed_data)
        # lru_data = remove_outliers_iqr(lru_data)

        if not proposed_data.empty:
            print(f"[Proposed] {service}: mean={proposed_data.mean():.2f} ms")
            box_data.append(proposed_data)
            labels.append(f'{service}\nProposed')

        if not lru_data.empty:
            print(f"[LRU] {service}: mean={lru_data.mean():.2f} ms")
            box_data.append(lru_data)
            labels.append(f'{service}\nLRU')

    if not box_data:
        print("⚠️ 데이터가 없습니다.")
        return

    plt.figure(figsize=(12, 6))
    plt.boxplot(box_data, labels=labels, showmeans=True, showfliers=False)
    plt.xticks(fontsize=16)

    plt.ylabel('latency (ms)')
    plt.grid(True, linestyle=':', alpha=0.6)
    plt.tight_layout()
    plt.savefig(output_path)

    print("✅ 박스플롯 저장 완료")


if __name__ == "__main__":
    scenario = SCENARIOS["scenario1"]

    for i in range(1, 7):
        print("####### scenario", i, "#######")
        key = f"scenario{i}"
        scenario = SCENARIOS[key]

        plot_box_by_service(
            start_time=scenario["Proposed"]["start"],
            end_time=scenario["Proposed"]["end"],
            start_time2=scenario["LRU"]["start"],
            end_time2=scenario["LRU"]["end"],
            services=scenario["services"],
            output_path=f"/home/ubuntu/fairness_control/latency{i}.png"  # 🔥 파일명 자동
        )