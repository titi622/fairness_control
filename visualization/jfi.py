import sqlite3
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

plt.rcParams.update({"font.size": 20})

DB_PATH = "/home/ubuntu/fairness_control/trace_store.db"
OUT_PROPOSED = "/home/ubuntu/fairness_control/jfi_proposed.png"
OUT_LRU = "/home/ubuntu/fairness_control/jfi_lru.png"

SCENARIOS = {
    "scenario1": {
        "services": ["small-fast", "small-fast2"],
        "Proposed": {"start": "2026-04-23 17:43:00", "end": "2026-04-23 17:54:01"},
        "LRU": {"start": "2026-04-23 20:40:19", "end": "2026-04-23 20:51:07"},
    },
    "scenario2": {
        "services": ["small-fast", "small-fast2"],
        "Proposed": {"start": "2026-04-23 17:20:35", "end": "2026-04-23 17:31:56"},
        "LRU": {"start": "2026-04-23 22:38:55", "end": "2026-04-23 22:53:32"},
    },
    "scenario3": {
        "services": ["small-fast", "medium-fast"],
        "Proposed": {"start": "2026-04-23 11:25:20", "end": "2026-04-23 11:37:07"},
        "LRU": {"start": "2026-04-23 23:02:07", "end": "2026-04-23 23:22:05"},
    },
    "scenario4": {
        "services": ["medium-fast", "medium-slow"],
        "Proposed": {"start": "2026-04-22 11:42:36", "end": "2026-04-22 11:54:07"},
        "LRU": {"start": "2026-04-23 23:35:24", "end": "2026-04-23 23:55:01"},
    },
    "scenario5": {
        "services": ["small-fast", "medium-slow", "large"],
        "Proposed": {"start": "2026-04-23 13:39:35", "end": "2026-04-23 14:00:49"},
        "LRU": {"start": "2026-04-24 08:12:21", "end": "2026-04-24 08:47:34"},
    },
    "scenario6": {
        "services": ["small-fast", "small-fast2"],
        "Proposed": {"start": "2026-04-22 12:43:58", "end": "2026-04-22 12:55:14"},
        "LRU": {"start": "2026-04-24 10:47:22", "end": "2026-04-24 11:07:39"},
    },
}


def compute_jfi(conn, start, end, services):
    service_placeholders = "', '".join(services)

    query = f"""
    SELECT service, qos, weight, creation_time
    FROM profile_hst
    WHERE datetime(creation_time / 1000000, 'unixepoch', '+9 hours')
          BETWEEN '{start}' AND '{end}'
          AND service IN ('{service_placeholders}')
    ORDER BY creation_time ASC;
    """

    df = pd.read_sql_query(query, conn)

    if df.empty:
        return pd.DataFrame(columns=["elapsed_sec", "jfi"])

    df["x"] = df["qos"] / df["weight"]

    rows = []

    for t, group in df.groupby("creation_time"):
        x = group["x"].dropna().values
        n = len(x)

        if n == 0:
            continue

        denom = n * (x ** 2).sum()
        if denom == 0:
            continue

        jfi = (x.sum() ** 2) / denom

        rows.append({
            "creation_time": t,
            "jfi": jfi
        })

    result = pd.DataFrame(rows)

    if result.empty:
        return pd.DataFrame(columns=["elapsed_sec", "jfi"])

    result["elapsed_sec"] = (
        result["creation_time"] - result["creation_time"].min()
    ) / 1_000_000

    return result


def plot_jfi_all(mode, output_path):
    conn = sqlite3.connect(DB_PATH)

    plt.figure(figsize=(14, 8))
    cmap = plt.get_cmap("tab10")

    final_jfi_values = []  # 🔥 추가

    for i in range(1, 7):
        key = f"scenario{i}"
        scenario = SCENARIOS[key]

        df_jfi = compute_jfi(
            conn=conn,
            start=scenario[mode]["start"],
            end=scenario[mode]["end"],
            services=scenario["services"]
        )

        if df_jfi.empty:
            print(f"⚠️ {key} {mode}: 데이터 없음")
            continue
        if mode == "LRU" and key != "scenario5":
            df_jfi = df_jfi[df_jfi["elapsed_sec"] <= 700]

        if df_jfi.empty:
            continue
        # 🔥 마지막 값 저장
        final_jfi = df_jfi["jfi"].iloc[-1]
        final_jfi_values.append(final_jfi)

        plt.plot(
            df_jfi["elapsed_sec"],
            df_jfi["jfi"],
            label=f"Scenario {i}",
            color=cmap(i - 1),
            linewidth=2.5
        )

    conn.close()

    # 🔥 평균 계산
    if final_jfi_values:
        avg_jfi = sum(final_jfi_values) / len(final_jfi_values)
        print(f"\n[{mode}] Average Final JFI: {avg_jfi:.4f}\n")

    plt.xlabel("Time (seconds)", fontsize=30)
    plt.ylabel("JFI score", fontsize=30)
    plt.tick_params(axis='both', labelsize=30)
    plt.ylim(0, 1)
    plt.grid(True, linestyle=":", alpha=0.6)
    plt.xlim(left=0)
    plt.legend(fontsize=20, loc='lower right')

    if mode == "LRU":
        plt.xlim(0, 1250)
        plt.xticks(range(0, 1251, 200))

    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()

    print(f"✅ 저장 완료: {output_path}")


if __name__ == "__main__":
    plot_jfi_all("Proposed", OUT_PROPOSED)
    plot_jfi_all("LRU", OUT_LRU)