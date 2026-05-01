import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
from io import StringIO
import numpy as np
from matplotlib.ticker import MaxNLocator

service_alias = {
    "small-fast": "F1",
    "small-fast2": "F2"
}

service_colors = {
    "small-fast":  "#1f77b4",
    "small-fast2": "#d62728",
}

def create_plot(raw_data, filename):
    df = pd.read_csv(
        StringIO(raw_data.strip()),
        header=None,
        names=["timestamp", "service", "request_count"]
    )

    df["timestamp"] = pd.to_datetime(df["timestamp"])

    t0 = df["timestamp"].min()
    df["elapsed_sec"] = (df["timestamp"] - t0).dt.total_seconds()

    fig, ax = plt.subplots(figsize=(14, 8))

    width = 20  # 막대 폭 (초 단위)

    for svc in df["service"].unique():
        sub = df[df["service"] == svc]

        ax.bar(
            sub["elapsed_sec"],
            sub["request_count"],
            width=width,
            label=service_alias.get(svc, svc),
            color=service_colors.get(svc, "black"),
            alpha=0.8
        )

    # ===== 핵심: 자동으로 보기 좋은 간격 =====
    ax.xaxis.set_major_locator(MaxNLocator(nbins=6))  # 자동 100/200 단위 맞춤

    ax.set_xlabel("Time (seconds)", fontsize=28)
    ax.set_ylabel("Request Count", fontsize=28)

    ax.tick_params(axis="both", labelsize=24)
    ax.legend(fontsize=24)

    ax.grid(True, linestyle=":", alpha=0.6)

    plt.tight_layout()
    plt.savefig(filename)
    plt.close()


# ===== 실험1 =====
data_exp1 = """
2026-04-23 00:21:09,small-fast,67
2026-04-23 00:22:06,small-fast2,4
2026-04-23 00:23:20,small-fast,67
2026-04-23 00:24:09,small-fast2,4
2026-04-23 00:25:23,small-fast,67
2026-04-23 00:26:12,small-fast2,4
2026-04-23 00:27:27,small-fast,67
2026-04-23 00:28:15,small-fast2,4
"""

create_plot(data_exp1, "[moti]request_1.png")


# ===== 실험2 =====
data_exp2 = """
2026-04-24 21:57:39,small-fast,67
2026-04-24 21:58:31,small-fast,67
2026-04-24 21:59:12,small-fast,67
2026-04-24 22:00:14,small-fast2,67
2026-04-24 22:01:33,small-fast,67
2026-04-24 22:02:40,small-fast,67
2026-04-24 22:03:26,small-fast,67
2026-04-24 22:04:32,small-fast2,67
2026-04-24 22:05:52,small-fast,67
2026-04-24 22:06:51,small-fast,67
2026-04-24 22:07:33,small-fast,67
2026-04-24 22:08:34,small-fast2,67
"""

create_plot(data_exp2, "[moti]request_2.png")