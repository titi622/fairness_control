import sqlite3
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np


SCENARIOS = [
    {
        "services": ["small-fast", "small-fast2"],
        "start": "2026-04-23 00:21:09",
        "end":   "2026-04-23 00:28:25",
    },
    {
        "services": ["small-fast", "small-fast2"],
        "start": "2026-04-24 21:57:39",
        "end":   "2026-04-24 22:09:20",
    },
    {
        "services": ["small-fast", "small-fast2"],
        "start": "2026-04-21 12:54:31",
        "end":   "2026-04-21 13:00:50",
    },
]


def create_combined_chart(start_time, end_time, services, output_path):
    conn = sqlite3.connect('/home/ubuntu/fairness_control/trace_store.db')

    service_placeholders = "', '".join(services)

    service_alias = {
        'small-fast': 'F1',
        'small-fast2': 'F2',
        'medium-fast': 'F3',
        'medium-slow': 'F4',
        'large': 'F5',
    }

    service_colors = {
        'small-fast':  '#1f77b4',
        'small-fast2': '#d62728',
        'medium-fast': '#2ca02c',
        'medium-slow': '#ff7f0e',
        'large':       '#9467bd',
    }

    pod_query = f"""
    SELECT creation_time_us, service, pod_count
    FROM pod_snapshots
    WHERE datetime(creation_time_us / 1000000, 'unixepoch', '+9 hours')
          BETWEEN '{start_time}' AND '{end_time}'
          AND service IN ('{service_placeholders}')
    ORDER BY creation_time_us ASC;
    """
    df_pod = pd.read_sql_query(pod_query, conn)

    qos_query = f"""
    SELECT creation_time, service, qos
    FROM profile_hst
    WHERE datetime(creation_time / 1000000, 'unixepoch', '+9 hours')
          BETWEEN '{start_time}' AND '{end_time}'
          AND service IN ('{service_placeholders}')
    ORDER BY creation_time ASC;
    """
    df_qos = pd.read_sql_query(qos_query, conn)
    conn.close()

    if df_pod.empty and df_qos.empty:
        print(f"⚠️ 조회된 데이터가 없습니다: {output_path}")
        return

    pod_min = df_pod['creation_time_us'].min() if not df_pod.empty else None
    qos_min = df_qos['creation_time'].min() if not df_qos.empty else None
    t0 = min(x for x in [pod_min, qos_min] if pd.notna(x))

    if not df_pod.empty:
        df_pod['elapsed_sec'] = (df_pod['creation_time_us'] - t0) / 1_000_000

    if not df_qos.empty:
        df_qos['elapsed_sec'] = (df_qos['creation_time'] - t0) / 1_000_000

    fig, ax1 = plt.subplots(figsize=(14, 8))
    ax2 = ax1.twinx()

    for service in services:
        p_data = df_pod[df_pod['service'] == service]
        q_data = df_qos[df_qos['service'] == service]

        current_color = service_colors.get(service, 'black')
        alias = service_alias.get(service, service)

        if not p_data.empty:
            ax1.step(
                p_data['elapsed_sec'],
                p_data['pod_count'],
                where='post',
                label=f'{alias} (#Pods)',
                color=current_color,
                linewidth=1.5,
                alpha=0.7,
                linestyle='--'
            )

        if not q_data.empty:
            ax2.plot(
                q_data['elapsed_sec'],
                q_data['qos'],
                label=f'{alias} (QoS)',
                color=current_color,
                linewidth=2.5
            )

    ax1.set_xlabel('Time (Seconds)', fontsize=30)
    ax1.set_ylabel('Pod Count', color='black', fontsize=30)
    ax2.set_ylabel('QoS', color='black', fontsize=30)

    ax1.tick_params(axis='both', labelsize=30)
    ax2.tick_params(axis='both', labelsize=30)

    ax1.set_yticks(np.arange(0, 71, 10))
    ax1.set_ylim(0, 70)
    ax2.set_ylim(0, 1)

    x_max = 0
    if not df_pod.empty:
        x_max = max(x_max, df_pod['elapsed_sec'].max())
    if not df_qos.empty:
        x_max = max(x_max, df_qos['elapsed_sec'].max())

    # ax1.set_xlim(0, x_max)

    # tick_step = 100 if x_max <= 800 else 200
    # ax1.set_xticks(np.arange(0, x_max + tick_step, tick_step))

    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()

    ax1.legend(
        lines1 + lines2,
        labels1 + labels2,
        loc='upper left',
        bbox_to_anchor=(1.1, 1.0),
        fontsize=20
    )

    ax1.grid(True, linestyle=':', alpha=0.6)
    plt.tight_layout()

    plt.savefig(output_path)
    print(f"✅ 분석 완료: {output_path}")
    plt.close()


if __name__ == "__main__":
    for idx, scenario in enumerate(SCENARIOS, start=1):
        output_path = f"/home/ubuntu/fairness_control/[moti]result_{idx}.png"

        create_combined_chart(
            start_time=scenario["start"],
            end_time=scenario["end"],
            services=scenario["services"],
            output_path=output_path
        )