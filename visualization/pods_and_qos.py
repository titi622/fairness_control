import sqlite3
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import pandas as pd

def create_combined_chart(start_time, end_time, services):
    conn = sqlite3.connect('/home/ubuntu/fairness_control/trace_store.db')
    
    service_placeholders = "', '".join(services)
    
    # 데이터 조회 (시간 조건은 내부적으로만 사용)
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
        print("⚠️ 조회된 데이터가 없습니다.")
        return

    # 기준 시각(t0) 계산
    pod_min = df_pod['creation_time_us'].min() if not df_pod.empty else None
    qos_min = df_qos['creation_time'].min() if not df_qos.empty else None
    t0 = min(x for x in [pod_min, qos_min] if pd.notna(x))

    # 경과 시간(초) 계산
    if not df_pod.empty:
        df_pod['elapsed_sec'] = (df_pod['creation_time_us'] - t0) / 1_000_000
    if not df_qos.empty:
        df_qos['elapsed_sec'] = (df_qos['creation_time'] - t0) / 1_000_000

    fig, ax1 = plt.subplots(figsize=(14, 8))
    ax2 = ax1.twinx()
    
    colors = plt.get_cmap('tab20', len(services) * 2)

    for i, service in enumerate(services):
        p_data = df_pod[df_pod['service'] == service]
        q_data = df_qos[df_qos['service'] == service]
        current_color = colors(i)

        if not p_data.empty:
            ax1.step(p_data['elapsed_sec'], p_data['pod_count'], where='post',
                    label=f'{service} (Pods)', color=current_color, alpha=0.5, linestyle='--')

        if not q_data.empty:
            ax2.plot(q_data['elapsed_sec'], q_data['qos'],
                    label=f'{service} (QoS)', color=current_color, linewidth=2.5)

    # --- 변경 포인트: 시간 정보 제거 ---
    ax1.set_xlabel('Elapsed Time (Seconds)') # 'from start_time' 제거
    ax1.set_ylabel('Pod Count', color='blue', fontsize=12)
    ax2.set_ylabel('QoS Value', color='red', fontsize=12)
    plt.title('Pod Count & QoS Trends Analysis', fontsize=15) # 제목에서 시간 제거

    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + labels2, labels1 + labels2, loc='upper left', bbox_to_anchor=(1.1, 1))

    ax1.grid(True, linestyle=':', alpha=0.6)
    plt.tight_layout()

    plt.savefig("/home/ubuntu/fairness_control/pod_qos_analysis.png")
    print("✅ 분석 완료: 실제 시간 정보 없이 'pod_qos_analysis.png'로 저장되었습니다.")

if __name__ == "__main__":
    TARGET_START = '2026-03-12 09:15:00'
    TARGET_END   = '2026-03-12 09:20:59'
    TARGET_SERVICES = ['medium-fast', 'small-fast2']
    
    create_combined_chart(TARGET_START, TARGET_END, TARGET_SERVICES)