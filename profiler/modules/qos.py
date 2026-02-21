import sqlite3


def update_qos(db_path: str) -> int:
    """
    모든 서비스에 대해 '최신 service_profile row 1개'의 qos를 계산하여 UPDATE 한다.

    qos = t_warm / t_execute
    - t_execute가 0 또는 NULL이면 1로 환산
    - t_warm이 NULL이면 0으로 처리

    Returns:
        업데이트된 row 수
    """

    conn = sqlite3.connect(db_path, timeout=5)
    cur = conn.cursor()

    try:
        sql = """
        UPDATE service_profile
        SET qos =
            CASE 
                WHEN t_execute IS NULL OR t_execute = 0.0 THEN 1.0
                ELSE (t_warm / t_execute)

            END;
        """
        cur.execute(sql)
        conn.commit()
        return cur.rowcount

    finally:
        conn.close()
