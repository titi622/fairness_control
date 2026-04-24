import sqlite3
import time


class ProfileCollector:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self.cur = conn.cursor()

    def save_profile(self):
        now_us = time.time_ns() // 1_000  #
        try:
            self.cur.execute("""
                INSERT INTO profile_hst (
                    service,
                    creation_time,
                    t_warm,
                    t_cold,
                    t_execute,
                    weight,
                    qos,
                    max_container,
                    min_container,
                    active_container,
                    request_cnt
                )
                SELECT
                    service,
                    ?,
                    t_warm,
                    t_cold,
                    t_execute,
                    weight,
                    qos,
                    max_container,
                    min_container,
                    active_container,
                    request_cnt
                FROM service_profile
            """, (now_us,))

            self.conn.commit()
        except sqlite3.Error as e:
            print(f"DB 업데이트 실패: {e}")
            self.conn.rollback()