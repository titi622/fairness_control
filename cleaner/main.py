import sqlite3
import time
from datetime import datetime, timezone
from typing import Optional, List, Set

from kubernetes import client, config
from kubernetes.client.rest import ApiException


class PendingPodCleaner:
    def __init__(
        self,
        v1: client.CoreV1Api,
        db_path: str,
        interval_sec: int = 10,
        pending_age_threshold_sec: int = 30,
    ):
        self.v1 = v1
        self.db_path = db_path
        self.interval_sec = interval_sec
        self.pending_age_threshold_sec = pending_age_threshold_sec

        self.target_namespaces: Set[str] = {
            "small-fast",
            "small-fast2",
            "medium-fast",
            "medium-slow",
            "large",
        }

    def _get_min_container(self, namespace: str) -> Optional[int]:
        conn = sqlite3.connect(self.db_path, timeout=5)
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT min_container FROM service_profile WHERE service=?",
                (namespace,),
            )
            row = cur.fetchone()
            if row is None or row[0] is None:
                return None
            return int(row[0])
        finally:
            conn.close()

    def _get_pod_age_sec(self, pod) -> float:
        ct = pod.metadata.creation_timestamp
        if ct is None:
            return 0.0

        now = datetime.now(timezone.utc)
        return (now - ct).total_seconds()

    def _list_running_pending_pods(self, namespace: str):
        pod_list = self.v1.list_namespaced_pod(namespace=namespace).items

        running_pods = []
        pending_pods = []
        old_pending_pods = []

        for pod in pod_list:
            phase = (pod.status.phase or "").strip()

            if phase == "Running":
                running_pods.append(pod)

            elif phase == "Pending":
                pending_pods.append(pod)

                age_sec = self._get_pod_age_sec(pod)
                if age_sec >= self.pending_age_threshold_sec:
                    old_pending_pods.append(pod)

        return running_pods, pending_pods, old_pending_pods

    def _delete_pending_pods(self, namespace: str, pending_pods: List, delete_count: int) -> int:
        deleted = 0

        # 오래 Pending 상태인 것부터 삭제
        pending_pods.sort(
            key=lambda p: p.metadata.creation_timestamp.timestamp()
            if p.metadata.creation_timestamp else 0
        )

        for pod in pending_pods[:delete_count]:
            pod_name = pod.metadata.name
            age_sec = self._get_pod_age_sec(pod)

            try:
                self.v1.delete_namespaced_pod(
                    name=pod_name,
                    namespace=namespace,
                    body=client.V1DeleteOptions(grace_period_seconds=0),
                )
                deleted += 1
                print(f"[delete] ns={namespace} pod={pod_name} age={age_sec:.1f}s")
            except ApiException as e:
                print(f"[error] ns={namespace} pod={pod_name} delete failed: {e}")
            except Exception as e:
                print(f"[error] ns={namespace} pod={pod_name} delete failed: {e}")

        return deleted

    def cleanup_once(self) -> None:
        for namespace in self.target_namespaces:
            try:
                min_container = self._get_min_container(namespace)
                if min_container is None:
                    print(f"[skip] ns={namespace} min_container not found")
                    continue

                running_pods, pending_pods, old_pending_pods = self._list_running_pending_pods(namespace)
                current_pod_count = len(running_pods) + len(pending_pods)

                if current_pod_count <= min_container:
                    print(
                        f"[noop] ns={namespace} current={current_pod_count}, "
                        f"min={min_container}, pending={len(pending_pods)}, "
                        f"old_pending={len(old_pending_pods)}"
                    )
                    continue

                need_delete = current_pod_count - min_container

                # 전체 pending이 아니라 장시간 pending만 삭제 대상으로 사용
                actual_delete_count = min(need_delete, len(old_pending_pods))

                if actual_delete_count <= 0:
                    print(
                        f"[noop] ns={namespace} current={current_pod_count}, "
                        f"min={min_container}, pending={len(pending_pods)}, "
                        f"old_pending=0"
                    )
                    continue

                deleted = self._delete_pending_pods(
                    namespace=namespace,
                    pending_pods=old_pending_pods,
                    delete_count=actual_delete_count,
                )

                print(
                    f"[done] ns={namespace} current={current_pod_count}, "
                    f"min={min_container}, pending={len(pending_pods)}, "
                    f"old_pending={len(old_pending_pods)}, "
                    f"target_delete={actual_delete_count}, deleted={deleted}"
                )

            except Exception as e:
                print(f"[error] ns={namespace} cleanup failed: {e}")

    def run_forever(self) -> None:
        print(
            f"[start] pending pod cleaner started, interval={self.interval_sec}s, "
            f"pending_age_threshold={self.pending_age_threshold_sec}s"
        )
        while True:
            try:
                self.cleanup_once()
            except Exception as e:
                print(f"[error] cleanup loop failed: {e}")
            time.sleep(self.interval_sec)


def build_k8s_client() -> client.CoreV1Api:
    try:
        config.load_incluster_config()
        print("[init] loaded incluster config")
    except Exception:
        config.load_kube_config()
        print("[init] loaded kube config")

    return client.CoreV1Api()


def main():
    db_path = "/home/ubuntu/fairness_control/trace_store.db"
    interval_sec = 10
    pending_age_threshold_sec = 30

    v1 = build_k8s_client()
    cleaner = PendingPodCleaner(
        v1=v1,
        db_path=db_path,
        interval_sec=interval_sec,
        pending_age_threshold_sec=pending_age_threshold_sec,
    )
    cleaner.run_forever()


if __name__ == "__main__":
    main()