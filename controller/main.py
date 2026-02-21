#!/usr/bin/env python3
### 쓰레드 분리 테스트 ###
import os
import sys
import time
import sqlite3
import threading
import signal
from datetime import datetime, timezone
from typing import Optional, Tuple, Dict

from kubernetes import client, config, watch
from kubernetes.client import V1Pod

from eviction.eviction_manager import EvictionManager

# ---- Config ----
SQLITE_PATH = "/home/ubuntu/fairness_control/trace_store.db"
SERVICE_TABLE = "service_profile"
MAXCOL = "max_container"
SERVICECOL = "service"
PENDING_MIN_SECONDS = float(1)
PRINT_REPEAT_SECONDS = float(5)

IN_FLIGHT_TIMEOUT = 5  # seconds


def load_kube_config() -> None:
    try:
        config.load_incluster_config()
        return
    except Exception:
        pass

    try:
        config.load_kube_config()
        return
    except Exception as e:
        raise RuntimeError(f"kube config load failed: {e}")


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def parse_dt(x) -> Optional[datetime]:
    if x is None:
        return None
    if isinstance(x, datetime):
        return x if x.tzinfo else x.replace(tzinfo=timezone.utc)
    try:
        return datetime.fromisoformat(str(x).replace("Z", "+00:00"))
    except Exception:
        return None


def pod_age_seconds(pod: V1Pod) -> float:
    ct = parse_dt(getattr(pod.metadata, "creation_timestamp", None))
    if not ct:
        return 0.0
    return max(0.0, (now_utc() - ct).total_seconds())


def is_pending_unschedulable(pod: V1Pod) -> Tuple[bool, str]:
    if pod.status is None or pod.status.phase != "Pending":
        return (False, "")

    conds = pod.status.conditions or []
    for c in conds:
        if c.type == "PodScheduled" and c.status == "False":
            reason = (c.reason or "").strip()
            msg = (c.message or "").strip()
            if reason in ("Unschedulable", "SchedulingDisabled"):
                return (True, f"{reason}: {msg}")

    return (False, "Pending (no PodScheduled detail)")


def sqlite_get_max_container(conn: sqlite3.Connection, service: str) -> Optional[int]:
    q = f"SELECT {MAXCOL} FROM {SERVICE_TABLE} WHERE {SERVICECOL} = ? LIMIT 1"
    cur = conn.execute(q, (service,))
    row = cur.fetchone()
    if not row:
        return None
    try:
        return int(row[0])
    except Exception:
        return None


def count_running_pods(v1: client.CoreV1Api, namespace: str) -> int:
    try:
        pods = [
            p for p in v1.list_namespaced_pod(
                namespace=namespace,
                field_selector="status.phase=Running"
            ).items
            if p.metadata.deletion_timestamp is None
        ]
        return len(pods)
    except Exception:
        return 0


class EvictionWatcher(threading.Thread):
    """
    기존 main의 watch pending → sqlite(max_container) → eviction 진입 로직을 스레드로 분리
    """
    def __init__(self, stop_event: threading.Event):
        super().__init__(name="eviction-watcher", daemon=True)
        self.stop_event = stop_event

        # 중복 방지/출력 레이트리밋 상태는 스레드 내부 상태로 보관
        self.in_flight_pods: Dict[str, float] = {}  # {uid: ts}
        self.last_print: Dict[Tuple[str, str, str], float] = {}  # {(ns,name,uid): ts}

    def run(self) -> None:
        v1 = client.CoreV1Api(client.ApiClient())  # 스레드별 ApiClient 권장
        w = watch.Watch()

        conn = sqlite3.connect(SQLITE_PATH, timeout=5.0, check_same_thread=False)
        evict_mgr = EvictionManager(conn)

        print("[thread] eviction watcher started")
        print("[watch] pending→sqlite(max_container)→evict-gate (all namespaces)")

        list_res = v1.list_pod_for_all_namespaces(limit=1)
        current_rv = list_res.metadata.resource_version

        while not self.stop_event.is_set():
            try:
                for evt in w.stream(
                    v1.list_pod_for_all_namespaces,
                    field_selector="status.phase=Pending",
                    resource_version=current_rv,
                    timeout_seconds=30,  # 스레드 종료 반응성을 위해 600 → 30 권장
                ):
                    if self.stop_event.is_set():
                        break

                    pod: V1Pod = evt.get("object")
                    etype: str = evt.get("type", "")

                    if not pod or etype != "MODIFIED":
                        continue

                    ok, reason = is_pending_unschedulable(pod)
                    if not ok:
                        continue

                    uid = pod.metadata.uid or ""
                    namespace = pod.metadata.namespace
                    service = namespace  # 네 코드 가정 유지

                    now = time.time()

                    # 1) in-flight(쿨타임) 중복 방지
                    if uid in self.in_flight_pods:
                        cool = now - self.in_flight_pods[uid]
                        if cool < IN_FLIGHT_TIMEOUT:
                            continue
                        else:
                            del self.in_flight_pods[uid]

                    self.in_flight_pods[uid] = now

                    # 2) print rate-limit
                    key = (namespace, pod.metadata.name, uid)
                    if key in self.last_print and (now - self.last_print[key]) < PRINT_REPEAT_SECONDS:
                        continue
                    self.last_print[key] = now

                    # resource_version 갱신
                    current_rv = pod.metadata.resource_version

                    try:
                        maxc = sqlite_get_max_container(conn, service)
                    except sqlite3.OperationalError as e:
                        print(f"[warn] sqlite read failed: {e}")
                        maxc = None

                    pod_count = count_running_pods(v1, namespace)

                    print("\n========== PENDING DETECTED ==========")
                    print(f"pod       : {namespace}/{pod.metadata.name}")
                    # print(f"reason    : {reason}")
                    print(f"max_cont  : {maxc}")
                    print(f"pod_count : {pod_count}")

                    # 네 기존 조건 유지 (단 maxc None 방어)
                    if maxc is None:
                        print("[noop] max_container is None")
                        continue

                    if pod_count < maxc:
                        print("[action] eviction planning")
                        plan = evict_mgr.find_eviction_plan(service, pod)

                        if plan:
                            print(f"=== EVICTION PLAN FOUND ({plan['strategy']}) ===")
                            print(f"Target Node : {plan['node']}")
                            for item in plan["evict_list"]:
                                print(f" - Evict {item['count']} pod(s) from {item['service']}")
                            evict_mgr.execute_eviction(plan["node"], plan["evict_list"])
                        else:
                            # 기존 fallback 로직은 그대로 두되, 여기서는 자리만 남겨둠
                            print("[warn] No feasible eviction plan found. (keep your fallback here)")
                            # TODO: 네 fallback(Quota 축소 + pending pod delete) 블록을 그대로 옮기면 됨
                    else:
                        print(f"[noop] pod_count {pod_count} >= max_container {maxc}")

            except Exception as e:
                if self.stop_event.is_set():
                    break
                print(f"[warn] eviction watch error: {e} (retry in 2s)", file=sys.stderr)
                time.sleep(2)

        print("[thread] eviction watcher stopped")


class QuotaReleaserWatcher(threading.Thread):
    def __init__(self, stop_event: threading.Event, quota_name: str = "pod-quota"):
            super().__init__(name="quota-releaser", daemon=True)
            self.stop_event = stop_event
            self.quota_name = quota_name

    def _is_quota_block_event(self, ev_obj) -> bool:
        reason = (ev_obj.reason or "").lower()
        msg = (ev_obj.message or "").lower()
        kind = (ev_obj.involved_object.kind or "").lower()

        if kind not in ("replicaset", "deployment"):
            return False
        if "failedcreate" not in reason:
            return False
        if "exceeded quota" in msg:
            return True
        return False

    def _get_pods_quota(self, v1: client.CoreV1Api, namespace: str) -> Optional[int]:
        rq = v1.read_namespaced_resource_quota(name=self.quota_name, namespace=namespace)
        pods_str = (rq.spec.hard or {}).get("pods")
        if pods_str is None:
            return None
        return int(str(pods_str))

    def _patch_pods_quota(self, v1: client.CoreV1Api, namespace: str, new_quota: int) -> None:
        patch_body = {"spec": {"hard": {"pods": str(new_quota)}}}
        v1.patch_namespaced_resource_quota(
            name=self.quota_name,
            namespace=namespace,
            body=patch_body
        )

    def run(self) -> None:
        api_client = client.ApiClient()
        v1 = client.CoreV1Api(api_client)
        w = watch.Watch()

        field_sel = "reason=FailedCreate"
        cur_rv = v1.list_event_for_all_namespaces(limit=1).metadata.resource_version
        conn = sqlite3.connect(SQLITE_PATH, timeout=5.0, check_same_thread=False)

        print("[thread] quota releaser started")
        while not self.stop_event.is_set():
            try:
                for ev in w.stream(
                    v1.list_event_for_all_namespaces,
                    field_selector=field_sel,
                    timeout_seconds=30,
                ):
                    if self.stop_event.is_set():
                        break

                    obj: client.V1Event = ev.get("object")
                    if not obj:
                        continue
                    cur_rv = obj.metadata.resource_version

                    if not self._is_quota_block_event(obj):
                        continue

                    ns = obj.metadata.namespace
                    src = obj.involved_object.name
                    msg = obj.message or ""

                    try:
                        cur = self._get_pods_quota(v1, ns)
                        if cur is None:
                            print(f"[quota][skip] ns={ns} '{self.quota_name}' has no hard.pods")
                            continue

                        try:
                            maxc = sqlite_get_max_container(conn, ns)
                        except sqlite3.OperationalError as e:
                            print(f"[warn] sqlite read failed: {e}")
                            maxc = None
                        # max container 보다 현재 파드수가 적다면
                        if cur < maxc:
                            new = cur + 1
                        else:
                            continue
                        self._patch_pods_quota(v1, ns, new)
                        print(f"[quota][patch] ns={ns} {self.quota_name}.hard.pods {cur} -> {new} (obj={src})")
                        print(f"             msg={msg}")

                    except Exception as e:
                        print(f"[quota][error] ns={ns} patch failed: {e}")

            except Exception as e:
                if self.stop_event.is_set():
                    break
                print(f"[quota][warn] watch error: {e} (retry in 2s)")
                time.sleep(2)

        print("[thread] quota releaser stopped")


def main() -> None:
    try:
        load_kube_config()
    except Exception as e:
        print(f"[fatal] {e}", file=sys.stderr)
        sys.exit(1)

    print("DB absolute path:", os.path.abspath(SQLITE_PATH))
    print("DB exists?:", os.path.exists(SQLITE_PATH))

    stop_event = threading.Event()

    def _handle_sig(*_):
        stop_event.set()

    signal.signal(signal.SIGINT, _handle_sig)
    signal.signal(signal.SIGTERM, _handle_sig)

    t_evict = EvictionWatcher(stop_event)
    t_quota = QuotaReleaserWatcher(stop_event)  # 아직 stub

    t_evict.start()
    t_quota.start()

    # main thread: liveness + join
    try:
        while not stop_event.is_set():
            time.sleep(0.5)
    finally:
        stop_event.set()
        t_evict.join(timeout=5)
        t_quota.join(timeout=5)
        print("[main] exit")


if __name__ == "__main__":
    main()