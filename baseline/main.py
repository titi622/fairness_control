#!/usr/bin/env python3
### 쓰레드 분리 테스트 ###
import os
import sys
import time
import sqlite3
import threading
import signal
import json
import subprocess
from datetime import datetime, timezone
from typing import Optional, Tuple, Dict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from kubernetes import client, config, watch
from kubernetes.client import V1Pod
from kubernetes.client.rest import ApiException

from eviction.eviction_manager import EvictionManager

# ---- Config ----
SQLITE_PATH = "/home/ubuntu/fairness_control/trace_store.db"
SERVICE_TABLE = "service_profile"
MAXCOL = "max_container"
SERVICECOL = "service"
PENDING_MIN_SECONDS = float(1)
PRINT_REPEAT_SECONDS = float(5)

IN_FLIGHT_TIMEOUT = 5  # seconds

TRIGGER_HOST = "0.0.0.0"
TRIGGER_PORT = 9999
TRIGGER_COOLDOWN_SECONDS = 2.0  # 같은 서비스에 대해 너무 자주 patch되는 것 방지
TRIGGER_DELETE_KSERVICE = True  # 트리거 시 ksvc 삭제 여부

TARGET_POD_QUOTA = {
    "small-fast": 67,
    "small-fast2": 67,
    "medium-fast": 41,
    "medium-slow": 41,
    "large": 14,
}

SERVICE_YAML_PATH = {
    "small-fast": "/home/ubuntu/fairness_control/services/small.fast/small_fast.yaml",
    "small-fast2": "/home/ubuntu/fairness_control/services/small.fast2/small_fast2.yaml",
    "medium-fast": "/home/ubuntu/fairness_control/services/medium.fast/medium_fast.yaml",
    "medium-slow": "/home/ubuntu/fairness_control/services/medium.slow/medium_slow.yaml",
    "large": "/home/ubuntu/fairness_control/services/large/large.yaml",
}
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

        def _refresh_rv() -> str:
            return v1.list_pod_for_all_namespaces(limit=1).metadata.resource_version
        current_rv = _refresh_rv()

        # list_res = v1.list_pod_for_all_namespaces(limit=1)
        # current_rv = list_res.metadata.resource_version

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

                    if getattr(pod, "metadata", None) and pod.metadata.resource_version:
                        current_rv = pod.metadata.resource_version

                    uid = pod.metadata.uid or ""
                    namespace = pod.metadata.namespace
                    service = namespace  
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

                    # try:
                    #     maxc = sqlite_get_max_container(conn, service)
                    # except sqlite3.OperationalError as e:
                    #     print(f"[warn] sqlite read failed: {e}")
                    #     maxc = None

                    pod_count = count_running_pods(v1, namespace)

                    print("\n========== PENDING DETECTED ==========")
                    print(f"pod       : {namespace}/{pod.metadata.name}")
                    # print(f"reason    : {reason}")
                    # print(f"max_cont  : {maxc}")
                    print(f"pod_count : {pod_count}")

                    # 네 기존 조건 유지 (단 maxc None 방어)
                    # if maxc is None:
                    #     print("[noop] max_container is None")
                    #     continue

                    # if maxc ==0 : # or pod_count < maxc:
                    ## eviction 로직 직전에 진짜 이 Pod가 아직도 pending 중인지 확인
                    pod_tmp = v1.read_namespaced_pod(name=pod.metadata.name, namespace=namespace)
                    if pod_tmp.status.phase == "Pending":
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
                    # else:
                    #     # print(f"[noop] pod_count {pod_count} >= max_container {maxc}")
                    #     print(f"[noop] pod_count is 0")

            except ApiException as e:
                if self.stop_event.is_set():
                    break
                if e.status == 410:
                    current_rv = _refresh_rv()
                    print(f"[warn] eviction watch expired(410). reset rv -> {current_rv}")
                    continue
                print(f"[warn] eviction watch api error: {e} (retry in 2s)", file=sys.stderr)
                time.sleep(2)

            except Exception as e:
                if self.stop_event.is_set():
                    break
                print(f"[warn] eviction watch error: {e} (retry in 2s)", file=sys.stderr)
                time.sleep(2)

        print("[thread] eviction watcher stopped")



class TriggerRequestHandler(BaseHTTPRequestHandler):
    server_version = "quota-trigger/1.0"

    def _send_json(self, status_code: int, payload: dict) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        return

    def do_POST(self):
        if self.path != "/trigger":
            self._send_json(404, {"ok": False, "error": "not found"})
            return

        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length)

        try:
            data = json.loads(raw.decode("utf-8"))
        except Exception:
            self._send_json(400, {"ok": False, "error": "invalid json"})
            return

        service = str(data.get("service", "")).strip()
        ksvc = str(data.get("ksvc", service)).strip()

        if not service:
            self._send_json(400, {"ok": False, "error": "service is required"})
            return

        ok, msg = self.server.thread_obj.handle_trigger(service, ksvc)
        if ok:
            self._send_json(200, {"ok": True, "service": service, "ksvc": ksvc, "message": msg})
        else:
            self._send_json(400, {"ok": False, "service": service, "ksvc": ksvc, "error": msg})



class TriggerHTTPServer(ThreadingHTTPServer):
    def __init__(self, server_address, RequestHandlerClass, thread_obj):
        super().__init__(server_address, RequestHandlerClass)
        self.thread_obj = thread_obj


class TriggerServerThread(threading.Thread):
    """
    외부에서 POST /trigger {"service":"medium-fast"} 를 받으면
    해당 namespace의 pod quota를 즉시 목표값으로 patch
    """
    def __init__(self, stop_event: threading.Event, host: str = TRIGGER_HOST, port: int = TRIGGER_PORT,
                 quota_name: str = "pod-quota"):
        super().__init__(name="trigger-server", daemon=True)
        self.stop_event = stop_event
        self.host = host
        self.port = port
        self.quota_name = quota_name
        self.last_trigger_times: Dict[str, float] = {}
        self.lock = threading.Lock()
        self.httpd: Optional[TriggerHTTPServer] = None

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
    
    def _delete_ksvc(self, namespace: str, ksvc_name: str) -> None:
        api = client.CustomObjectsApi(client.ApiClient())
        v1 = client.CoreV1Api(client.ApiClient())

        # original_quota = None

        # # 1) 현재 pods quota 백업
        # original_quota = self._get_pods_quota(v1, namespace)
        # print(f"[trigger] original pods quota in ns {namespace}: {original_quota}")

        # # 2) 삭제 전에 quota를 0으로 낮춰서 새 pod 생성 차단
        # if original_quota is not None:
        #     self._patch_pods_quota(v1, namespace, 0)
        #     print(f"[trigger] patched pods quota to 0 in ns {namespace}")



        api.delete_namespaced_custom_object(
            group="serving.knative.dev",
            version="v1",
            namespace=namespace,
            plural="services",
            name=ksvc_name
        )
        # time.sleep(2)  
        # # 바로 안지워 질수있으니 딜리트로 한번더 사살
        # subprocess.run([
        #     "kubectl", "delete", "pods", "--all",
        #     "-n", namespace,
        #     "--force",
        #     "--grace-period=0"
        # ], check=False)
        # time.sleep(2)

        # if original_quota is not None:
        #     self._patch_pods_quota(v1, namespace, original_quota)
        # pods = v1.list_namespaced_pod(namespace=namespace).items
        # for pod in pods:
        #     try:
        #         v1.delete_namespaced_pod(
        #             name=pod.metadata.name,
        #             namespace=namespace,
        #             grace_period_seconds=0
        #         )
        #         print(f"[trigger] deleted pod {pod.metadata.name} in ns {namespace} after ksvc delete")
        #         time.sleep(0.1)  
        #     except Exception as e:
        #         print(f"[WARN] failed to delete pod {pod.metadata.name}: {e}")

    def _reduce_quota_and_delete_pods(self, namespace: str, delete_count: int = 4) -> None:
        api_client = client.ApiClient()
        v1 = client.CoreV1Api(api_client)

        # 1) 현재 quota 조회
        rq = v1.read_namespaced_resource_quota(
            name=self.quota_name,
            namespace=namespace
        )

        pods_str = (rq.spec.hard or {}).get("pods")
        if pods_str is None:
            raise RuntimeError(f"'pods' quota not found in namespace={namespace}")

        cur_quota = int(str(pods_str))
        new_quota = max(0, cur_quota - delete_count)

        # 2) quota 감소 patch
        patch_body = {
            "spec": {
                "hard": {
                    "pods": str(new_quota)
                }
            }
        }

        v1.patch_namespaced_resource_quota(
            name=self.quota_name,
            namespace=namespace,
            body=patch_body
        )
        print(f"[trigger][quota] ns={namespace} pods {cur_quota} -> {new_quota}")

        time.sleep(1)

        # 3) pod 목록 조회
        pods = v1.list_namespaced_pod(namespace=namespace).items

        # 종료 대상 제외하고 Running 위주로 정렬
        candidate_pods = [
            pod for pod in pods
            if pod.metadata.deletion_timestamp is None
        ]

        # Running 파드 우선 삭제
        candidate_pods.sort(
            key=lambda p: (
                0 if p.status.phase == "Running" else 1,
                p.metadata.creation_timestamp
            )
        )

        # 4) 최대 delete_count개 삭제
        deleted = 0
        for pod in candidate_pods[:delete_count]:
            try:
                v1.delete_namespaced_pod(
                    name=pod.metadata.name,
                    namespace=namespace,
                    grace_period_seconds=0
                )
                deleted += 1
                print(f"[trigger][pod-delete] ns={namespace} pod={pod.metadata.name}")
                time.sleep(0.1)
            except Exception as e:
                print(f"[WARN] failed to delete pod {pod.metadata.name}: {e}")

        print(f"[trigger][done] ns={namespace} requested_delete={delete_count}, actual_deleted={deleted}")

    def _restore_quota(self, namespace: str, quota_count: int = 4) -> None:
        api_client = client.ApiClient()
        v1 = client.CoreV1Api(api_client)

        # 2) quota 원복
        patch_body = {
            "spec": {
                "hard": {
                    "pods": str(quota_count)
                }
            }
        }

        v1.patch_namespaced_resource_quota(
            name=self.quota_name,
            namespace=namespace,
            body=patch_body
        )
        print(f"[trigger][quota] ns={namespace} pods quota_count {quota_count}")

        time.sleep(1)

        # 3) pod 목록 조회
        pods = v1.list_namespaced_pod(namespace=namespace).items

        # 종료 대상 제외하고 Running 위주로 정렬
        candidate_pods = [
            pod for pod in pods
            if pod.metadata.deletion_timestamp is None
        ]

        # Running 파드 우선 삭제
        candidate_pods.sort(
            key=lambda p: (
                0 if p.status.phase == "Running" else 1,
                p.metadata.creation_timestamp
            )
        )

        # 4) 파드 하나 삭제
        deleted = 0
        for pod in candidate_pods[:1]:
            try:
                v1.delete_namespaced_pod(
                    name=pod.metadata.name,
                    namespace=namespace,
                    grace_period_seconds=0
                )
                deleted += 1
                print(f"[trigger][pod-delete] ns={namespace} pod={pod.metadata.name}")
                time.sleep(0.1)
            except Exception as e:
                print(f"[WARN] failed to delete pod {pod.metadata.name}: {e}")

        print(f"[trigger][done] ns={namespace} restored")
    
    def handle_trigger(self, service: str, ksvc_name: str) -> Tuple[bool, str]:
        if service not in TARGET_POD_QUOTA:
            return False, f"unknown service: {service}"


        v1 = client.CoreV1Api(client.ApiClient())
        new_quota = TARGET_POD_QUOTA[service]

        try:
            cur_quota = self._get_pods_quota(v1, service)
        except ApiException as e:
            return False, f"read quota failed: {e}"

        try:
            if cur_quota != new_quota:
                self._patch_pods_quota(v1, service, new_quota)
                print(f"[trigger][patch] ns={service} {self.quota_name}.hard.pods {cur_quota} -> {new_quota}")
            else:
                print(f"[trigger][noop] ns={service} already pods={cur_quota}")
        except ApiException as e:
            return False, f"patch quota failed: {e}"
        except Exception as e:
            return False, f"unexpected quota patch error: {e}"

        if ksvc_name == "create":
            # yaml apply
            yaml_path = SERVICE_YAML_PATH.get(service)
            if not yaml_path:
                return False, f"no yaml path defined for {service}"

            try:
                subprocess.run([
                    "kubectl", "apply",
                    "-f", yaml_path
                ], check=True)

                print(f"[trigger][create] applied {yaml_path}")
                return True, f"quota {cur_quota}->{new_quota}, applied {yaml_path}"

            except subprocess.CalledProcessError as e:
                return False, f"kubectl apply failed: {e}"

        elif ksvc_name == "delete":
            # 기존 ksvc delete 로직 유지
            if TRIGGER_DELETE_KSERVICE:
                try:
                    self._delete_ksvc(service, service)
                    print(f"[trigger][delete] ksvc {service}/{service}")
                except ApiException as e:
                    if e.status == 404:
                        return False, f"ksvc not found: {service}/{service}"
                    return False, f"delete ksvc failed: {e}"
                except Exception as e:
                    return False, f"unexpected ksvc delete error: {e}"

            if cur_quota != new_quota:
                return True, f"patched quota {cur_quota}->{new_quota}, deleted ksvc {service}/{service}"
            return True, f"quota already {new_quota}, deleted ksvc {service}/{service}"
        elif ksvc_name == "reduce":
            # 기존 ksvc delete 로직 유지
            if TRIGGER_DELETE_KSERVICE:
                try:
                    self._reduce_quota_and_delete_pods(service, delete_count=4)
                    print(f"[trigger][reduce] ksvc {service}/{service}")
                except ApiException as e:
                    if e.status == 404:
                        return False, f"ksvc not found: {service}/{service}"
                    return False, f"reduce ksvc failed: {e}"
                except Exception as e:
                    return False, f"unexpected ksvc reduce error: {e}"

            if cur_quota != new_quota:
                return True, f"patched quota {cur_quota}->{new_quota}, reduced ksvc {service}/{service}"
            return True, f"quota already {new_quota}, reduced ksvc {service}/{service}"
        elif ksvc_name == "restore":
            # 기존 ksvc delete 로직 유지
            if TRIGGER_DELETE_KSERVICE:
                try:
                    self._restore_quota(service, quota_count=67)
                    print(f"[trigger][restore] ksvc {service}/{service}")
                except ApiException as e:
                    if e.status == 404:
                        return False, f"ksvc not found: {service}/{service}"
                    return False, f"restore ksvc failed: {e}"
                except Exception as e:
                    return False, f"unexpected ksvc restore error: {e}"

            if cur_quota != new_quota:
                return True, f"patched quota {cur_quota}->{new_quota}, restored ksvc {service}/{service}"
            return True, f"quota already {new_quota}, restored ksvc {service}/{service}"

    def run(self) -> None:
        print(f"[thread] trigger server started on {self.host}:{self.port}")
        self.httpd = TriggerHTTPServer((self.host, self.port), TriggerRequestHandler, self)
        self.httpd.timeout = 1.0

        while not self.stop_event.is_set():
            self.httpd.handle_request()

        try:
            self.httpd.server_close()
        except Exception:
            pass

        print("[thread] trigger server stopped")

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

    # t_evict = EvictionWatcher(stop_event)
    # t_quota = QuotaReleaserWatcher(stop_event)  # 아직 stub
    t_trigger = TriggerServerThread(stop_event, host=TRIGGER_HOST, port=TRIGGER_PORT)

    # t_evict.start()
    # t_quota.start()
    t_trigger.start()

    # main thread: liveness + join
    try:
        while not stop_event.is_set():
            time.sleep(0.5)
    finally:
        stop_event.set()
        t_evict.join(timeout=5)
        # t_quota.join(timeout=5)
        t_trigger.join(timeout=5)
        print("[main] exit")


if __name__ == "__main__":
    main()