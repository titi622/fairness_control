#### eviction 타이밍개선 ####
#!/usr/bin/env python3
import os
import sys
import time
import sqlite3
from datetime import datetime, timezone
from typing import Optional, Tuple, Dict
from eviction.eviction_manager import EvictionManager
import json

from kubernetes import client, config, watch
from kubernetes.client import V1Pod


# ---- Config ----
SQLITE_PATH = "/home/ubuntu/fairness_control/trace_store.db"  # 네 sqlite 파일 경로로 맞추기
SERVICE_TABLE = "service_profile"
MAXCOL = "max_container"  # 컬럼명: max_container 라고 가정
SERVICECOL = "service"  # 컬럼명: service 라고 가정

PENDING_MIN_SECONDS = float(1)
PRINT_REPEAT_SECONDS = float(5)

in_flight_pods: Dict[str, float] = {} # {uid: timestamp}
IN_FLIGHT_TIMEOUT = 5 # 이빅션 후 30초 동안만 중복 방지



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
        # print(c.type, c.status)
        if c.type == "PodScheduled" and c.status == "False":
            reason = (c.reason or "").strip()
            msg = (c.message or "").strip()

            # print(f"reason: {reason} \n msg:{msg}")

            # 스케줄 실패(리소스 부족/노드 부족 등)만 트리거로 인정
            if reason in ("Unschedulable", "SchedulingDisabled"):
                return (True, f"{reason}: {msg}")
            # if "Insufficient" in msg or "nodes are available" in msg or msg.startswith("0/"):
            #     return (True, f"NotScheduled: {msg}")

    return (False, "Pending (no PodScheduled detail)")


def sqlite_get_max_container(conn: sqlite3.Connection, service: str) -> Optional[int]:
    """
    service_profile 테이블에 service, max_container 컬럼이 있다고 가정.
    컬럼명이 다르면 env로 SERVICE_TABLE/MAXCOL/SERVICECOL 변경.
    """
    q = f"SELECT {MAXCOL} FROM {SERVICE_TABLE} WHERE {SERVICECOL} = ? LIMIT 1"
    cur = conn.execute(q, (service,))
    row = cur.fetchone()
    if not row:
        return None
    try:
        return int(row[0])
    except Exception:
        return None


def count_pods_for_service(v1: client.CoreV1Api, namespace: str, service: str) -> int:
    try:
        # 러닝상태인 파드만 추려야하는지 전체를 봐야하는지 테스트 --> 러닝상태만 봐야함. 왜냐면 이빅션 실행 기준이 맥스 값보다 실행중인 파드수가 적을때이기 때문임.
        # pods = v1.list_namespaced_pod(namespace=namespace, field_selector="status.phase=Running").items
        pods = [
                    p for p in v1.list_namespaced_pod(
                        namespace=namespace,
                        field_selector="status.phase=Running"
                    ).items
                    if p.metadata.deletion_timestamp is None
                ]
        # pods = v1.list_namespaced_pod(namespace=namespace).item
        return len(pods)
    except Exception:
        return 0


def evict(service: str, namespace: str, max_container: int, pod_count: int, reason: str) -> None:
    """
    TODO: eviction 로직 연결 (지금은 공란 placeholder)
    """
    # 예: 특정 revision pod delete, priority 낮은 서비스부터 delete, etc.
    return

#### 이벤트 메시지 출력용 ###
api_client = client.ApiClient()

def dump_evt(evt):
    etype = evt.get("type")
    # raw_object가 있으면 이게 진짜 "전문"에 가장 가까움
    raw = evt.get("raw_object")

    if raw is not None:
        payload = {"type": etype, "raw_object": raw}
    else:
        # V1Pod 같은 모델 객체를 dict로 변환
        obj = evt.get("object")
        payload = {
            "type": etype,
            "object": api_client.sanitize_for_serialization(obj) if obj is not None else None,
            # 다른 키들도 같이 보고 싶으면 evt 자체를 얕게 덧붙여도 됨
            "keys": list(evt.keys()),
        }

    print(json.dumps(payload, indent=2, ensure_ascii=False))
    print("-" * 80)

def main() -> None:
    try:
        load_kube_config()
    except Exception as e:
        print(f"[fatal] {e}", file=sys.stderr)
        sys.exit(1)

    print("DB absolute path:", os.path.abspath(SQLITE_PATH))
    print("DB exists?:", os.path.exists(SQLITE_PATH))
    v1 = client.CoreV1Api()
    w = watch.Watch()

    # sqlite는 watch loop에서 자주 조회하니 connection 1개 유지
    conn = sqlite3.connect(SQLITE_PATH, timeout=5.0)

    # (ns, podname, uid) -> last_print_ts
    last_print: Dict[Tuple[str, str, str], float] = {}
    evict_mgr = EvictionManager(conn)

    print("[watch] pending→sqlite(max_container)→evict-gate started (all namespaces)")
    print(f"[cfg] SQLITE_PATH={SQLITE_PATH}, table={SERVICE_TABLE}, maxcol={MAXCOL}")
    print(f"[cfg] PENDING_MIN_SECONDS={PENDING_MIN_SECONDS}, PRINT_REPEAT_SECONDS={PRINT_REPEAT_SECONDS}")

    list_res = v1.list_pod_for_all_namespaces(limit=1)
    current_rv = list_res.metadata.resource_version
    while True:
        try:
            for evt in w.stream(v1.list_pod_for_all_namespaces,field_selector="status.phase=Pending", resource_version=current_rv, timeout_seconds=600):
                # dump_evt(evt)
                print("================================================")
                pod: V1Pod = evt["object"]
                etype: str = evt.get("type", "")

                if etype != "MODIFIED":
                    continue

                ok, reason = is_pending_unschedulable(pod)
                if not ok:
                    continue

                print(pod.status.phase, "/", pod.metadata.namespace, "/", pod.metadata.name, "/", pod.metadata.uid)
                # print(f"reason: {reason}")

                # 🔴 1. 삭제된 파드는 추적 목록에서 제거
                uid = pod.metadata.uid
                namespace = pod.metadata.namespace
                service = namespace
                
                # 🔴 2. 이미 처리 중인 파드인지 확인 (최근 5초 이내)
                now = time.time()
                if uid in in_flight_pods:
                    cooltime = now - in_flight_pods[uid]
                    if cooltime < IN_FLIGHT_TIMEOUT:
                        print("so fast: ", cooltime)
                        continue # 너무 빨리 돌아오는 이벤트 무시
                    else:
                        del in_flight_pods[uid] # 타임아웃 지났으면 제거
                        print("enough slow : ", cooltime)
                
                in_flight_pods[uid] = now

                # ok, reason = is_pending_unschedulable(pod)
                # if not ok:
                #     continue
                # # print(reason)

                # age = pod_age_seconds(pod)
                # if age < PENDING_MIN_SECONDS:
                #     print("we don't need eviction!!")
                #     continue

                uid = pod.metadata.uid or ""
                key = (pod.metadata.namespace, pod.metadata.name, uid)
                now = time.time()
                if key in last_print and (now - last_print[key]) < PRINT_REPEAT_SECONDS:
                    continue
                last_print[key] = now

                try:
                    maxc = sqlite_get_max_container(conn, service)
                except sqlite3.OperationalError as e:
                    print(f"[warn] sqlite read failed: {e}")
                    maxc = None

                pod_count = count_pods_for_service(v1, namespace, service)

                ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                print("\n=== PENDING DETECTED ===")
                # print(f"ts        : {ts}")
                print(f"pod       : {namespace}/{pod.metadata.name}")
                # print(f"service   : {service} (assumed: service==namespace)")
                # print(f"reason    : {reason}")
                print(f"max_cont  : {maxc}")
                print(f"pod_count : {pod_count}")

                current_rv = pod.metadata.resource_version

                # ✅ 요청한 조건 그대로: 현재 파드 수가 max_container 보다 "작으면" eviction 수행
                if pod_count < maxc:
                    print("[action] call evict()")
                    in_flight_pods[uid] = now

                    # 🔴 [추가/병합] 기존 evict() 호출 대신 상세 계획 수립 로직 진입
                    # 1. 팬딩된 파드의 리소스를 분석하고 최적의 노드와 타겟 서비스를 찾습니다.
                    plan = evict_mgr.find_eviction_plan(service, pod)

                    if plan:
                        # 🔴 [추가] 계획이 성공적으로 수립된 경우의 출력 및 처리
                        print(f"=== EVICTION PLAN FOUND ({plan['strategy']}) ===")
                        print(f"Target Node : {plan['node']}")
                        for item in plan['evict_list']:
                            # 이 서비스의 실제 파드 리퀘스트를 분석하여 계산된 결과입니다.
                            print(f" - Action: Evict {item['count']} pod(s) from service: {item['service']}")
                        
                        # TODO: 여기서 실제 삭제 로직을 호출하게 됩니다.
                        evict_mgr.execute_eviction(plan['node'], plan['evict_list'])
                    else:
                        # 🔴 모든 후보를 동원해도 자리가 안 나는 경우 내 서비스 min 컨테이너 수 까지 팬딩 파드들을 삭제한다. 단, 최소 1개는 보장.
                        print("[warn] No feasible eviction plan found to satisfy resource requirements.")
                        # DB에서 min_container 정보 가져오기
                        cur = conn.execute(f"SELECT min_container FROM {SERVICE_TABLE} WHERE {SERVICECOL} = ?", (service,))
                        m_row = cur.fetchone()
                        min_c = int(m_row[0]) if m_row else 0

                        # 1. 현재 설정된 ResourceQuota ("pod-quota") 읽기
                        # 사용자님의 명령어에 명시된 'pod-quota' 이름을 사용합니다.
                        quota_name = "pod-quota"
                        current_quota = v1.read_namespaced_resource_quota(name=quota_name, namespace=namespace)
                        
                        # 현재 설정된 hard pods 값 추출
                        current_hard_pods = int(current_quota.spec.hard.get("pods", 0))
                        
                        # DB에서 최소 유지 기준(min_container) 확인
                        cur = conn.execute(f"SELECT min_container FROM {SERVICE_TABLE} WHERE {SERVICECOL} = ?", (service,))
                        m_row = cur.fetchone()
                        min_c = int(m_row[0]) if m_row else 1 # 기본값 1

                        # 2. 만약 내 최소값(최대값)보다 현재 파드가 많다면 애초에 최소값 만큼만 생성될수 있으므로 현재 설정값에서 1을 줄여나가면서, min_container 까지 쿼터를 축소
                        if current_hard_pods > min_c:
                            new_hard_pods = current_hard_pods - 1
                            
                            # 3. Patch 명령어 수행 (kubectl patch ... --type='merge' 와 동일)
                            patch_body = {
                                "spec": {
                                    "hard": {
                                        "pods": str(new_hard_pods)
                                    }
                                }
                            }
                            v1.patch_namespaced_resource_quota(
                                name=quota_name, 
                                namespace=namespace, 
                                body=patch_body
                            )
                            print(f"[success] Patched ResourceQuota '{quota_name}': {current_hard_pods} -> {new_hard_pods}")

                            # 4. 현재 Pending 파드 삭제 (Quota가 줄었으므로 다시 생성되지 않음)
                            v1.delete_namespaced_pod(name=pod.metadata.name, namespace=namespace)
                            print(f"[success] Deleted pending pod: {pod.metadata.name}")
                        #만약 그게아니라 정말 리소스가 없어서 내 최소값도 못 맞추는 경우라면, 그냥 팬딩상태로 둔다. 왜냐면 누군가 리소스를 반납하면 바로떠야하니까

                else:
                    print(f"[noop] {namespace}/{pod.metadata.name} Pending, but pod_count {pod_count} >= max_container {maxc}")

                print("========================\n")


        except Exception as e:
            print(f"[warn] watch error: {e} (retry in 2s)", file=sys.stderr)
            time.sleep(2)


if __name__ == "__main__":
    main()
