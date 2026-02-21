import sqlite3
import math
from datetime import datetime
from kubernetes import client, config

class EvictionManager:
    def __init__(self, db_conn):
        # 쿠버네티스 인증 (로컬 환경 kubeconfig 우선)
        try:
            config.load_kube_config()
        except:
            config.load_incluster_config()
        
        self.v1 = client.CoreV1Api()
        self.conn = db_conn # 외부에서 관리되는 DB 연결 객체

    # ---------- 리소스 파싱 헬퍼 ----------
    def _parse_cpu(self, cpu_str):
        if not cpu_str: return 0
        s = str(cpu_str)
        if s.endswith("m"): return int(s[:-1])
        return int(float(s) * 1000)

    def _parse_mem(self, mem_str):
        if not mem_str: return 0
        units = {"Ki": 1024, "Mi": 1024**2, "Gi": 1024**3, "Ti": 1024**4,
                 "K": 1000, "M": 1000**2, "G": 1000**3, "T": 1000**4}
        s = str(mem_str)
        for u, mul in units.items():
            if s.endswith(u): return int(float(s[:-len(u)]) * mul)
        return int(float(s))

    def _get_pod_res(self, pod):
        """파드 객체(V1Pod)에서 CPU/Mem Request 합계를 추출"""
        cpu, mem = 0, 0
        if not pod.spec or not pod.spec.containers:
            return 0, 0
        for c in pod.spec.containers:
            req = (c.resources.requests or {}) if c.resources else {}
            cpu += self._parse_cpu(req.get("cpu", "0"))
            mem += self._parse_mem(req.get("memory", "0"))
        return cpu, mem

    def _get_node_realtime_free(self, node_name):
        """[핵심] 판단 직전, 해당 노드의 실제 여유 리소스를 API로 즉시 계산"""
        node = self.v1.read_node(node_name)
        allocatable = node.status.allocatable
        
        total_cpu = self._parse_cpu(allocatable.get("cpu", "0"))
        total_mem = self._parse_mem(allocatable.get("memory", "0"))

        # 해당 노드의 모든 파드(Running + Pending) 조회하여 예약된 리소스 합산
        sel = f"spec.nodeName={node_name},status.phase!=Succeeded,status.phase!=Failed"
        pods = self.v1.list_pod_for_all_namespaces(field_selector=sel).items
        
        used_cpu, used_mem = 0, 0
        for p in pods:
            c_cpu, c_mem = self._get_pod_res(p)
            used_cpu += c_cpu
            used_mem += c_mem
        return max(0, total_cpu - used_cpu), max(0, total_mem - used_mem)

    # ---------- 핵심 로직: 서비스 리소스 분석 및 이득 계산 ----------
    def _get_service_gain_on_node(self, node_name, service_name, min_c):
        """
        [분석 핵심] 
        1. 노드 내 해당 서비스 파드들 조회
        2. 임의의 파드(첫 번째)를 잡아 리소스 설정(Request) 분석
        3. 해당노드에서 삭제 가능한 최대 파드수(reducible count) 와 파드당 확보가능한 리소스 리턴
        """
        sel = f"spec.nodeName={node_name},status.phase=Running"
        # pods = self.v1.list_namespaced_pod(namespace=service_name, field_selector=sel).items
        pods = [
            p for p in self.v1.list_namespaced_pod(
                namespace=service_name,
                field_selector=sel
            ).items
            if p.metadata.deletion_timestamp is None
        ]
        
        if not pods:
            return 0, 0, 0
            
        # 전역 실행 수 확인 (정책 준수를 위해)
        #all_running = len(self.v1.list_namespaced_pod(namespace=service_name, field_selector="status.phase=Running").items)
        reducible_count = len(pods)
        # reducible_count = min(len(pods), all_running - min_c)
        # print(f"all running: {all_running},   reducible count: {reducible_count}")
        # if reducible_count <= 0:
        #     return 0, 0, 0
            
        # [사용자 요청 반영] 이 서비스의 실제 파드 하나를 분석하여 리소스 기준점 대입
        p_cpu, p_mem = self._get_pod_res(pods[0])
        
        return p_cpu, p_mem, reducible_count

    # ---------- Eviction 계획 수립 ----------
    def find_eviction_plan(self, trigger_service, pending_pod):
        # 1. 실행하려는 파드의 리소스 요구량 파악
        req_cpu, req_mem = self._get_pod_res(pending_pod)
        print(f"request CPU: {req_cpu}, MEM: {req_mem}")

        # 2. DB에서 Victim 후보(우선순위 순) 및 노드 상태 로드
        candidates = self.conn.execute("""
            SELECT service, min_container FROM service_profile 
            WHERE service != ? ORDER BY t_cold ASC, weight ASC
        """, (trigger_service,)).fetchall()
        
        nodes_dict = {
            row[0]: [row[1], row[2]] 
            for row in self.conn.execute("SELECT node_name, cpu_free_m, mem_free_bytes FROM node_resource_status").fetchall()
        }
        # nodes = self.conn.execute("SELECT node_name, cpu_free_m, mem_free_bytes FROM node_resource_status").fetchall()

        # [Level 1] 단일 서비스 하나만으로 해결 가능한 노드가 있는지 전수 조사
        for service_name, min_c in candidates:
            all_running = len(self.v1.list_namespaced_pod(namespace=service_name, field_selector="status.phase=Running").items)
            for node_name, res in nodes_dict.items():  # 노드별 루프를 돌기위해 사용

                cpu_free, mem_free = self._get_node_realtime_free(node_name)
                gain_cpu, gain_mem, reducible_count = self._get_service_gain_on_node(node_name, service_name, min_c)
                print(f"candidate: SERVICE: {service_name}  NODE: {node_name} reduce cnt: {reducible_count}")
                # 삭제가능한 파드가 없다면 이번 노드는 제외
                if reducible_count == 0:
                    continue
                # 자원확보를 위해 필요한 최소 노드수 개산
                count = max(math.ceil((req_cpu - cpu_free)/gain_cpu), math.ceil((req_mem - mem_free)/gain_mem))
                # 유효성검증
                if count > reducible_count:
                    continue
                if all_running - count < min_c:
                    continue

                res[0] = cpu_free  # db 값을 안쓰고 nodes_dict[node_name][0] 을 실시간 데이터로 갱신
                res[1] = mem_free  # db 값을 안쓰고 nodes_dict[node_name][1] 을 실시간 데이터로 갱신

                # if (cpu_free + gain_cpu >= req_cpu) and (mem_free + gain_mem >= req_mem):
                return {
                    "strategy": "Single Service",
                    "node": node_name,
                    "evict_list": [{"service": service_name, "count": count}]
                }

        # [Level 2] 단일로 안될 경우, 우선순위 순으로 누적(1순위 + 2순위...)하여 조사
        # node_states = {n[0]: {"cpu": n[1], "mem": n[2], "plan": []} for n in nodes}
        node_states = {name: {"cpu": res[0], "mem": res[1], "plan": []} for name, res in nodes_dict.items()}
        for service_name, min_c in candidates:
            for node_name in node_states.keys():
                state = node_states[node_name]
                gain_cpu, gain_mem, count = self._get_service_gain_on_node(node_name, service_name, min_c)
                
                if count > 0:
                    state["cpu"] += gain_cpu
                    state["mem"] += gain_mem
                    state["plan"].append({"service": service_name, "count": count})

                if state["cpu"] >= req_cpu and state["mem"] >= req_mem:
                    return {
                        "strategy": "Cumulative Services",
                        "node": node_name,
                        "evict_list": state["plan"]
                    }
        
        return None

    def execute_eviction(self, node_name, evict_list):
        """
        계획된 리스트에 따라 실제 파드를 삭제함.
        """
        for item in evict_list:
            service_name = item['service']
            needed_count = item['count']
            
            # 1. 해당 노드에 있는 해당 서비스의 Running 파드 목록 가져오기
            # (서비스명이 곧 네임스페이스인 구조 반영)
            sel = f"spec.nodeName={node_name},status.phase=Running"
            try:
                # pods = self.v1.list_namespaced_pod(namespace=service_name, field_selector=sel).items
                pods = [
                    p for p in self.v1.list_namespaced_pod(
                        namespace=service_name,
                        field_selector=sel
                    ).items
                    if p.metadata.deletion_timestamp is None
                ]
                # 삭제 후 다시 재기동 되는 현상 방지를 위해 삭제 이전에 현재 실행중인 파드수 - victim 파드 수로 쿼타 조정 (Min/max는 건드리지 않음)
                global_pods = [
                    p for p in self.v1.list_namespaced_pod(
                        namespace=service_name,
                        field_selector="status.phase=Running"
                    ).items
                    if p.metadata.deletion_timestamp is None
                ]
                pods_counts = len(global_pods)
                quota = pods_counts - needed_count
                print(f"!!!!evict!!!! pod_count: {pods_counts}, needed_count: {needed_count}")
                patch_body = {
                                "spec": {
                                    "hard": {
                                        "pods": str(quota)
                                    }
                                }
                            }
                self.v1.patch_namespaced_resource_quota(
                    name="pod-quota", 
                    namespace=service_name, 
                    body=patch_body
                )
                evicted_count = 0
                print(f"evict pod count: {len(pods)}")
                for pod in pods:
                    if evicted_count >= needed_count:
                        break
                    
                    # 2. 파드 삭제 실행 (Graceful 옵션 적용)
                    print(f"  [exec] Deleting pod {pod.metadata.name} (GracePeriod: 3s)...")
                    self.v1.delete_namespaced_pod(
                        name=pod.metadata.name,
                        namespace=service_name,
                        # 3초 동안 기존 요청 처리 시간을 보장
                        body=client.V1DeleteOptions(grace_period_seconds=1)
                    )
                    evicted_count += 1
                
                print(f"[success] Evicted {evicted_count} pods from {service_name} on {node_name}")
            
            except Exception as e:
                print(f"[error] Failed to evict pods for {service_name}: {e}")