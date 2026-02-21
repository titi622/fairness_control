import sqlite3
from datetime import datetime
from kubernetes import client, config

class NodeResourceManager:
    def __init__(self, sqlite_conn: sqlite3.Connection):
        self.conn = sqlite_conn
        self._prepare_table()


        try:
            config.load_kube_config()
            print("성공: 로컬 kube-config를 로드했습니다.")
        except Exception as e:
            print(f"오류: kube-config를 찾을 수 없거나 로드에 실패했습니다: {e}")
            raise 
        self.v1 = client.CoreV1Api()

    def _prepare_table(self):
        """데이터 저장용 테이블 생성 (최초 1회)"""
        query = """
        CREATE TABLE IF NOT EXISTS node_resource_status (
            node_name TEXT PRIMARY KEY,
            cpu_allocatable_m INTEGER,
            cpu_request_total_m INTEGER,
            cpu_free_m INTEGER,
            mem_allocatable_bytes INTEGER,
            mem_request_total_bytes INTEGER,
            mem_free_bytes INTEGER,
            last_updated DATETIME
        )
        """
        self.conn.execute(query)
        self.conn.commit()

    @staticmethod
    def _parse_cpu(cpu_str: str) -> int:
        if not cpu_str: return 0
        if str(cpu_str).endswith("m"): return int(cpu_str[:-1])
        return int(float(cpu_str) * 1000)

    @staticmethod
    def _parse_mem(mem_str: str) -> int:
        if not mem_str: return 0
        units = {"Ki": 1024, "Mi": 1024**2, "Gi": 1024**3, "Ti": 1024**4,
                 "K": 1000, "M": 1000**2, "G": 1000**3, "T": 1000**4}
        s = str(mem_str)
        for u, mul in units.items():
            if s.endswith(u):
                return int(float(s[:-len(u)]) * mul)
        return int(float(s))

    def _get_node_allocated_resource(self, node_name: str):
        """특정 노드에 배치된 모든 파드의 리소스 Request 합산"""
        # Succeeded(성공), Failed(실패) 상태인 파드는 리소스를 점유하지 않음
        field_selector = f"spec.nodeName={node_name},status.phase!=Succeeded,status.phase!=Failed"
        pods = self.v1.list_pod_for_all_namespaces(field_selector=field_selector).items
        
        cpu_sum = 0
        mem_sum = 0
        for pod in pods:
            if not pod.spec.containers: continue
            for container in pod.spec.containers:
                req = (container.resources.requests or {}) if container.resources else {}
                cpu_sum += self._parse_cpu(req.get("cpu", "0"))
                mem_sum += self._parse_mem(req.get("memory", "0"))
        return cpu_sum, mem_sum

    def sync_cluster_nodes_to_db(self):
        """
        클러스터의 모든 노드를 조회하여 리소스 상태를 DB에 동기화
        """
        # 1. 현재 클러스터의 모든 노드 목록 조회
        try:
            node_list = self.v1.list_node().items
        except Exception as e:
            print(f"노드 목록 조회 실패: {e}")
            return
        
        synced_count = 0

        for node in node_list:
            node_name = node.metadata.name

            labels = node.metadata.labels or {}
            if "node-role.kubernetes.io/control-plane" in labels or "node-role.kubernetes.io/master" in labels:
                # print(f"[skip] 마스터 노드 제외: {node_name}") # 필요시 로그 해제
                continue

            allocatable = node.status.allocatable
            
            # 노드 전체 할당 가능 용량 (Capacity - K8s System Reserved)
            cpu_total = self._parse_cpu(allocatable.get("cpu", "0"))
            mem_total = self._parse_mem(allocatable.get("memory", "0"))
            
            # 해당 노드에서 현재 사용(예약) 중인 리소스 계산
            cpu_used, mem_used = self._get_node_allocated_resource(node_name)
            
            # 순수 가용량 계산
            cpu_free = max(0, cpu_total - cpu_used)
            mem_free = max(0, mem_total - mem_used)
            
            # 2. DB 업데이트 (UPSERT: 있으면 업데이트, 없으면 삽입)
            query = """
            INSERT INTO node_resource_status (
                node_name, cpu_allocatable_m, cpu_request_total_m, cpu_free_m,
                mem_allocatable_bytes, mem_request_total_bytes, mem_free_bytes, last_updated
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(node_name) DO UPDATE SET
                cpu_allocatable_m=excluded.cpu_allocatable_m,
                cpu_request_total_m=excluded.cpu_request_total_m,
                cpu_free_m=excluded.cpu_free_m,
                mem_allocatable_bytes=excluded.mem_allocatable_bytes,
                mem_request_total_bytes=excluded.mem_request_total_bytes,
                mem_free_bytes=excluded.mem_free_bytes,
                last_updated=excluded.last_updated
            """
            self.conn.execute(query, (
                node_name, cpu_total, cpu_used, cpu_free,
                mem_total, mem_used, mem_free, datetime.now()
            ))
            self.conn.commit()
        
        print(f"[{datetime.now()}] 총 {synced_count}개 워커 노드의 상태 동기화 완료.")