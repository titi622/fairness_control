from kubernetes import client, config


class K8sPodCollector:
    def __init__(self):
        try:
            config.load_kube_config()
        except Exception:
            config.load_incluster_config()

        self.v1 = client.CoreV1Api()

    def get_service_info(self):
        pod_data = []

        # default 제외한 모든 namespace 조회
        namespaces = self.v1.list_namespace().items
        target_namespaces = [
            ns.metadata.name
            for ns in namespaces
            if ns.metadata.name != "default"
        ]

        for namespace in target_namespaces:
            pods = self.v1.list_namespaced_pod(namespace=namespace).items

            counts = {}
            for pod in pods:
                labels = pod.metadata.labels or {}

                service_name = labels.get("serving.knative.dev/service")
                revision_name = labels.get("serving.knative.dev/revision")

                # Knative pod만 대상으로
                if not service_name or not revision_name:
                    continue

                # Running pod만 카운트
                if pod.status.phase != "Running":
                    continue

                key = (service_name, revision_name)
                counts[key] = counts.get(key, 0) + 1

            for (service_name, revision_name), count in counts.items():
                pod_data.append({
                    "service": service_name,
                    "revision": revision_name,
                    "pod_count": count
                })

        return pod_data