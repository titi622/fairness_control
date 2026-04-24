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

        # 시스템관련을 제외한 모든 namespace 조회
        exclude = {"default", "kube-system", "istio-system", "knative-serving", "observability", "kube-public", "kube-node-lease"}
        namespaces = self.v1.list_namespace().items
        target_namespaces = [
            ns.metadata.name
            for ns in namespaces
            if ns.metadata.name not in exclude
        ]

        for namespace in target_namespaces:
            pods = self.v1.list_namespaced_pod(namespace=namespace).items
            counts = {}
            service_name = namespace #labels.get("serving.knative.dev/service")
            revision_name = namespace #labels.get("serving.knative.dev/revision")
            key = (service_name, revision_name)
            if key not in counts:
                counts[key] = 0

            for pod in pods:
                # labels = pod.metadata.labels or {}
                service_name = namespace #labels.get("serving.knative.dev/service")
                revision_name = namespace #labels.get("serving.knative.dev/revision")

                # Knative pod만 대상으로
                # if not service_name or not revision_name:
                #     continue

                # Running pod만 카운트
                if pod.status.phase == "Running":
                    counts[key] += 1

            for (service_name, revision_name), count in counts.items():
                pod_data.append({
                    "service": service_name,
                    "revision": revision_name,
                    "pod_count": count
                })

        return pod_data