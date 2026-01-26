import requests
import logging

# 로그 출력 설정
logger = logging.getLogger(__name__)

class PrometheusCollector:
    def __init__(self, prometheus_url):
        # URL 끝에 /가 있으면 제거하여 일관성 유지
        self.prometheus_url = prometheus_url.rstrip('/')
        self.services = []

    def get_service_info(self):
        """
        특정 서비스의 현재 실행 중인(Running) Pod 개수를 쿼리합니다.
        """
        # Collecting Metric
        query = 'kn_revision_pods_count'
        endpoint = f"{self.prometheus_url}/api/v1/query"
        
        try:
            response = requests.get(endpoint, params={'query': query}, timeout=20)
            result = response.json()
            if result['status'] != 'success':
                logger.error(f"Prometheus 쿼리 실패: {result.get('error')}")
                return []
            pod_data = []
            for item in result['data']['result']:
                metric_labels = item['metric']
                value = item['value'][1]  # [timestamp, value] 형태 중 value 추출

                # 필요한 레이블 추출 (Key 에러 방지를 위해 .get() 사용)
                service_name = metric_labels.get('kn_service_name', 'N/A')
                revision_name = metric_labels.get('kn_revision_name', 'N/A')
                
                pod_data.append({
                    'service': service_name,
                    'revision': revision_name,
                    'pod_count': int(float(value))
                })

            return pod_data

        except Exception as e:
            logger.error(f"데이터 수집 중 오류 발생: {e}")
            return []