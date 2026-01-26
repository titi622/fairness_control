import time
import logging
from collector.prometheus import PrometheusCollector

# 로깅 설정: 터미널에 실시간 상황 출력
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def main():
    # 1. 설정: K8s 내부 서비스라면 'http://prometheus:9090' 이겠지만, 
    # 지금은 포트포워딩 중이므로 localhost를 사용합니다.
    PROMETHEUS_URL = "http://localhost:9090"
    
    collector = PrometheusCollector(PROMETHEUS_URL)
    
    logging.info("Knative 프로파일러를 시작합니다...")

    try:
        while True:
            # 2. Knative 서비스 목록 새로고침
            services = collector.get_service_info()
            
            if not services:
                logging.warning("조회된 Knative 서비스가 없습니다. Prometheus 연결을 확인하세요.")
            else:
                print(f"{'SERVICE':<20} | {'REVISION':<30} | {'POD COUNT':<10}")
                print("-" * 65)
                for item in services:
                    print(f"{item['service']:<20} | {item['revision']:<30} | {item['pod_count']:<10}")
            
            # 4. 10초 대기 후 재실행
            time.sleep(10)
            
    except KeyboardInterrupt:
        logging.info("사용자에 의해 프로파일러가 종료되었습니다.")

if __name__ == "__main__":
    main()