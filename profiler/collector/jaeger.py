import requests
import time
from typing import Any, Dict, List, Optional


class JaegerCollector:
    def __init__(self, base_url: str, timeout_sec: int = 10):
        self.base_url = base_url.rstrip("/")
        self.timeout_sec = timeout_sec

    # ----------------------------
    # internal http helper
    # ----------------------------
    def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = f"{self.base_url}{path}"
        resp = requests.get(url, params=params, timeout=self.timeout_sec)
        resp.raise_for_status()
        # print(f"[DEBUG] Jaeger GET: {resp.url}")
        return resp.json()

    # ----------------------------
    # Jaeger에 등록된 service 목록
    # ----------------------------
    def list_services(self) -> List[str]:
        data = self._get("/api/services")
        return data.get("data", [])

    # ----------------------------
    # trace 조회
    # ----------------------------
    def get_traces(
        self,
        service: str,
        lookback_sec: int = 60,
        limit: int = 500,
    ) -> List[Dict[str, Any]]:
        # 현재 UTC 시각 (epoch microseconds)
        end_us = int(time.time() * 1_000_000)
        start_us = end_us - (lookback_sec * 1_000_000)

        params = {
        "service": service,      # ← 하드코딩 버그 수정
        "start": start_us,       # epoch µs
        "end": end_us,           # epoch µs
        "limit": limit,
    }
        data = self._get("/api/traces", params=params)
        return data.get("data", [])

    # ----------------------------
    # 핵심: trace → 요청 단위 정보 추출
    # ----------------------------
    @staticmethod
    def extract_request_info(traces: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        반환 형태:
        {
          trace_id,
          start_us,
          duration_ms,
          service,
          revision,
          configuration
        }
        """
        out: List[Dict[str, Any]] = []

        for tr in traces:
            trace_id = tr.get("traceID")
            spans = tr.get("spans", [])

            handle_span = None
            for sp in spans:
                if sp.get("operationName") == "handle":
                    # print(sp.get("operationName"))
                    handle_span = sp
                    break

            if not handle_span:
                # handle span이 없으면 스킵(원하면 logging 추가)
                continue

            dur_us = handle_span.get("duration")  # Jaeger는 보통 microseconds
            if dur_us is None:
                continue
            start_us = handle_span.get("startTime")  # Jaeger는 보통 microseconds
            if dur_us is None:
                continue

            # tags에서 kn.revision.name 찾기
            revision = None
            for tag in handle_span.get("tags", []):
                if tag.get("key") == "kn.revision.name":
                    revision = tag.get("value")
                if tag.get("key") == "kn.service.name":
                    service = tag.get("value")


            # print(trace_id, start_us, dur_us, service, revision)

            out.append({
                "trace_id": trace_id,
                "start_us": start_us,
                "duration_ms": dur_us / 1000.0,
                "service": service,
                "revision": revision,
            })


        # 시간 역순 정렬 (최신 요청 먼저)
        out.sort(key=lambda x: x["start_us"], reverse=True)
        return out
