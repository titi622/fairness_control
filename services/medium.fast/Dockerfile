FROM python:3.11-slim

WORKDIR /app

# (선택) 타임존/인증서 등 기본 패키지 최소만
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
  && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY medium_fast.py /app/medium_fast.py

ENV PORT=8090
# workers=1로 두면 "컨테이너당 1 프로세스"라 cold/warm 관찰이 직관적
CMD ["gunicorn", "-b", "0.0.0.0:8090", "medium_fast:app", "--workers=1", "--threads=8", "--timeout=0"]
