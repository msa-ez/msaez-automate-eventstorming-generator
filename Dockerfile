FROM python:3.12-slim

WORKDIR /app

# 시스템 의존성 설치
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Python 의존성 파일 복사
COPY pyproject.toml ./
COPY uv.lock ./

# 애플리케이션 코드 복사 (uv sync 이전에 필요)
COPY src/ ./src/

# uv 설치 및 의존성 설치 (빌드 시점에만 네트워크 사용)
# --frozen: uv.lock 그대로 설치, --no-dev: 개발 의존성 제외
RUN pip install uv
RUN uv sync --frozen --no-dev

# Python 경로 설정
ENV PYTHONPATH=/app/src
# 서브프로세스 stdout 버퍼링으로 인한 자식 로그 손실 방지
# (multiprocessing spawn 자식까지 환경변수가 그대로 전파됨)
ENV PYTHONUNBUFFERED=1

# tiktoken 인코딩을 빌드 시점에 미리 다운로드해 이미지에 캐싱.
# 런타임(폐쇄망) 에서 langchain-openai 가 tiktoken.encoding_for_model 을 부를 때
# openaipublic.blob.core.windows.net 으로 외부 다운로드를 시도하다 ConnectTimeout
# 으로 worker 가 무한 재시도되는 증상을 방지한다.
# (gpt-4.1/4o/4o-mini = o200k_base, gpt-4/3.5 = cl100k_base)
ENV TIKTOKEN_CACHE_DIR=/app/.tiktoken_cache
RUN mkdir -p ${TIKTOKEN_CACHE_DIR} \
 && /app/.venv/bin/python -c "import tiktoken; tiktoken.get_encoding('o200k_base'); tiktoken.get_encoding('cl100k_base'); print('tiktoken encodings cached at', __import__('os').environ['TIKTOKEN_CACHE_DIR'])"

# 포트 노출 (A2A 서버)
EXPOSE 5000

# 시간 맞추기
ENV TZ=Asia/Seoul
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# 런타임에는 uv를 거치지 않고 .venv의 python을 직접 호출한다.
# (uv run은 기본적으로 pyproject.toml을 보고 프로젝트 wheel을 재빌드하려 하므로,
#  pypi가 차단된 폐쇄망에서 "failed to build ... @ file:///app" 오류가 발생한다.)
CMD ["/app/.venv/bin/python", "-m", "eventstorming_generator.main"]
