# 유지보수 현황과 실행 안내

## 로컬 검증

저장소 루트에서 실행한다. 컨테이너의 기준 버전은 Python 3.11이다.

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements/dev.txt
python -m unittest discover -s tests -v
python -m compileall -q weather_risk_assessment dags tests
```

전체 런타임 의존성은 `requirements/runtime.txt`에서 관리하며 Docker 빌드가 사용한다.
개발 의존성은 기본 계산·데이터 변환 테스트에 필요한 최소 구성이다.
로컬 모듈 실행 시 환경변수를 미리 설정한다. `.env`는 Compose가 컨테이너에 주입하며,
개별 Python 모듈이 자동으로 읽는 것으로 가정하지 않는다.

단위·계약 테스트와 Spark Job import 테스트는 실제 API 키나 AWS 자격증명 없이 실행한다.
KMA 수집과 S3 읽기·쓰기를 포함한 통합 테스트에서만 `.env`의 `KMA_API_KEY`,
`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION`,
`S3_RISK_STREAM_BUCKET`을 사용한다. 알림 종단간 테스트에는 `SLACK_WEBHOOK_URL`이 추가로
필요하다. 실제 값은 저장소에 커밋하지 않는다.

## Docker 실행

```bash
cp .env.example .env
# .env에 필요한 키와 버킷을 설정하고 AIRFLOW_UID에 호스트 UID(id -u)를 입력
unzip data/border/N3A_G0100000.zip -d data/border
# data/border/N3A_G0100000.shp 및 동반 파일이 있는지 확인

docker compose --env-file .env -f docker/docker-compose.yaml config --quiet
docker compose --env-file .env -f docker/docker-compose.yaml up -d --build
```

Airflow UI는 `http://localhost:8080`이다. 현재 DAG는 `schedule=None`, `retries=0`이므로
수동 실행한다. 시간별 자동화 및 태스크 재시도는 향후 운영 정책을 결정한 후 설정한다.
KMA/S3를 사용하는 DAG 전체 실행은 외부 통신과 쓰기를 수반한다.

## 이번 점검에서 정리한 사항

- 소스와 데이터를 분리하고 수집기·준비 스크립트·DAG·Silver의 경로를 통합했다.
- 잘못된 Silver 기본 경로(`/opt/***/...`)와 패키지 밖 `utils` import를 수정했다.
- import 시 데이터 디렉터리를 생성하는 동작을 줄이고 저장 시 생성하도록 했다.
- GeoJSON 태스크는 지원하지 않는 `run_dir` 인자를 전달했고, 그 입력 파일도 upstream에서
  생성되지 않았다. 독립 도구로 남겨 Slack 경로에서 분리했다.
- Slack 태스크의 Kafka라는 주석은 정정했다. 기존 task_id는 이력 호환을 위해 유지했다.
- README의 시간별 스케줄·Airflow 재시도 설명을 실제 설정에 맞게 정정했다.
- DAG의 `data_interval_start`를 공통 실행 식별자로 수집기부터 Silver까지 전달하고,
  로컬 수집 결과를 실행별 디렉터리로 분리했다.
- 대표 KMA 응답 fixture와 Bronze 계약을 추가해 파일·스키마·키·시각 오류를 S3 업로드
  전에 차단한다.
- Spark Job의 환경변수 조회와 SparkSession 생성을 실행 진입점으로 옮겼다. 명시적인
  `file://` 입출력 경로를 넘기면 S3 환경변수 없이 로컬 Spark 실행도 가능하다.
- Slack 결과 경로도 실행 시점에 해석하고, 무거운 지리 라이브러리는 태스크 실행 시
  import한다. Airflow `DagBag`으로 비밀값 없이 DAG와 태스크 구성을 검사한다.

## 후속 개선 우선순위

1. 인증정보를 제거한 실제 KMA 응답 사례를 추가하고 오류·결측 응답 회귀 테스트를 확장한다.
2. Silver/Gold의 스키마, 위험도 범위, 지역·시각별 유일성 계약을 추가한다.
3. 동일 실행 식별자의 기준 시각 계산은 재현 가능하지만, KMA API의 과거 데이터 보존
   범위 밖에서는 원본 재수집이 불가능하므로 Bronze 보존·수명주기 정책을 정한다.
4. 런타임 의존성 대부분이 미고정이다. Airflow/Python/Spark 조합을 실제 빌드로 확인한 뒤
   constraints/lock과 CI를 도입한다. 이번 작업에서는 버전을 일괄 업그레이드하지 않았다.
5. GeoJSON/Tableau가 요구하는 로컬 파일과 S3 export의 스키마·전달 방식을 정한다.
6. Slack 실패 처리 기준을 검토한다.

현재 점검은 코드·설정과 로컬 테스트를 기준으로 한다. 실제 API 수집, S3 데이터 검증,
Spark 실행, Slack 전송을 완료했다는 의미는 아니다.

## 이번 변경의 검증 결과

- Python 3.12 임시 가상환경에서 unittest 46개 통과: 경로와 실행 시각, 대표 KMA 응답
  파싱, Bronze/Silver/Gold 계약, Spark Job import, Airflow DAG import, Slack 경로,
  CSV/Parquet 생성, 위험도·결측치·강수 단위·좌표 변환.
- Python 구문 컴파일 및 `git diff --check` 통과.
- Compose YAML 파싱과 5개 Airflow 서비스의 데이터 마운트·빌드 경로 확인.
- WSL Docker 연동이 비활성화되어 Compose CLI 검증·이미지 빌드·컨테이너 실행은 미수행.
  컨테이너 기준 Python 3.11에서의 통합 검증도 후속 확인이 필요하다.
