# 유지보수 현황과 실행 안내

## 로컬 검증

저장소 루트에서 실행한다. 컨테이너의 기준 버전은 Python 3.11이다.

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements/dev.txt
python -m unittest discover -s tests -q
python -m compileall -q weather_risk_assessment dags tests
```

Bronze → Silver → Gold 로컬 Spark 통합 테스트는 별도 의존성을 설치해 실행한다.

```bash
python -m pip install -r requirements/integration.txt
python -m unittest tests.test_medallion_pipeline -q
```

MinIO S3 호환 통합 테스트는 로컬 전용 자격증명으로 실행한다.

```bash
docker compose -f docker/docker-compose.minio.yaml up -d
until curl --fail --silent http://127.0.0.1:9000/minio/health/ready; do sleep 1; done
AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin123 \
AWS_REGION=ap-northeast-2 S3_ENDPOINT_URL=http://127.0.0.1:9000 \
python -m unittest tests.test_minio_integration -q
docker compose -f docker/docker-compose.minio.yaml down -v
```

실제 AWS 검증은 로그인한 CLI 프로필과 임시 버킷을 명시해야만 실행된다.

```bash
python -m pip install -r requirements/aws.txt
AWS_PROFILE=weather-risk AWS_REGION=ap-northeast-2 \
AWS_INTEGRATION_BUCKET=<임시 버킷 이름> \
python -m unittest tests.test_aws_s3_integration -q
```

직접 사용하는 런타임 의존성은 `requirements/runtime.in`, Python 3.11/Linux에서 해석한
전체 잠금 버전은 `requirements/runtime.txt`에서 관리하며 Docker 빌드가 잠금 파일을
사용한다. 개발 의존성은 기본 계산·데이터 변환 테스트에 필요한 최소 구성이다.
로컬 모듈 실행 시 환경변수를 미리 설정한다. `.env`는 Compose가 컨테이너에 주입하며,
개별 Python 모듈이 자동으로 읽는 것으로 가정하지 않는다.

런타임 버전을 갱신할 때는 Airflow 3.3.2 공식 constraints를 기준으로 한다. Spark 3.5와
호환되도록 직접 고정한 PySpark, pandas, NumPy, PyArrow는 공식 목록에서 제외하고
잠금 파일을 다시 만든 뒤 Spark 테스트와 Docker 빌드를 확인한다.

```bash
curl -fsSLo /tmp/airflow-constraints-3.11.txt \
  https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.11.txt
sed -E '/^(pyspark|pandas|numpy|pyarrow)==/d' \
  /tmp/airflow-constraints-3.11.txt > /tmp/weather-risk-constraints.txt
uv pip compile requirements/runtime.in \
  --constraint /tmp/weather-risk-constraints.txt \
  --python-version 3.11 --python-platform x86_64-manylinux2014 \
  --no-annotate --output-file requirements/runtime.txt \
  --custom-compile-command "see docs/maintenance.md: 런타임 잠금 파일 갱신"
```

단위·계약 테스트와 Spark Job import 테스트는 실제 API 키나 AWS 자격증명 없이 실행한다.
KMA 수집과 S3 읽기·쓰기를 포함한 통합 테스트에서만 `.env`의 `KMA_API_KEY`,
`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`,
`S3_RISK_STREAM_BUCKET`을 사용한다. 알림 종단간 테스트에는 `SLACK_WEBHOOK_URL`이 추가로
필요하다. 실제 값은 저장소에 커밋하지 않는다.

## Docker 실행

```bash
cp .env.example .env
# .env에 키·버킷·AIRFLOW_UID를 채우고 AIRFLOW_JWT_SECRET은 openssl rand -hex 32로 생성
# 기존 .env는 덮어쓰지 않는다. JWT 키는 비워두면 Compose 실행이 중단된다.
unzip data/border/N3A_G0100000.zip -d data/border
# data/border/N3A_G0100000.shp 및 동반 파일이 있는지 확인

cd docker
docker compose config --quiet
docker compose up -d --build --remove-orphans
```

`docker/.env`는 저장소 루트 `.env`를 가리킨다. 현재 DAG에는 deferrable 태스크가 없어
triggerer를 실행하지 않으며, 필요해지면 서비스를 다시 추가한다. 기존 triggerer 컨테이너는
`--remove-orphans`로 정리하되 PostgreSQL 볼륨은 유지한다.

Airflow UI는 `http://localhost:8080`이다. DAG는 기상청 자료 게시 시간을 고려해 매시
10분에 실행하며(`10 * * * *`), 실패 작업은 5분 간격으로 최대 2회 재시도한다. 과거 실행은
자동으로 소급하지 않고(`catchup=False`), 한 번에 하나의 DAG 실행만 허용한다.
KMA/S3를 사용하는 DAG 전체 실행은 외부 통신과 쓰기를 수반한다.

기존 Airflow 2.7.3 메타데이터 DB를 승계할 때는 먼저 DAG를 일시중지하고 실행 중인
태스크 종료를 확인한다. PostgreSQL 전체를 별도 안전한 경로에 `pg_dump -Fc`로 백업하고
백업 파일이 읽히는지 확인한 뒤 기존 Airflow 서비스를 중지한다. 그 후에만 새 Compose의
`airflow-init`가 `airflow db migrate`를 수행하도록 기동한다. 마이그레이션 후 이전
이미지로만 되돌릴 수 없으며 DB 백업 복원이 필요하다. `docker compose down -v`는
메타데이터를 삭제하므로 실행하지 않는다.

2026-09-23 Airflow 3.3.2 전환 후 17:10 KST 정기 실행에서 12개 태스크가 모두 첫 시도에
성공했다. 최신 Gold의 `dt=2026092317`과 최신·일별 Parquet Export는 각각 228행으로
확인했다. 전환 전 16:10 KST 실행은 Export가 KST 예보 시각을 UTC로 비교해 실패했으며,
해당 이력은 보존하고 Spark 세션 시간대에서 검사하도록 수정했다.

## 이번 점검에서 정리한 사항

- 소스와 데이터를 분리하고 수집기·준비 스크립트·DAG·Silver의 경로를 통합했다.
- 잘못된 Silver 기본 경로(`/opt/***/...`)와 패키지 밖 `utils` import를 수정했다.
- import 시 데이터 디렉터리를 생성하는 동작을 줄이고 저장 시 생성하도록 했다.
- GeoJSON 태스크는 지원하지 않는 `run_dir` 인자를 전달했고 입력도 upstream에서 생성되지
  않아 DAG에서 분리했으며, 시각화 방향을 Tableau Public으로 정하면서 관련 도구를 제거했다.
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
- 단기·초단기예보의 KMA 응답 헤더와 `items` 구조를 공통 검증하고, 인증 오류·자료 없음·
  정상 빈 응답 fixture로 회귀 테스트한다.
- Silver/Gold 저장 전에 스키마, 위험도 범위, 지역·시각별 유일성을 검사한다.
- 샘플 Bronze 5종과 행정구역 매핑으로 실제 Spark Silver·Gold 변환을 연결 검증한다.
- GitHub Actions에서 Java 17·Python 3.11·PySpark 3.5.1 조합으로 통합 테스트를 실행한다.
- MinIO의 S3 API에 Bronze 5종을 업로드하고 파티션 키와 Parquet 내용을 검증한다.
- 실제 AWS S3에서도 고유 테스트 prefix로 같은 검증을 수행하고 객체를 즉시 삭제한다.
- Terraform으로 퍼블릭 차단·TLS·AES-256 암호화·수명주기를 적용한 S3, 파이프라인 경로
  최소 권한 IAM 정책, 월 비용 Budget을 정의한다. 실행 역할과 Access Key는 만들지 않는다.
- GitHub Actions에서 Terraform 포맷과 provider 스키마 검증을 수행한다.
- Airflow 2.7.3과 프로젝트 직접 의존성을 Python 3.11/Linux 기준으로 해석한 런타임 잠금
  파일을 추가하고 Docker가 설치 후 `pip check`를 수행한다. GitHub Actions에서도 같은
  Dockerfile의 이미지 빌드를 검증한다.
- 버전 관리·퍼블릭 차단·TLS·AES-256 암호화를 적용한 별도 S3 backend를 만들고 메인 및
  bootstrap state를 서로 다른 key로 이전한다. S3 네이티브 잠금을 사용한다.
- Slack 웹훅 미설정과 HTTP 오류를 알림 태스크 실패로 처리한다. 알림은 Gold/Export 뒤에
  실행되므로 전송 실패가 이미 생성된 데이터에는 영향을 주지 않는다.
- 사용하지 않는 Tableau Cloud/Hyper와 kepler.gl 코드를 제거하고, 운영 DAG와 분리된
  Tableau Public용 CSV 변환기와 공개 샘플을 둔다.
- Airflow DAG를 매시 10분 자동 실행으로 전환하고 실패 작업에 5분 간격 2회 재시도를
  적용했다. 실행별 경로는 스케줄 구간의 KST 종료 시각을 기준으로 유지한다.
- 매시간 동일한 행정경계 변환을 반복하지 않도록 원본 SHP·중심점 CSV·생성 코드의 수정
  시각을 비교하고, 변경이 없으면 기존 중심점 CSV와 격자 Parquet을 재사용한다.
- 한 실행이 다음 시간대를 막지 않도록 DAG 실행을 55분으로 제한하고, 재시도 후에도 실패한
  태스크는 실행 ID와 Airflow 로그 주소를 Slack으로 알린다. 콜백 전송 실패는 원래 태스크
  실패를 가리지 않으며 네트워크 예외에 웹훅 주소를 포함하지 않는다.
- KMA `baseDate/baseTime`이 실행 시각보다 4시간을 초과해 오래된 경우 Bronze 업로드 전에
  실패시킨다. 수집기의 네 차례 발표 시각 후보 탐색 범위는 허용한다.

## 후속 개선 우선순위

1. 실제 배포 대상이 정해질 때만 CD를 추가한다.

2026-09-23 12시 KST 파티션의 DAG 12개 task는 성공했지만 V4 UV 응답은 0행이었다.
V5 전환 후 기상청의 2026년 행정구역 개편 코드(전남광주통합특별시 `12`, 인천 제물포구·서구 `28`)를
국토정보플랫폼 법정동코드와 매핑하는 `AREA_CODE_ALIAS` 테이블을 적용하고, 미등록 지역은
광역시도 코드 폴백 및 `resultCode: "99"` 결측 처리를 지원하도록 보완했다.
실제 API로 전국 247개 행정구역에 대해 `fetch_and_save_uv_wide`를 검증한 결과 247개 지역
전부 결측 없이 정상 수집(rows=247, warnings=0)됨을 확인했다.

## 이번 변경의 검증 결과

- Python 3.12 임시 가상환경에서 unittest 57개 실행(53개 통과, 4개 건너뜀): 경로와 실행
  시각, 정상·오류·결측 KMA 응답 파싱, Bronze/Silver/Gold 계약, Spark Job import,
  Slack 경로·전송 실패, Tableau Public CSV, CSV/Parquet 생성, 위험도·결측치·강수 단위·
  좌표 변환.
- Python 구문 컴파일 및 `git diff --check` 통과.
- 임시 MinIO 서버에서 버킷 생성, Bronze 5개 업로드, 객체 키와 Parquet 재읽기 통과.
- 서울 리전의 임시 AWS S3 버킷에서도 같은 검증을 통과하고 객체와 버킷을 삭제함.
- Terraform 1.16.3과 AWS provider 6.66.0으로 `fmt -check`와 `validate`를 통과하고,
  비루트 임시 자격증명으로 서울 리전에 S3·IAM 정책·월 비용 Budget을 최초 적용했다.
  적용 후 refresh plan은 변경 0개이며 state와 사용자 변수는 Git에서 제외한다.
- 메인 10개 항목과 bootstrap 7개 항목의 state를 버전 관리되는 S3 backend로 이전했다.
  두 구성 모두 원격 state 기반 refresh plan에서 변경 0개를 확인했다.
- 잠금된 런타임 158개 패키지를 Python 3.11 환경에 설치하고 의존성 검사, 전체
  unittest, Spark Bronze → Silver → Gold 통합 테스트와 구문 컴파일 통과.
- Airflow 2.7.3/Python 3.11/OpenJDK 17 이미지를 빌드하고 `pip check`를 통과했다.
  PostgreSQL, Redis와 Airflow 웹서버·스케줄러·워커·트리거를 모두 healthy 상태로 기동하고
  DAG import 오류 없음, 웹 health 응답, 로컬 Spark Bronze → Silver → Gold 변환,
  컨테이너 내 unittest 57개(55개 통과, 2개 건너뜀)를 확인했다.
