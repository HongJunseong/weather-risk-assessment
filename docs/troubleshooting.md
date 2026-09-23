# 설계 선택과 트러블슈팅

[README로 돌아가기](../README.md)

처리 구조를 선택한 이유와 구현 중 해결한 문제를 정리했습니다. 각 사례는 코드·테스트로 확인할 수 있는 동작을 중심으로 기술합니다.

- [Spark·Delta Lake 선택](#spark-delta)
- [UV API의 빈 응답과 오류 구분](#uv-response)
- [KST·UTC 차이로 Export 검사 실패](#export-timezone)
- [Export 이력과 실행 결과 확인](#export-history)
- [입력 검증·반복 연산·운영 환경 개선](#operations)

<a id="spark-delta"></a>
## Spark·Delta Lake 선택

### 요구사항과 대안

수집한 기상 데이터를 격자·예보 시각으로 결합하고, 같은 위험도 계산 결과에서 지역별 최신 예보와 날짜별 요약을 만들어야 합니다. 수집은 Python에서 처리하고, Silver·Gold 변환을 Spark 작업으로 분리했습니다.

| 선택지 | 프로젝트에서의 판단 |
|---|---|
| pandas + Parquet | 현재 데이터 규모를 처리할 수 있고 실행 환경이 단순합니다. 단일 프로세스로 처리하는 구성이 우선이라면 적합합니다. |
| Spark + Delta Lake | 조인·윈도 함수·집계를 단계별 작업으로 구성하고, S3의 정제·집계 데이터를 Delta 테이블로 관리하는 구조를 구현하기 위해 선택했습니다. JVM·Spark 세션과 S3 연동 설정이 추가되는 비용이 있습니다. |

### 구현에 연결된 선택

1. **정제(Silver)와 윈도 집계(Gold)의 계층적 파이프라인 분리**:
   - **Silver 변환**: 5종 원천 기상 데이터를 결합하고 결측치를 보정한 뒤, 표준 스키마의 정제 데이터셋으로 적재합니다.
   - **Gold 집계**: `Window.partitionBy("admin_names").orderBy(col("forecast_time").desc())` 윈도 함수를 통해 지역별 최신 예보 시점을 추출(`risk_latest`)하고, 날짜·지역 그룹별 평균·최대 위험도 및 최고 위험 시각을 시계열로 집계(`risk_daily`)하는 복합 집계 작업을 Spark로 처리합니다.
2. **공통 계산 함수의 분산 처리 (`mapInPandas`)**:
   - 5대 지표별 가중치와 피크 지수를 산출하는 복잡한 위험도 계산 로직을 Spark 파티션 단위로 병렬 적용합니다.
   - 단일 컬럼 연산으로 풀기 어려운 도메인 로직을 Python/pandas로 사전에 정밀 검증하고, 이를 Spark 작업에서 그대로 재사용하여 계산식 중복과 불일치 문제를 방지했습니다.
3. **저장과 활용의 계층 분리 (Medallion Lakehouse)**:
   - Silver·Gold 정제 및 집계 테이블은 Delta Lake로 관리하여 ACID 트랜잭션과 메타데이터 버저닝(Time Travel)을 보장합니다.
   - 반면 Slack 알림이나 Tableau 대시보드 변환을 위한 최종 서빙 산출물은 표준 Parquet으로 내보내(Export), 데이터 소비 측이 무거운 Delta 라이브러리에 의존하지 않도록 격리했습니다.
4. **확장성(Scalability)과 대량 백필(Backfill)**:
   - 현재 247개 시군구 단위는 단일 머신의 pandas로도 처리할 수 있지만, 향후 5km 고해상도 전국 격자(수만 개 지점) 확장이나 다개월치 과거 기상 데이터의 대규모 백필 시 단일 프로세스의 메모리 한계(OOM)를 방어하고 클러스터 수평 확장이 가능한 데이터 파이프라인 구조를 선제적으로 구축했습니다.

Spark는 변환·집계를 담당하고, Delta Lake는 테이블 변경을 트랜잭션 로그로 관리합니다. 두 기술의 역할을 명확히 구분하며, Delta 테이블 쓰기 단위와 서빙용 Parquet Export의 갱신 단위도 분리했습니다.

### 검증과 적용 범위

[로컬 Spark 통합 테스트](../tests/test_medallion_pipeline.py)는 샘플 Bronze 5종에서 Silver·Gold를 만들고 집계 결과와 검증 규칙을 확인합니다. 현재는 다중 노드 처리량, pandas 대비 속도 벤치마크, 대규모 클러스터에서의 비용 효율을 별도로 측정한 상태는 아닙니다. 데이터 증가 시 파티션 수·조인 전략·워커 자원을 유연하게 조정할 수 있는 처리 구조를 구현하는 데 초점을 두었으며, 실 클러스터 성능 튜닝은 별도 검증 대상입니다.

구현: [Silver 변환](../weather_risk_assessment/jobs/build_silver_from_bronze.py) · [Gold latest](../weather_risk_assessment/jobs/build_gold_risk_latest.py) · [Gold daily](../weather_risk_assessment/jobs/build_gold_risk_daily.py)

<a id="uv-response"></a>
## UV API의 빈 응답과 오류 구분

### 현상과 원인

UV 수집에서 지역별 빈 응답과 `KMA UV API 99` 오류가 발생했습니다. 요청 엔드포인트와 지역 조회 코드가 맞는지 확인해야 했고, 응답 코드만으로 자료 없음과 시스템 오류를 구분하기 어려웠습니다. UV는 격자 좌표 대신 `areaNo`를 조회 기준으로 사용하므로 다른 예보 API의 성공만으로 UV 조회도 정상이라고 판단할 수 없었습니다.

### 수정

- `LivingWthrIdxServiceV5/getUVIdxV5`를 사용하고 `AREA_CODE_ALIAS`로 지역 조회 코드를 매핑했습니다.
- 매핑한 코드로 조회되지 않을 때 광역 코드로 재조회하도록 했습니다.
- 코드 `99`라도 메시지가 ‘검색결과가 없습니다’ 또는 `NO_DATA`인 경우에만 빈 결과로 처리합니다. 그 외 API 오류는 예외로 유지합니다.

### 검증과 범위

[수집기 회귀 테스트](../tests/test_collector_fixtures.py)에서 V5 요청 경로, 정상 응답, 자료 없음, 시스템 오류를 구분합니다. 광역 코드 폴백은 자료 확보를 보완하는 방식이며, 시군구별 독립적인 관측값을 확보한 것과 같지 않습니다. 지역별 행이 생성됐다는 사실만으로 값의 정확성이나 지속적인 수집 품질을 보장하지 않습니다.

구현: [UV 수집기](../weather_risk_assessment/collectors/uv_forecast.py)

<a id="export-timezone"></a>
## KST·UTC 차이로 Export 검사 실패

### 현상과 원인

2026-09-23 전환 점검에서 Gold 데이터가 생성됐지만 Export의 예보 신선도 검사에 실패했습니다. KST 실행 시각과 UTC로 해석된 예보 시각을 비교해 정상 입력을 오래된 데이터로 판정한 사례였습니다.

### 수정

Spark 세션 시간대를 `Asia/Seoul`로 통일하고, 예보 시각을 해당 세션 기준으로 포맷해 `run_dt`와 비교하도록 수정했습니다. 입력 Gold의 실행 식별자가 요청한 실행과 일치하는지도 검사합니다.

### 검증

[로컬 Spark 통합 테스트](../tests/test_medallion_pipeline.py)는 KST 세션에서 정상 Gold 입력의 통과와 더 늦은 실행 시각을 지정했을 때의 거부를 확인합니다. 시간대가 포함된 시각과 문자열 실행 식별자를 비교하는 경계에서 기준을 명시했습니다.

구현: [DAG의 Spark 설정](../dags/weather_risk_assessment_dag.py) · [Export 입력 검사](../weather_risk_assessment/jobs/export_gold_parquet.py)

<a id="export-history"></a>
## Export 이력과 실행 결과 확인

### 문제

최신 Parquet만 덮어쓰면 이전 실행 결과를 비교하기 어렵습니다. 또한 DAG 성공 여부만으로는 어떤 데이터가 얼마나 수집됐고 최종 위험도가 어떻게 나왔는지 확인하기 어렵습니다.

### 수정

- `gold_export/history/dt=YYYYMMDDHH/`에 latest·daily 결과를 저장합니다.
- 이력 Parquet을 재읽어 비어 있지 않은지와 입력 대비 행 수를 확인한 뒤 기존 최신 경로를 갱신합니다.
- Export 이후 수집 건수·지역 커버리지와 Gold 위험도 요약을 JSON·Markdown으로 기록합니다.

### 검증과 범위

[Export 테스트](../tests/test_export_promotion.py)는 행 수 불일치·빈 결과의 거부와 로컬 파일의 이력 저장·최신 경로 갱신을 확인합니다. [리포트 테스트](../tests/test_quality_report.py)는 샘플 집계와 JSON·Markdown 생성을 확인합니다.

현재 최신 경로 두 곳은 순차적으로 덮어씁니다. 두 결과를 한 번에 전환하는 원자적 게시 방식은 아니며, 동일 `dt`를 재실행하면 해당 이력도 덮어씁니다. 행 수 검증은 값 전체의 동일성 검사가 아닙니다.

리포트의 지역 커버리지는 지역 키의 존재 여부를 기준으로 합니다. 개별 기상 값의 결측률이나 예측 정확도와 구분하며, 단일 실행 수치로 장기간 품질을 주장하지 않습니다.

구현: [Export 작업](../weather_risk_assessment/jobs/export_gold_parquet.py) · [실행 요약 생성기](../weather_risk_assessment/scripts/generate_quality_report.py)

<a id="operations"></a>
## 입력 검증·반복 연산·운영 환경 개선

### 저장 전 입력 검사

Bronze 업로드 전에 필수 컬럼·키 중복·시각·좌표를 검사하고, 발표 시각이 있는 데이터는 실행 시각보다 4시간을 초과해 오래됐는지 확인합니다. Silver·Gold도 저장 전에 스키마·위험도 범위·유일성을 검사합니다.

[Bronze 테스트](../tests/test_bronze_contract.py)는 잘못된 입력이 S3 클라이언트 생성 전에 거부되는지 확인합니다. [Silver·Gold 검사 테스트](../tests/test_medallion_contract.py)는 정의한 검증 규칙을 점검합니다. 이는 검사 대상 오류를 걸러내는 동작이며 모든 데이터 오류를 탐지한다는 의미는 아닙니다.

### 고정 행정경계 산출물 재사용

행정경계 원본과 생성 코드의 수정 시각을 산출물과 비교하고, 변경이 없으면 중심점 CSV·격자 Parquet을 재사용합니다. 매시간 같은 변환을 반복하지 않도록 했으며, 실행 시간·메모리 절감률은 별도 측정치로 제시하지 않습니다.

구현: [경로·산출물 갱신 검사](../weather_risk_assessment/paths.py) · [중심점 생성](../weather_risk_assessment/scripts/build_admin_centroids_from_shp.py)

### 아키텍처 단순화: Kafka 배제 및 Slack 직결

1시간 주기의 소규모 배치 알림(시간당 수~십수 건)에 Kafka 메시지 브로커를 상시 구동하는 것은 불필요한 인프라 복잡도와 컨테이너 메모리 낭비를 유발하므로 과감히 배제했습니다. 대신 지수 백오프(Exponential Backoff) 재시도와 실패 콜백을 내장한 Slack Incoming Webhook 직결 구조로 단순화하여 시스템 리소스를 절감했습니다.

소비용 Parquet을 읽어 임계값 이상 지역을 Slack으로 전송합니다. 웹훅 미설정·HTTP 오류는 실패로 처리하며 네트워크 예외에 웹훅 URL이 노출되지 않도록 했습니다. 재시도는 DAG에서 5분 간격으로 최대 2회 수행합니다. 최종 실패 콜백의 전송 오류는 원래 태스크 오류를 가리지 않도록 처리합니다.

검증: [Slack 경로·전송·실패 콜백 테스트](../tests/test_slack_alert.py)

### Airflow 3·Compose·CI

Airflow 3의 `airflow.sdk`와 standard provider 연산자를 사용하고, API 서버·DAG 프로세서·스케줄러·워커를 Compose로 구성했습니다. `docker/.env`는 루트 환경파일을 가리켜 `docker/`에서 실행하는 Compose도 필요한 설정을 읽습니다. Airflow와 MinIO Compose는 프로젝트 이름을 공유하므로 `--remove-orphans`로 다른 구성의 컨테이너를 제거하지 않도록 운영 안내에 명시했습니다.

[CI 설정](../.github/workflows/ci.yml)에 DAG import, 단위 테스트, 로컬 Spark 변환, MinIO 통합, Docker 빌드, Terraform 검증을 구성했습니다. CI 검사는 실행 환경과 코드의 회귀 검증이며 상시 운영 가용성의 측정 결과와는 구분합니다.
