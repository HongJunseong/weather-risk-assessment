# 재해 대응을 위한 준실시간 기상 위험도 산출 파이프라인
*Near Real-time Weather Risk Scoring Pipeline*

![python](https://img.shields.io/badge/Python-3.11-blue)
![airflow](https://img.shields.io/badge/Apache%20Airflow-3.3.2-017CEE)
![spark](https://img.shields.io/badge/Apache%20Spark-3.5.1-E25A1C)
![aws s3](https://img.shields.io/badge/AWS-S3%20Delta%20Lake-FF9900)
![slack](https://img.shields.io/badge/Slack-Alert-4A154B)
![tableau public](https://img.shields.io/badge/Tableau%20Public-Demo-E97627)

> **요약**: 기상청 API를 1시간 주기로 수집하고, Spark와 AWS S3 Delta Lake로 지역별 복합 기상 위험도를 산출하는 파이프라인입니다. 실행별 결과 보존과 Slack 자동 알림을 구현하고, Tableau Public 대시보드로 시각화하는 흐름을 구성했습니다.

---

## 프로젝트 개요

강수·폭염·바람·자외선·태풍 데이터를 공통 척도로 변환해 지역별 위험도를 비교합니다. **수집 → 정제·계산 → 저장 → 알림·시각화**를 연결하고, 각 단계에 입력 검증과 실행 결과 확인 기능을 추가한 데이터 엔지니어링 프로젝트입니다.

---

## 프로젝트 배경

기상청은 다양한 관측·예보 정보를 제공하지만, API마다 발표 시각과 조회 기준, 지표 단위가 다릅니다. 지역별 위험도를 비교하려면 행정구역을 기상청 격자에 매핑하고, 예보 시각과 데이터 형식을 맞추는 과정이 필요합니다.

본 프로젝트는 이 반복 작업을 자동화하고, **서로 다른 기상 지표를 지역 단위의 비교 가능한 정보로 가공**하는 것을 목표로 합니다.

---

## 프로젝트 내용 요약

- **데이터 파이프라인 구축**: Airflow DAG로 기상청 API 5종을 수집하고, KST 실행 시각별로 수집 파일과 S3 Bronze를 구분
- **위험도 산출 로직 구현**: 지표별 위험도를 계산한 뒤 최고값과 가중평균으로 종합 위험도(`R_total`) 산출. 공식과 가중치를 한 파일에서 관리
- **Medallion Architecture**: Bronze Parquet에 원천 데이터를 보존하고, Silver·Gold Delta Lake에서 정제·계산 및 최신·일별 집계 수행
- **Spark 기반 데이터 처리**: `mapInPandas`로 공통 pandas 위험도 함수를 호출해 로컬 계산과 Spark 변환에서 같은 로직 사용
- **결과 이력·검증**: 저장 단계별 스키마·시각·위험도 검사, 시간별 Export 이력 보존 및 저장 결과의 행 수 재검증
- **알림·실행 요약**: 위험 지역과 최종 실패 태스크를 Slack으로 통지하고, 수집 건수·위험도 요약을 JSON·Markdown으로 기록
- **시각화 데모**: Export Parquet을 좌표가 포함된 공개용 CSV로 변환해 Tableau Public 대시보드에 활용

---

## 데이터 구성

- **출처**: 기상청 Open API — 초단기실황, 초단기예보, 단기예보, 생활기상지수(UV), 태풍 예측
- **지역 기준**: 행정경계 SHP에서 중심점을 구하고 기상청 격자(`nx, ny`)에 매핑
- **저장**: AWS S3의 Bronze·Export는 Parquet, Silver·Gold는 Delta Lake
- **집계**: Gold latest는 지역 그룹별 가장 뒤의 예보 시각, Gold daily는 해당 실행의 예보를 날짜별로 묶은 평균·최대 위험도와 최대 위험 시각

**위험도 계산 공식**

```text
R_total = 0.7 × peak + 0.3 × weighted_avg
가중치: 강수 28% | 바람 22% | 태풍 20% | 폭염 18% | UV 12%
```

`peak`는 지표별 최고 위험도, `weighted_avg`는 가중평균입니다. 특정 지표의 높은 위험이 평균에 묻히지 않도록 최고값에 더 큰 비중을 두었습니다. 공식은 [risk/config.py](weather_risk_assessment/risk/config.py)에서 관리하며, 점수는 프로젝트에서 정의한 비교 지표로 기상청 공식 특보 등급과 구분합니다.

---

## 전체 시스템 구성

![Weather Risk Assessment 시스템 아키텍처](docs/assets/weather-risk-architecture.svg)

- **Airflow DAG**: 매시 10분 자동 실행(`10 * * * *`), 실패 작업은 5분 간격으로 최대 2회 재시도하고 실행 전체를 55분으로 제한
- **타임존**: `Asia/Seoul`(KST) 기준 시각 처리
- **실행 제어**: 동시 DAG 실행은 1개로 제한하고 과거 구간의 자동 소급 실행은 비활성화
- **Export 이후**: 실행 결과 요약을 생성한 뒤 Slack 알림 수행

---

## 결과 및 시각화

### Slack 알림

종합 위험도가 HIGH(`≥0.6`) / VERY_HIGH(`≥0.8`)인 지역의 점수·등급·예보 시각을 전송합니다. 최종 실패 태스크는 실행 ID와 Airflow 로그 링크를 함께 알립니다.

```
🚨 기상 위험 지역 알림 (예시)
🔴 *강원도 강릉시*  |  위험도: 0.83 (VERY_HIGH)  |  예보: 2025-08-10 14:00
🟠 *경상남도 창원시*  |  위험도: 0.67 (HIGH)  |  예보: 2025-08-10 14:00
```

위험 지역이 없을 경우:

```
✅ 기상 위험 알림 | 현재 위험 지역 없음
```

### Tableau Public 대시보드 *(Demo)*

Export된 Parquet에 행정구역 대표 좌표를 결합해 Tableau Public용 CSV를 만듭니다. Tableau 계정과 게시 과정은 운영 DAG에서 분리하며, 생성된 CSV는 무료 웹 편집기에 별도로 게시합니다.

- 전국 **종합 위험도 지도**와 위험도 상위 지역 비교
- 강수·폭염·바람·자외선·태풍의 지표별 평균 위험도 비교
- [Tableau Public에서 대시보드 열기](https://public.tableau.com/views/WeatherRiskDashboard_17900819962470/WeatherRiskDashboard?:showVizHome=no)

[![Weather Risk Dashboard](docs/assets/tableau-dashboard.png)](https://public.tableau.com/views/WeatherRiskDashboard_17900819962470/WeatherRiskDashboard?:showVizHome=no)

공개 Demo는 합성 데이터를 사용하며, 실제 예보의 자동 갱신과는 구분합니다.

### 실행 결과 요약

데이터셋별 수집 건수와 Gold 위험도 분포를 자동 집계해 [실행 결과 요약](docs/latest_execution_report.md)으로 남깁니다. 개별 실행을 확인하는 기록이며, 장기간의 수집 품질이나 예측 정확도를 나타내는 지표는 아닙니다.

---

## 🛠️ 주요 문제 해결 및 엔지니어링 개선

파이프라인 구축 및 운영 과정에서 발생한 데이터 결측, 시공간 정합성, 서빙 안정성 문제를 해결한 핵심 엔지니어링 사례입니다.

* **비표준 공공데이터 결측 방어 및 계약 검증**: 기상청 생활기상지수(UV) API가 2026년 행정구역 개편 코드를 선반영해 발생한 `Code 99` 오류를 역추적하여 별칭 매핑(`AREA_CODE_ALIAS`)과 광역 폴백으로 247개 전 지역 결측률 0%를 달성하고, 4시간 초과 노후 발표본의 S3 적재를 사전 차단
* **분산 환경의 시공간 정합성 확보**: KST와 UTC 혼용으로 발생한 예보 신선도 검증 실패를 Spark 세션 타임존 통일(`Asia/Seoul`)로 해결하고, 행정구역 대표 좌표와 기상청 격자(`nx, ny`) 간 매핑 기준을 일원화
* **서빙 안정성을 위한 원자적 승격 (Atomic Promotion)**: Export 실패 시 기존 정상 데이터가 훼손되는 문제를 방지하기 위해, 실행별 이력 경로(`history/dt=...`)에 격리 저장 후 행 수·스키마 검증이 완료된 시점에만 최신본으로 승격
* **아키텍처 단순화 및 리소스 절감**: 1시간 주기의 소규모 배치 알림에 불필요한 Kafka 브로커를 걷어내고 재시도 내장 Slack Webhook으로 경량화했으며, 수백 MB 행정경계 SHP 파싱 병목을 `mtime` 증분 캐싱으로 해결해 반복 실행 지연 제거

👉 각 사례별 상세 원인 분석, 재현 과정, 회귀 테스트 근거 및 인프라/검증 환경(Airflow 3, CI, Terraform)은 [**엔지니어링 트러블슈팅 상세 문서 (docs/troubleshooting.md)**](docs/troubleshooting.md)에서 확인하실 수 있습니다.

---

## 인프라 및 개발 환경

- **Python**: 3.11
- **Workflow**: Apache Airflow 3.3.2 (Docker Compose)
- **Processing**: pandas + PySpark 3.5.1
- **Storage**: AWS S3 Parquet + Delta Lake (Medallion Architecture)
- **Alert**: Slack Incoming Webhook
- **Visualization** *(demo)*: Tableau Public
- **Infrastructure**: Docker Compose + Terraform (AWS S3 · IAM · Budgets)
- **Validation**: GitHub Actions · unittest · MinIO

---

## 향후 확장 아이디어

- **머신러닝 기반 예측 모델 결합**: 단순 평가 → 위험도 예측으로 확장
- **PostGIS 공간 분석 적용**: 태풍 경로와 행정구역의 교차 분석으로 영향권 비교
- **REST API 레이어 추가**: FastAPI로 위험도 데이터를 외부 서비스에 제공

---

## 기본 실행 가이드

```bash
# 저장소 루트에서 실행
# 1) .env가 없을 때만 생성 (.env.example 참고)
test -f .env || cp .env.example .env
# .env에 KMA_API_KEY, AWS 키·리전, S3 버킷명, Slack Webhook URL, AIRFLOW_UID 입력
# AIRFLOW_JWT_SECRET은 openssl rand -hex 32로 생성해 입력 (기존 .env는 덮어쓰지 않음)

# 2) 행정경계 준비 및 Airflow 컨테이너 실행
unzip -n data/border/N3A_G0100000.zip -d data/border
cd docker
docker compose up -d --build

# 3) Airflow UI 접속
# http://localhost:8080  (ID: airflow / PW: airflow)
# weather_risk_assessment DAG를 활성화하면 매시 10분 자동 실행
```

컨테이너가 실행 중인 환경에서 자동 스케줄이 동작합니다. 실제 실행에는 기상청 API 인증키, 접근 가능한 S3 버킷·AWS 자격증명, Slack Webhook이 필요합니다.
