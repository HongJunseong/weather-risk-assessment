# 🌏 재해 대응을 위한 준실시간 기상 위험 **평가** 시스템  
*Near Real-time Weather Risk **Assessment** System*

![python](https://img.shields.io/badge/Python-3.11-blue)
![airflow](https://img.shields.io/badge/Apache%20Airflow-2.7.3-017CEE)
![postgresql](https://img.shields.io/badge/PostgreSQL-15-336791)
![tableau](https://img.shields.io/badge/Tableau-Visualization-orange)
![kepler.gl](https://img.shields.io/badge/kepler.gl-Geospatial-00C2A0)

> **요약 한 줄**: 기상청 API를 30분 단위로 수집해 **준실시간(near real-time)** 으로 위험도를 **평가/산출**하고, **Tableau**와 **kepler.gl**로 직관적으로 시각화합니다. 현재 **Parquet + PostgreSQL 병행 운영**으로 로컬 검증과 DB 기반 자동화를 동시에 지원하도록 하였습니다.

---

## 🗂 목차
- [프로젝트 요약](#-프로젝트-요약)
- [프로젝트 배경](#-프로젝트-배경)
- [기존 한계와 해결](#-기존-한계와-해결)
- [데이터 구성](#-데이터-구성)
- [전체 시스템 구성](#-전체-시스템-구성)
- [결과 및 시각화](#-결과-및-시각화)
- [기술적 도전 과제](#-기술적-도전-과제)
- [인프라 및 개발 환경](#-인프라-및-개발-환경)
- [PostgreSQL 도입 의의](#-postgresql-도입-의의)

---

## 📌 프로젝트 요약
- **목표**: 태풍·UV·단기예보 등 다양한 기상 지표를 통합하여 **지역별 기상 위험도**를 **30분 주기**로 평가하고 시각화
- **핵심**: Airflow 기반 자동화 파이프라인 → Parquet/DB 저장 → Tableau/kepler.gl 대시보드
- **현재 운영**: **Parquet(로컬 확인/백업) + PostgreSQL(시각화/자동화)** 병행  
- **용어**: *예측(prediction)*이 아니라 **평가(assessment)/산출(computation)/모니터링(monitoring)** 중심

---

## 🏗 프로젝트 배경
- 극한 기상(태풍/폭우/폭염) 빈도 증가 → **시의성 높은 위험도 제공** 필요
- 기상청 API는 단편적 지표 제공 → **종합 위험도** 부재
- 기존 대시보드: **수동 갱신**·**단일 지표 중심** → 즉각적 의사결정에 한계

---

## ⚠ 기존 한계와 해결
| 기존 한계 | 본 프로젝트의 해결 |
|---|---|
| 단일 지표 중심(예: 강수량만) | 태풍·UV·단기예보 **다중 지표 통합** 위험도 산출 |
| 배치·수동 갱신 | **Airflow**로 30분 주기 **자동화** |
| 파일 기반만 사용 | **PostgreSQL 연동**으로 무결성/확장성/자동화 강화 |
| 지도 시각화 정적/단편 | **Tableau(분석)** + **kepler.gl(동적 공간/시간)** 결합 |

---

## 📊 데이터 구성
- **출처**: 기상청 API
  - 초단기예보(기온·강수·풍속 등), 단기예보(시간별 변화)
  - 생활기상지수(UV Index), 태풍 예측(위치·거리·최대 풍속)
- **처리**: API 호출 → pandas 전처리 → **각 지표별 가중치(※ `risk/weights.yaml`)** 적용 → `risk_latest.parquet` 산출
- **저장**: Parquet/CSV(로컬 검증) + **PostgreSQL**(시각화·자동화)

---

## 🔧 전체 시스템 구성
<img width="350" height="520" alt="disrisk structure" src="https://github.com/user-attachments/assets/eba12d59-d31e-4637-9e71-0ac13a97d825" />



(모든 단계는 Airflow DAG로 30분 주기 자동 실행)


- **Airflow DAG**: 주기 실행 · 오류 로그 · 자동 재시도(백오프)
- **타임존**: `Asia/Seoul`(KST) 기준 스케줄링

---

## 📈 결과 및 시각화

### 1) Tableau 대시보드
- 지역별 **통합 위험도/지표 비교**(UV, 태풍 거리, 강수 등)
- 툴팁에서 **예측 시각 · 세부 지표** 확인
- **PostgreSQL 라이브 연결** 시 Airflow 갱신 → 대시보드 **자동 반영**
- 스크린샷 예시(교체): `dashboards/tableau_overview.png`

### 2) kepler.gl 시각화
- **행정구역 중심 좌표** 기반 공간 시각화
- **시간 슬라이더**로 시간별 위험도 변화 추적
- 애니메이션/GIF로 **동적 공유** 가능
- 예시(교체): `dashboards/kepler_timelapse.gif`

---

## 🧩 기술적 도전 과제
1. **이질적 API 응답(XML/JSON) 통합** → 공통 파서 + pandas 스키마 표준화
2. **시간대 정렬/결측치 처리** → `pendulum`으로 타임존 일원화(KST) 및 보정
3. **DAG 안정성 강화** → 재시도/백오프 전략, 구조화된 로깅
4. **DB 전환 병행 운영** → Parquet + PostgreSQL 동시 운영, Tableau/kepler.gl 연동 검증

---

## 🖥 인프라 및 개발 환경
- **언어/라이브러리**: Python 3.11, pandas, requests, pendulum
- **워크플로우**: Apache Airflow 2.7.3 (Docker Compose)
- **저장소**: Parquet/CSV + **PostgreSQL 15**
- **시각화**: **Tableau Public**, **kepler.gl**
- **운영**: GitHub Actions(CI/CD), Docker

---

## 🗄 PostgreSQL 도입 의의

**왜 DB까지?** 파일 대비 **무결성/자동화/확장성**에서 장점이 큽니다.

- **데이터 관리/무결성**: PK·Index 기반으로 **중복 제어/품질 보장**
- **업서트(UPSERT)**: 30분 주기 갱신 시 **중복 없이 최신화**
- **SQL 탐색성**: 조건/집계/조인 기반 **즉시 분석**
- **지리공간 확장(PostGIS)**: 태풍 경로/버퍼/폴리곤 교차 등 **공간 분석** 고도화

### DDL 예시 (최소 스키마)

```sql
CREATE TABLE IF NOT EXISTS risk_latest (
  fcst_time      timestamptz NOT NULL,
  admin_code     varchar(20) NOT NULL,
  r_total        numeric,
  uv_index       numeric,
  ty_distance_km numeric,
  precip_mm      numeric,
  PRIMARY KEY (fcst_time, admin_code)
);

CREATE INDEX IF NOT EXISTS idx_risk_latest_time  ON risk_latest (fcst_time);
CREATE INDEX IF NOT EXISTS idx_risk_latest_admin ON risk_latest (admin_code);

-- 공간데이터 확장 시:
-- CREATE EXTENSION IF NOT EXISTS postgis;
-- ALTER TABLE risk_latest ADD COLUMN geom geometry(Point, 4326);

```

### 분석 쿼리 예시

```sql
-- 최근 6시간 지역별 평균 위험도 Top-N
SELECT admin_code, ROUND(AVG(r_total)::numeric, 3) AS avg_risk
FROM risk_latest
WHERE fcst_time >= NOW() - interval '6 hours'
GROUP BY admin_code
ORDER BY avg_risk DESC
LIMIT 20;

```
