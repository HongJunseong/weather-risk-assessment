# 🌏 재해 대응을 위한 준실시간 기상 위험 **평가** 시스템  
*Near Real-time Weather Risk **Assessment** System*

![python](https://img.shields.io/badge/Python-3.11-blue)
![airflow](https://img.shields.io/badge/Apache%20Airflow-2.7.3-017CEE)
![postgresql](https://img.shields.io/badge/PostgreSQL-15-336791)
![tableau](https://img.shields.io/badge/Tableau-Visualization-orange)
![kepler.gl](https://img.shields.io/badge/kepler.gl-Geospatial-00C2A0)

> **요약 한 줄**: 기상청 API를 30분 단위로 수집해 **준실시간(near real-time)** 으로 위험도를 **평가/산출**하고, **Tableau**와 **kepler.gl**로 직관적으로 시각화합니다. 현재 **Parquet + PostgreSQL 병행 운영**으로 로컬 검증과 DB 기반 자동화를 동시에 지원합니다.

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
- [로드맵](#-로드맵)
- [기여자](#-기여자)

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
- **처리**: API → pandas 전처리 → **가중치(※ `risk/weights.yaml`)** 적용 → `risk_latest.parquet` 산출
- **저장**: Parquet/CSV(로컬 검증) + **PostgreSQL**(시각화·자동화)

---

## 🔧 전체 시스템 구성
```mermaid
flowchart LR
    A[기상청 API] --> B[pandas 전처리/가공]
    B --> C[Parquet/CSV 저장]
    B --> D[PostgreSQL 적재]
    C --> E[로컬 검증/백업]
    D --> F[Tableau 라이브 연결]
    D --> G[kepler.gl (SQL 결과 Export/Feed)]
    subgraph H[Airflow DAG (30분 주기)]
    A
    B
    C
    D
    end
