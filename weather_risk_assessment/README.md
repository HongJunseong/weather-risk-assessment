# weather_risk_assessment

재해 대응을 위한 실시간 기상 위험 예측 프로젝트의 핵심 로직과 데이터 파이프라인 모듈을 담고 있는 패키지입니다.  
각 서브 폴더는 다음과 같은 역할을 합니다.

## 폴더 구조

### 1. collectors
- 기상청 OpenAPI 및 외부 데이터 소스로부터 원천 데이터를 수집하는 모듈
- 초단기/단기 예보, 태풍, 자외선 지수 등 수집 및 Parquet 변환

### 2. data
- 프로젝트 데이터 저장 공간
- `live/` : Airflow 실행 시 생성되는 최신 산출물 (gitignore 처리됨)  
- `border/` : 행정경계, 중심점 등 고정 레퍼런스 데이터 (압축 해제 필요)

### 3. risk
- 수집된 기상 데이터를 바탕으로 위험도를 계산하는 핵심 로직
- Heat, Rain, UV, Typhoon 등 개별 위험도 + 가중치 기반 통합 위험도 산출

### 4. scripts
- Airflow DAG 및 로컬 실행에 필요한 스크립트 모음
- 예: amdin_centroids 생성, 위험도 산출 태스크 실행 등

### 5. utils
- 공통 유틸리티 함수
- 단위 변환(mm, °C, m/s), 안전한 값 추출, 수학적 변환(logistic 등) 포함

### 6. warehouse
- 데이터 저장/적재 계층
- PostgreSQL 및 Parquet에 위험도 데이터 적재, risk_latest → risk_history 관리
