# 프로젝트 구조

```text
.
├── AGENTS.md                 # AI 작업 지침
├── README.md                 # 소개와 시작점
├── docs/                     # 구조·운영·점검 결과
├── dags/                     # Airflow 오케스트레이션
├── docker/                   # 컨테이너와 Compose 설정
├── infra/terraform/          # AWS S3·IAM 정책·Budget 정의
├── requirements/             # 실행·개발 의존성
├── tests/                    # 외부 서비스 없이 수행하는 회귀 테스트
├── data/                     # 로컬 입력과 생성 결과
│   ├── border/               # 행정경계 원본 ZIP 및 압축 해제 파일
│   └── live/                 # 수집된 Parquet (실행 시 생성)
└── weather_risk_assessment/  # Python 패키지
    ├── paths.py              # 공통 데이터 경로
    ├── collectors/           # 기상청 API 수집
    ├── risk/                 # 지표별 위험도와 종합 공식
    ├── jobs/                 # Spark Silver/Gold/Export 작업
    ├── scripts/              # 행정구역 준비·Bronze 업로드·Tableau CSV 준비
    ├── alerts/               # Slack 알림
    └── utils/                # 시간·좌표·수치 변환
```

## 실행 흐름

행정경계 → 중심점 CSV → 고유 격자/호출 목록 → 기상청 수집 → 로컬 `data/live/dt=.../`
→ Bronze 계약 검사 → S3 Bronze → Spark Silver → Gold latest/daily → S3 Parquet export → Slack.
Airflow는 이 흐름을 KST 기준 매시 10분에 시작하며 실패 작업을 5분 간격으로 최대 2회
재시도한다. 실행 경로의 `dt`는 스케줄 구간 종료 시각의 연월일시를 사용한다.
Tableau Public 데모는 운영 DAG와 분리하며, 내려받은 latest Parquet을 공개용 CSV로 변환해
수동으로 게시한다.

코드 패키지명과 기존 실행 태스크의 DAG/task ID는 유지한다. `scripts/`도 DAG에서 import하는
Python 모듈이므로 패키지 내부에 둔다. 별도 `src/` 계층은 도입하지 않았다.

## 경로와 데이터

- 기본 데이터 루트: 저장소의 `data/`. `WEATHER_DATA_DIR`로 변경할 수 있다.
- 수집 데이터: `<데이터 루트>/live/dt=YYYYMMDDHH/`. 기존 `DRE_SINK_DIR`로
  `live` 루트를 재정의할 수 있다. 실행별 디렉터리는 이전 결과의 오업로드를 방지한다.
- 환경변수는 모듈을 import하기 전에 설정한다. 상대 경로는 프로세스 작업 디렉터리 기준이므로 운영에서는 절대 경로를 사용한다.
- Docker는 호스트 `data/`를 `/opt/airflow/data`에 마운트하고 해당 경로를 설정한다.
- `admin_centroids.csv`, `unique_admin_centroids.csv`, `grid_latlon.parquet`은 데이터 루트에 생성된다.
- S3 Bronze/Silver/Gold 경로와 위험도 공식은 이번 구조 변경에서 유지했다.
- Bronze 업로드 전 계약은 `docs/data-contracts.md`에 정의한다. 계약 오류가 있으면 AWS
  호출 전에 실패하므로 손상된 실행 파티션이 S3에 생성되지 않는다.
- Tableau Public용 CSV는 latest Parquet의 행정구역별 위험도와 `admin_centroids.csv`의
  대표 좌표를 결합한다. Tableau 계정 인증과 게시는 운영 DAG에서 수행하지 않는다.
- latest Parquet export에는 기존 컬럼을 유지하면서 정확한 좌표 결합을 위한 `nx`, `ny`와
  `R_rain`, `R_heat`, `R_wind`, `R_uv`, `R_typhoon`을 추가한다. 기존 export를 사용하는
  환경은 다시 실행해야 새 컬럼을 얻는다.

## 기존 환경에서 이전

기존 `weather_risk_assessment/data/`의 내용을 루트 `data/`로 이동한다.
이번 작업에서는 저장소에 존재하던 ZIP, Hyper 파일, Windows 부가정보 파일을 이동해 보존했다.
Hyper 및 `Zone.Identifier` 파일은 생성물로 취급하여 새 위치에서는 Git 추적에서 제외한다.
기존 `PROJECT_DRE_ROOT` 대신 `WEATHER_DATA_DIR`를 사용한다. 패키지 내부 데이터 경로를
가리키는 사용자 설정·외부 스크립트도 갱신한다. 컨테이너는 이미지 재빌드와 재생성이 필요하다.
