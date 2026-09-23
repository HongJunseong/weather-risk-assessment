# 데이터 계약

파이프라인은 S3 Bronze 업로드 전에 실행별 로컬 디렉터리의 모든 Parquet을 검사한다.
구현은 `weather_risk_assessment/contracts/bronze.py`에 있으며 계약 위반 시 업로드를
시작하지 않는다.

| 데이터셋 | 키 | 주요 지표 | 빈 데이터 |
|---|---|---|---|
| `ultra_nowcast` | `baseDate, baseTime, nx, ny` | `RN1, T1H, REH, WSD, PTY` 중 하나 이상 | 실패 |
| `ultra_shortfcst` | `fcstDate, fcstTime, nx, ny` | `RN1, T1H, REH, WSD, PTY, SKY` 중 하나 이상 | 실패 |
| `short_fcst` | `fcstDate, fcstTime, nx, ny` | `PCP, POP, TMP, REH, WSD, PTY, SKY` 중 하나 이상 | 실패 |
| `typhoon` | `fcstDate, fcstTime, nx, ny` | 거리, 최대풍속, 경보 | 경고 후 허용 |
| `uv` | `fcstDate, fcstTime, nx, ny` | `UVI` | 실패 |

모든 데이터셋은 파일과 필수 컬럼이 존재해야 한다. 행이 있는 파일에는 다음 규칙을
적용한다.

- 키 컬럼은 null일 수 없다.
- 하나의 파일 안에서 키가 중복될 수 없다.
- `baseDate/baseTime`, `fcstDate/fcstTime`은 유효한 날짜와 시각이어야 한다.
- `baseDate/baseTime`이 있는 자료는 실행 시각보다 4시간을 초과해 오래될 수 없다. API 게시
  지연에 대비한 초단기 수집기의 네 차례 후보 탐색 범위까지는 허용한다.
- `nx`, `ny`는 숫자로 변환할 수 있어야 한다.
- 해당 데이터셋의 기상 지표 컬럼이 하나 이상 존재해야 한다.

태풍 정보가 없는 상황만 스키마를 갖춘 빈 파일로 허용한다. UV는 수집 대상 지역·예보
시각의 95% 미만을 받으면 수집 단계에서 실패하고, 빈 파일도 Bronze 업로드 전에 거부한다.
빈 파일 자체를 생략하면 수집기 실행 누락과 구분할 수 없으므로 오류로 처리한다.
Gold Export는 비어 있거나 실행 시각보다 오래된 결과를 덮어쓰지 않는다.

`tests/fixtures/kma/`에는 외부 API 없이 파서를 회귀 테스트할 수 있는 대표 응답이 있다.
이 fixture는 KMA 응답 필드 구조를 재현한 테스트 데이터이며 실제 운영 응답 원본은 아니다.
정상 데이터 외에 인증 오류, 자료 없음, 정상 헤더와 빈 `items` 조합을 구분해 검사한다.
API 스키마 변경을 발견하면 개인정보나 인증정보를 제거한 응답 사례를 추가하고 기존
fixture를 무조건 덮어쓰지 않는다.

## Silver·Gold 계약

Silver와 Gold는 Delta 저장 직전에 `weather_risk_assessment/contracts/medallion.py`의
Spark 검증을 실행한다. 계약 위반이 있으면 잘못된 결과를 덮어쓰기 전에 작업을 실패시킨다.

| 데이터셋 | 유일 키 | 주요 검사 |
|---|---|---|
| `silver_risk_enriched` | `nx, ny, fcstDate, fcstTime` | 실행 파티션과 예보시각, 6개 위험도 범위 |
| `gold_risk_latest` | `admin_names` | 지역별 한 행, 예보시각, `R_total`과 위험등급 일치 |
| `gold_risk_daily` | `date, admin_names` | 일별 한 행, 평균·최댓값 범위, 관측 건수와 최대 위험 시각 |

모든 위험도 점수는 null 없이 0 이상 1 이하여야 한다. `gold_risk_latest`의 등급은
`LOW`, `MED`, `HIGH`, `VERY_HIGH` 순서로 각각 0.4, 0.6, 0.8 경계를 사용한다.
`gold_risk_daily`에서는 `r_total_max`가 `r_total_avg`보다 작을 수 없고 `obs_cnt`는
양의 정수여야 한다. 같은 계약의 pandas 검증 함수로 Spark 없이도 회귀 테스트한다.
