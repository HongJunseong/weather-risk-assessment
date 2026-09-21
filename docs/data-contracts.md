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
| `uv` | `fcstDate, fcstTime, nx, ny` | `UVI` | 경고 후 허용 |

모든 데이터셋은 파일과 필수 컬럼이 존재해야 한다. 행이 있는 파일에는 다음 규칙을
적용한다.

- 키 컬럼은 null일 수 없다.
- 하나의 파일 안에서 키가 중복될 수 없다.
- `baseDate/baseTime`, `fcstDate/fcstTime`은 유효한 날짜와 시각이어야 한다.
- `nx`, `ny`는 숫자로 변환할 수 있어야 한다.
- 해당 데이터셋의 기상 지표 컬럼이 하나 이상 존재해야 한다.

태풍이 없거나 UV 응답이 없는 상황은 파이프라인 장애와 구분하기 위해 스키마를 갖춘
빈 파일로 표현한다. 이후 Silver 단계에서는 해당 위험 요소의 결측 처리 정책을 적용해야
한다. 빈 파일 자체를 생략하면 수집기 실행 누락과 구분할 수 없으므로 오류로 처리한다.

`tests/fixtures/kma/`에는 외부 API 없이 파서를 회귀 테스트할 수 있는 대표 응답이 있다.
이 fixture는 KMA 응답 필드 구조를 재현한 테스트 데이터이며 실제 운영 응답 원본은 아니다.
API 스키마 변경을 발견하면 개인정보나 인증정보를 제거한 응답 사례를 추가하고 기존
fixture를 무조건 덮어쓰지 않는다.
