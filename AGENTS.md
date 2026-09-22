# 프로젝트 작업 지침

- Python 기상 데이터 파이프라인이다. 구조·데이터 흐름 작업에는 `docs/architecture.md`, 운영 제약·우선순위 작업에는 `docs/maintenance.md`를 필요한 범위만 확인한다.
- `dags/`는 작업 순서와 스케줄, `collectors/`는 수집, `risk/`는 계산, `jobs/`는 Spark 변환, `scripts/`는 데이터 준비·내보내기를 담당한다.
- 로컬 데이터 경로는 `weather_risk_assessment/paths.py`를 사용한다. 소스 패키지에 데이터를 저장하거나 `sys.path`를 수정하지 않는다. 유틸리티는 저장소 루트에서 `python -m weather_risk_assessment.scripts.<모듈>`로 실행한다.
- 위험도 가중치와 종합 공식의 기준은 `risk/config.py`다. 계산 변경 시 경계값·결측치·기존 결과에 미치는 영향을 검증한다.
- 예보 시각과 파티션은 KST 기준이다. 시간대가 없는 pandas 시각과 시간대가 있는 시각을 혼합하는 변경은 주의해서 검증한다.
- 스키마·S3 경로·Airflow task_id·스케줄 변경은 downstream 영향과 이력 호환성을 검토하고 문서에 기록한다.
- `.env`와 실제 수집 결과는 커밋하지 않는다. 환경변수 추가 시 `.env.example`과 실행 문서를 갱신한다.
- 외부 API·S3·Slack 없이 가능한 검증부터 수행한다. 기본 검증: `python3 -m unittest discover -s tests -q`, `python3 -m compileall -q weather_risk_assessment dags tests`.
- Docker 환경이 있으면 `docker compose --env-file .env -f docker/docker-compose.yaml config --quiet`로 설정을 확인한다. 외부 서비스를 사용한 실제 실행 여부는 검증 결과에 명시한다.
- 필요한 파일 구간만 읽고 독립적인 조회는 묶어서 실행한다. 같은 세션에서 변경 없는 문서·스킬을 다시 읽지 않으며, 도구 출력과 테스트 로그를 제한하고 통과한 검증은 관련 변경이 없는 한 반복하지 않는다.
- 하나의 검토 가능한 작업이 완료되고 관련 검증이 통과하면 `.agents/skills/commit/SKILL.md`에 따라 로컬 커밋을 만든다. 별도 요청 없이 push, merge, rebase, amend, tag 생성은 하지 않는다.
- 반복 절차가 확인된 뒤 스킬로 분리한다. 일반 개발 상식을 장황하게 지침에 추가하지 않는다.
