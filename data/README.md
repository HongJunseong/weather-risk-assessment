# 로컬 데이터

코드와 분리된 입력·작업 디렉터리다. 경로 설정은 `weather_risk_assessment/paths.py`를 사용한다.

- `border/`: 행정경계 ZIP 및 압축 해제한 Shapefile
- `admin_centroids.csv`: 행정구역 대표점
- `unique_admin_centroids.csv`, `grid_latlon.parquet`: 수집 호출 목록과 격자
- `live/`: 수집기 출력
- `tableau_public.csv`: Tableau Public에 수동 업로드하는 공개용 생성 파일

생성 파일은 Git에 추가하지 않는다. 저장소에 포함된 기존 경계 ZIP은 유지한다.
