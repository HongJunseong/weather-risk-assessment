# Tableau Public 포트폴리오 데모

Tableau Cloud 자동 게시가 아니라 무료 Tableau Public에 공개 가능한 CSV를 수동으로
업로드하는 데모다. Tableau 계정이나 인증정보는 파이프라인과 저장소에 넣지 않는다.

## 데이터 준비

화면을 먼저 구성할 때는 실제 예보가 아닌 시연용 값인
`examples/tableau/risk_dashboard_sample.csv`를 사용한다.

실제 파이프라인 결과로 교체하려면 S3의 `gold_export/risk_latest` Parquet 디렉터리를
로컬로 내려받고 다음을 실행한다. `admin_centroids.csv`는 기본 데이터 준비 과정에서
생성되는 파일이다.

```bash
aws s3 sync \
  s3://<버킷>/gold_export/risk_latest \
  data/tableau/risk_latest

python3 -m weather_risk_assessment.scripts.prepare_tableau_public \
  data/tableau/risk_latest \
  --output data/tableau_public.csv
```

CSV에는 공개 가능한 행정구역명, 예보 시각, 종합·지표별 위험도, 위험 등급, 대표 좌표만
포함된다. Tableau Public에 게시한 데이터는 누구나 볼 수 있으므로 자격증명과 비공개
데이터를 추가하지 않는다.

## 대시보드 만들기

1. [Tableau Public](https://public.tableau.com/)에 로그인하고 **Create → Web Authoring**을
   선택한다.
2. **Upload from computer**로 샘플 또는 생성한 CSV를 올린다.
3. `longitude`를 Columns, `latitude`를 Rows에 놓고 `admin_name`을 Detail,
   `risk_score`를 Color에 놓아 위험도 지도를 만든다.
4. 새 시트에서 `admin_name`을 Rows, `risk_score`를 Columns에 놓고 내림차순으로 정렬해
   지역별 막대 차트를 만든다.
5. 새 시트에서 Measure Values에 `rain_risk`, `heat_risk`, `wind_risk`, `uv_risk`,
   `typhoon_risk`를 넣고 Measure Names를 Color에 놓아 지표별 비교 차트를 만든다.
6. Dashboard에 세 시트를 배치하고 `risk_level` 필터와 `forecast_time` 표시를 추가한다.
7. 제목에 **Weather Risk Dashboard**를 입력하고 저장한 뒤 공개 URL을 복사한다.

## 포트폴리오 반영

게시 후 README의 Tableau 화면 아래에 공개 URL을 추가한다. 설명은 다음 범위가 정확하다.

> KMA 데이터 파이프라인이 산출한 지역별 위험도를 Tableau Public용 데이터셋으로 변환하고,
> 지도·지역 순위·위험요소 비교 대시보드로 시각화했습니다.

Tableau Cloud 자동 배포나 실시간 연결은 구현 범위에 포함하지 않는다.
