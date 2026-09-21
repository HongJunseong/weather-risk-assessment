# 커밋 규칙

이 문서는 사람과 자동화 에이전트가 동일한 기준으로 로컬 커밋을 만드는 데 사용한다.
각 커밋은 독립적으로 검토할 수 있고 가능한 한 검증을 통과한 상태여야 한다.

## 메시지 형식

```text
<type>(<scope>): <summary>
```

`scope`는 의미가 분명할 때만 사용한다. 제목은 영문 명령형으로 작성하고 72자 이내로
유지한다. 변경 이유나 마이그레이션 정보가 제목만으로 충분하지 않으면 본문에 기록한다.

사용 가능한 `type`은 다음과 같다.

| type | 사용 시점 |
|---|---|
| `feat` | 데이터 파이프라인 기능 추가 |
| `fix` | 잘못된 동작이나 데이터 오류 수정 |
| `refactor` | 외부 동작을 유지하는 구조 개선 |
| `test` | 테스트와 fixture만 변경 |
| `docs` | 문서만 변경 |
| `ci` | CI 워크플로 변경 |
| `chore` | 의존성, 설정, 저장소 관리 작업 |

권장 `scope`는 `dag`, `collector`, `bronze`, `silver`, `gold`, `risk`, `alert`,
`docker`, `docs`, `ci`다. 목록에 없는 scope도 변경 범위를 더 정확히 나타내면 사용할 수 있다.

예시:

```text
feat(bronze): validate collector outputs before upload
fix(collector): derive forecast slots from the Airflow run time
test(collector): add representative KMA response fixtures
docs: document the local data layout
```

호환성을 깨는 변경은 `feat(scope)!:`처럼 `!`를 표시하고 본문에 영향과 이전 방법을
기록한다. 실제 이슈가 있을 때만 본문에 `Refs #123` 또는 `Closes #123`을 사용한다.

## 커밋 범위

- 서로 무관한 변경을 한 커밋에 섞지 않는다.
- 구현을 검증하는 테스트는 원칙적으로 구현과 같은 커밋에 포함한다. 독립적인 테스트
  인프라나 fixture 추가는 `test` 커밋으로 분리할 수 있다.
- 변경을 이해하는 데 꼭 필요한 문서는 구현과 함께 커밋할 수 있다. 대규모 문서 정리는
  별도 `docs` 커밋으로 분리한다.
- `.env`, 인증정보, 실제 API 응답 원본, 생성 데이터, 캐시 파일은 커밋하지 않는다.
- 다른 사람이 만들었거나 현재 작업과 무관한 변경은 스테이징하지 않는다.

## 자동 커밋 절차

사용자가 코드 변경을 요청한 경우, 에이전트는 하나의 검토 가능한 작업이 완료되고 관련
검증이 통과하면 추가 확인 없이 로컬 커밋을 만든다. 사용자가 커밋하지 말라고 요청하면
그 지시를 우선한다.

1. `git status --short`와 diff를 확인해 현재 작업에서 만든 변경을 식별한다.
2. 관련 테스트와 저장소 기본 검증을 실행한다.
3. 현재 작업에 속한 파일 또는 hunk만 명시적으로 스테이징한다. `git add .`와
   `git add -A`는 사용하지 않는다.
4. `git diff --cached --check`와 `git diff --cached`로 커밋 내용을 다시 확인한다.
5. 변경이 여러 목적이면 각 커밋이 검증 가능한 상태를 유지하도록 나눈다.
6. 커밋 후 hash, 메시지, 수행한 검증, 남아 있는 uncommitted 변경을 보고한다.

관련 테스트가 실패하거나 현재 작업과 기존 변경을 안전하게 분리할 수 없으면 자동
커밋하지 않는다. 실패 원인과 커밋하지 않은 이유를 보고한다. 사용자가 명시적으로
요청하지 않는 한 `--amend`, rebase, push, merge, tag 생성은 수행하지 않는다.
