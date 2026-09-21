---
name: commit
description: 이 저장소에서 완료되고 검증된 변경을 type과 scope에 맞춰 로컬 커밋한다. 사용자가 커밋을 요청하거나 자동 커밋 정책에 따라 작업을 마무리할 때 사용하며 push, merge, rebase, tag, release에는 사용하지 않는다.
---

# 완료된 작업 커밋

커밋 전에 [`../../../docs/commit-conventions.md`](../../../docs/commit-conventions.md)를
읽고 메시지 형식, type·scope 선택, 커밋 분리 기준의 단일 기준으로 사용한다.

사용자는 완료된 저장소 작업의 로컬 커밋을 허용했다. 이 권한에는 push, merge, rebase,
amend, tag 생성, 배포가 포함되지 않는다.

1. `git status --short`와 staged·unstaged diff를 확인해 현재 작업과 기존 변경을 구분한다.
2. 변경에 맞는 검증을 실행한다. 사용자가 WIP 커밋을 명시하지 않았다면 실패가 확인된
   구현을 커밋하지 않는다.
3. diff의 주목적을 보고 `feat`, `fix`, `refactor`, `perf`, `test`, `docs`, `build`,
   `ci`, `chore`, `revert` 중 type을 자동 선택한다. 중심 영역이 하나일 때 scope도 자동
   선택한다. 커밋 제목과 본문은 한국어로 작성한다.
4. 하나의 목적에 해당하는 파일이나 hunk만 명시적으로 스테이징한다. `git add .`와
   `git add -A`는 사용하지 않는다.
5. `git diff --cached --check`, `git diff --cached`, 커밋 메시지를 검토한다. 인증정보,
   로컬 데이터, 캐시, 현재 작업과 무관한 변경을 포함하지 않는다.
6. 각 커밋이 검토 가능하고 실행 가능한 상태를 유지하도록 목적별로 나눠 로컬 커밋한다.
7. 커밋 hash와 제목, 수행한 검증, 의도적으로 남긴 변경을 보고한다.

현재 작업을 기존 변경에서 안전하게 분리할 수 없으면 소유권을 추측하지 말고 커밋하지
않은 채 겹치는 내용을 설명한다.
