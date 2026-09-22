# AWS 인프라

포트폴리오 환경의 S3 데이터 레이크, 파이프라인용 최소 권한 IAM 정책, 계정 전체 월 비용
Budget을 정의한다. IAM 사용자·역할·Access Key는 만들지 않는다. 배포 대상을 정한 뒤 출력된
정책 ARN을 EC2, ECS 또는 GitHub Actions OIDC 역할에 연결한다.

S3는 퍼블릭 접근과 암호화되지 않은 HTTP 요청을 차단하고 추가 비용이 없는 AES-256 기본
암호화를 사용한다. `bronze/`는 기본 30일, `integration-tests/`는 1일 후 만료하며 미완료
멀티파트 업로드는 7일 후 정리한다. Silver·Gold는 자동 삭제하지 않는다. Budget은 프로젝트
태그와 무관한 **AWS 계정 전체 비용**을 감시한다.

## 사용 방법

Terraform 1.10 이상과 AWS 임시 자격증명을 준비한 뒤 실행한다.

```bash
cd infra/terraform
cp terraform.tfvars.example terraform.tfvars
# bucket_name과 budget_alert_email 수정

terraform init
terraform fmt -check
terraform validate
terraform plan
```

`terraform.tfvars`와 state 파일은 Git에서 제외된다. Access Key나 비밀값을 Terraform 파일에
기록하지 않는다. `terraform apply`는 plan의 생성·변경·삭제 항목과 예상 비용을 검토한 뒤
직접 실행한다.

## 원격 state

`bootstrap/`은 메인 인프라와 분리된 S3 state 버킷을 만든다. 버킷은 퍼블릭 접근과 HTTP
요청을 차단하고 AES-256 암호화와 객체 버전 관리를 사용하며, Terraform에서 실수로 삭제할
수 없도록 보호한다. 메인 구성은 Git에서 제외된 `backend.hcl`을 읽어 S3 네이티브 잠금을
사용한다. DynamoDB 잠금 테이블은 만들지 않는다.

```bash
cd infra/terraform/bootstrap
cp terraform.tfvars.example terraform.tfvars
terraform init
terraform apply
cp backend.hcl.example backend.hcl
terraform init -migrate-state -backend-config=backend.hcl

cd ..
cp backend.hcl.example backend.hcl
terraform init -migrate-state -backend-config=backend.hcl
terraform plan
```
