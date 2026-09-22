output "bucket_name" {
  description = "파이프라인 데이터 레이크 버킷 이름"
  value       = aws_s3_bucket.data_lake.id
}

output "pipeline_s3_policy_arn" {
  description = "향후 실행 역할에 연결할 최소 권한 IAM 정책 ARN"
  value       = aws_iam_policy.pipeline_s3.arn
}
