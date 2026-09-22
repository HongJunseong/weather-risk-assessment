output "state_bucket_name" {
  description = "Terraform S3 backend 버킷 이름"
  value       = aws_s3_bucket.state.id
}
