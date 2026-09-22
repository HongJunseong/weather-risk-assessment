variable "aws_region" {
  description = "Terraform state 버킷 리전"
  type        = string
  default     = "ap-northeast-2"
}

variable "state_bucket_name" {
  description = "전 세계에서 고유한 Terraform state 버킷 이름"
  type        = string

  validation {
    condition     = length(var.state_bucket_name) >= 3 && length(var.state_bucket_name) <= 63 && can(regex("^[a-z0-9][a-z0-9.-]*[a-z0-9]$", var.state_bucket_name))
    error_message = "state_bucket_name은 3~63자의 소문자, 숫자, 점, 하이픈으로 입력해야 합니다."
  }
}
