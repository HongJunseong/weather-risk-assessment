variable "aws_region" {
  description = "AWS 리전"
  type        = string
  default     = "ap-northeast-2"
}

variable "bucket_name" {
  description = "전 세계에서 고유한 데이터 레이크 S3 버킷 이름"
  type        = string

  validation {
    condition     = length(var.bucket_name) >= 3 && length(var.bucket_name) <= 63 && can(regex("^[a-z0-9][a-z0-9.-]*[a-z0-9]$", var.bucket_name))
    error_message = "bucket_name은 3~63자의 소문자, 숫자, 점, 하이픈으로 입력해야 합니다."
  }
}

variable "project_name" {
  description = "리소스 태그와 이름에 사용할 프로젝트명"
  type        = string
  default     = "weather-risk-assessment"
}

variable "environment" {
  description = "리소스 환경 구분"
  type        = string
  default     = "portfolio"
}

variable "bronze_retention_days" {
  description = "Bronze 원천 데이터 보존 일수"
  type        = number
  default     = 30

  validation {
    condition     = var.bronze_retention_days >= 1
    error_message = "bronze_retention_days는 1 이상이어야 합니다."
  }
}

variable "monthly_budget_usd" {
  description = "계정 전체 월 비용 예산(USD)"
  type        = number
  default     = 5

  validation {
    condition     = var.monthly_budget_usd > 0
    error_message = "monthly_budget_usd는 0보다 커야 합니다."
  }
}

variable "budget_alert_email" {
  description = "예산 알림을 받을 이메일 주소"
  type        = string

  validation {
    condition     = can(regex("^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$", var.budget_alert_email))
    error_message = "유효한 budget_alert_email을 입력해야 합니다."
  }
}
