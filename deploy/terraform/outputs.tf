output "lakehouse_bucket" {
  value = aws_s3_bucket.lakehouse.bucket
}

output "dwh_base_path" {
  description = "DWH_BASE_PATH for the EKS jobs"
  value       = "s3a://${aws_s3_bucket.lakehouse.bucket}/dwh"
}

output "landing_path" {
  value = "s3a://${aws_s3_bucket.lakehouse.bucket}/landing/transactions"
}

output "ecr_repository_url" {
  value = aws_ecr_repository.banking_etl.repository_url
}

output "spark_irsa_role_arn" {
  description = "Annotate the banking-etl-spark service account with this role"
  value       = aws_iam_role.spark_irsa.arn
}

output "sqlserver_secret_id" {
  description = "SOURCE_DB_SECRET_ID"
  value       = aws_secretsmanager_secret.sqlserver_source.name
}

output "databricks_uc_role_arn" {
  value = try(aws_iam_role.databricks_uc[0].arn, null)
}
