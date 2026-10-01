variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "name" {
  description = "Prefix for all resources"
  type        = string
  default     = "banking-etl"
}

variable "eks_cluster_name" {
  description = "Existing EKS cluster (with an IAM OIDC provider) that runs the Spark jobs"
  type        = string
}

variable "k8s_namespace" {
  type    = string
  default = "banking-etl"
}

variable "spark_service_account" {
  type    = string
  default = "banking-etl-spark"
}

variable "install_spark_operator" {
  description = "Install the Kubeflow Spark Operator Helm chart into the cluster"
  type        = bool
  default     = true
}

variable "spark_operator_chart_version" {
  type    = string
  default = "2.0.2"
}

variable "databricks_account_id" {
  description = "Databricks account id, used as the external id of the Unity Catalog storage credential role (empty to skip)"
  type        = string
  default     = ""
}

variable "databricks_uc_master_role_arn" {
  description = "Databricks Unity Catalog master role allowed to assume the storage credential role"
  type        = string
  default     = "arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL"
}

variable "tags" {
  type = map(string)
  default = {
    project = "banking-etl-warehouse"
  }
}
