# Stubs for the AWS resources behind the PySpark warehouse (see ../README.md).
# Assumes an existing VPC + EKS cluster with an IAM OIDC provider and a Databricks
# workspace with Unity Catalog; review naming, encryption and retention before applying.

data "aws_caller_identity" "current" {}

locals {
  bucket_name   = "${var.name}-lakehouse-${data.aws_caller_identity.current.account_id}"
  oidc_issuer   = replace(data.aws_eks_cluster.this.identity[0].oidc[0].issuer, "https://", "")
  oidc_provider = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:oidc-provider/${local.oidc_issuer}"
}

# --- S3: landing zone (landing/) + Delta tables (dwh/) -------------------------------------

resource "aws_s3_bucket" "lakehouse" {
  bucket = local.bucket_name
}

resource "aws_s3_bucket_versioning" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "aws:kms"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "lakehouse" {
  bucket                  = aws_s3_bucket.lakehouse.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id
  rule {
    id     = "expire-noncurrent-versions"
    status = "Enabled"
    filter {}
    noncurrent_version_expiration {
      noncurrent_days = 30
    }
  }
}

# --- ECR: Spark application image ----------------------------------------------------------

resource "aws_ecr_repository" "banking_etl" {
  name                 = var.name
  image_tag_mutability = "IMMUTABLE"
  image_scanning_configuration {
    scan_on_push = true
  }
}

# --- Secrets Manager: SQL Server source credentials (value set out of band) ----------------

resource "aws_secretsmanager_secret" "sqlserver_source" {
  name        = "${var.name}/sqlserver-source"
  description = "JSON {\"username\": ..., \"password\": ...} for the legacy SQL Server source"
}

# --- IAM: IRSA role for the Spark driver/executor service account --------------------------

data "aws_iam_policy_document" "spark_assume" {
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]
    principals {
      type        = "Federated"
      identifiers = [local.oidc_provider]
    }
    condition {
      test     = "StringEquals"
      variable = "${local.oidc_issuer}:sub"
      values   = ["system:serviceaccount:${var.k8s_namespace}:${var.spark_service_account}"]
    }
    condition {
      test     = "StringEquals"
      variable = "${local.oidc_issuer}:aud"
      values   = ["sts.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "etl_access" {
  statement {
    sid       = "ListBucket"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.lakehouse.arn]
  }
  statement {
    sid       = "ReadWriteObjects"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = ["${aws_s3_bucket.lakehouse.arn}/*"]
  }
  statement {
    sid       = "ReadSourceSecret"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [aws_secretsmanager_secret.sqlserver_source.arn]
  }
}

resource "aws_iam_role" "spark_irsa" {
  name               = "${var.name}-spark-irsa"
  assume_role_policy = data.aws_iam_policy_document.spark_assume.json
}

resource "aws_iam_role_policy" "spark_irsa" {
  name   = "${var.name}-etl-access"
  role   = aws_iam_role.spark_irsa.id
  policy = data.aws_iam_policy_document.etl_access.json
}

# --- Spark Operator ---------------------------------------------------------------------------
# The chart creates RBAC in var.k8s_namespace: apply deploy/k8s/base before this release.

resource "helm_release" "spark_operator" {
  count            = var.install_spark_operator ? 1 : 0
  name             = "spark-operator"
  repository       = "https://kubeflow.github.io/spark-operator"
  chart            = "spark-operator"
  version          = var.spark_operator_chart_version
  namespace        = "spark-operator"
  create_namespace = true

  # Watch the job namespace; the webhook is required for envFrom/serviceAccount on pods.
  set {
    name  = "spark.jobNamespaces[0]"
    value = var.k8s_namespace
  }
  set {
    name  = "webhook.enable"
    value = "true"
  }
}

# --- Databricks Unity Catalog storage credential role (external location on the bucket) ----
# Register the role in Databricks as a storage credential, then create an external location
# for s3://<bucket>/landing (and optionally a managed location for the dwh schema).

data "aws_iam_policy_document" "databricks_uc_assume" {
  count = var.databricks_account_id == "" ? 0 : 1
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "AWS"
      identifiers = [var.databricks_uc_master_role_arn]
    }
    condition {
      test     = "StringEquals"
      variable = "sts:ExternalId"
      values   = [var.databricks_account_id]
    }
  }
}

resource "aws_iam_role" "databricks_uc" {
  count              = var.databricks_account_id == "" ? 0 : 1
  name               = "${var.name}-databricks-uc"
  assume_role_policy = data.aws_iam_policy_document.databricks_uc_assume[0].json
}

resource "aws_iam_role_policy" "databricks_uc" {
  count  = var.databricks_account_id == "" ? 0 : 1
  name   = "${var.name}-lakehouse-access"
  role   = aws_iam_role.databricks_uc[0].id
  policy = data.aws_iam_policy_document.etl_access.json
}
