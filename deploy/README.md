# Deployment

The `banking_etl` package (`pyspark/`) is deployed two ways:

| Target | Artifact | Orchestration | Tables |
|--------|----------|---------------|--------|
| Databricks | Python wheel built by the Asset Bundle (`databricks.yml`) | Databricks Workflow `banking-etl-warehouse` | Unity Catalog managed Delta tables `<catalog>.dwh.*` |
| AWS EKS | `linux/amd64` Spark image (`pyspark/Dockerfile`) in ECR | Argo Workflows + Spark Operator `SparkApplication`s | Delta tables on S3 (`s3a://<bucket>/dwh/<table>`) |

Both run the same dependency graph that replaced the manual Talend run order:

```
load_dim_branch ─┐
load_dim_account ─┼─> load_fact_transaction
load_dim_customer┘
```

```
deploy/
  databricks/banking_etl_job.yml       # Workflow (4 ETL tasks) + analytics job, included by ../databricks.yml
  databricks/analytics_notebook.py     # notebook wrapper for daily_transaction / balance_per_customer
  k8s/base/                            # namespace, IRSA service account + Spark RBAC, job ConfigMap
  k8s/sparkapplications/               # one SparkApplication per job (ad-hoc runs)
  k8s/jobs/spark-submit-local-job.yaml # alternative without the Spark Operator (plain Job, local[*])
  argo/                                # WorkflowTemplate (DAG), daily CronWorkflow, Argo RBAC
  terraform/                           # AWS resource stubs (S3, ECR, IAM/IRSA, Secrets Manager, Spark Operator)
```

## Required AWS resources

| Resource | Purpose | Terraform |
|----------|---------|-----------|
| S3 bucket `banking-etl-lakehouse-<account>` | `landing/transactions/` (Excel/CSV extracts) and `dwh/` (Delta tables for EKS); versioned, SSE-KMS, public access blocked | `aws_s3_bucket.lakehouse` |
| ECR repository `banking-etl` | Spark application image (immutable tags, scan on push) | `aws_ecr_repository.banking_etl` |
| Secrets Manager secret `banking-etl/sqlserver-source` | `{"username": "...", "password": "..."}` for the SQL Server source; value set out of band | `aws_secretsmanager_secret.sqlserver_source` |
| IAM role `banking-etl-spark-irsa` | IRSA role for the `banking-etl/banking-etl-spark` service account: S3 read/write on the bucket, `GetSecretValue` on the secret | `aws_iam_role.spark_irsa` |
| EKS cluster with IAM OIDC provider | Runs the Spark driver/executor pods; needs network access to SQL Server (1433) and S3 | existing (`var.eks_cluster_name`) |
| Spark Operator (Kubeflow, Helm) | Reconciles `SparkApplication`s; must watch the `banking-etl` namespace with the webhook enabled | `helm_release.spark_operator` |
| Argo Workflows | Runs the DAG / daily schedule (install with its Helm chart or upstream manifests) | not included |
| Databricks workspace with Unity Catalog | Catalog `banking`, schema `dwh`, volume `banking.landing.transactions` (or an external location on the bucket) | not included |
| IAM role `banking-etl-databricks-uc` | Unity Catalog storage credential for the bucket (set `databricks_account_id`) | `aws_iam_role.databricks_uc` |
| Instance profile for job clusters | Lets Databricks job clusters read the Secrets Manager secret (`instance_profile_arn` in `databricks.yml`) | not included |

```bash
cd deploy/terraform
terraform init
terraform apply -var eks_cluster_name=<cluster> [-var databricks_account_id=<id>]
aws secretsmanager put-secret-value --secret-id "$(terraform output -raw sqlserver_secret_id)" \
  --secret-string '{"username":"etl_reader","password":"<from your vault>"}'
```

The Terraform files are stubs: they assume an existing VPC/EKS cluster, use local state and default
encryption, and should be adapted to your module/backend conventions.

## AWS EKS

1. **Build and push the image** (`linux/amd64`; bundles Delta 3.2.1, mssql-jdbc 12.8.1, hadoop-aws 3.3.4):
   ```bash
   ECR=$(terraform -chdir=deploy/terraform output -raw ecr_repository_url)
   aws ecr get-login-password | docker login --username AWS --password-stdin "${ECR%%/*}"
   docker buildx build --platform linux/amd64 -f pyspark/Dockerfile -t "$ECR:1.0.0" --push .
   ```
2. **Fill in the environment-specific values** (or use a kustomize overlay):
   - `k8s/base/serviceaccount.yaml`: `eks.amazonaws.com/role-arn` = `terraform output spark_irsa_role_arn`
   - `k8s/base/configmap.yaml`: `DWH_BASE_PATH`, landing paths, `SOURCE_JDBC_URL`, `SOURCE_DB_SECRET_ID`, `AWS_REGION`
   - `image:` in `k8s/sparkapplications/*.yaml` and the `image` parameter in `argo/banking-etl-workflowtemplate.yaml`
3. **Upload the landing files** (`data_sources/transaction_excel.xlsx`, `transaction_csv.csv`) to
   `s3://<bucket>/landing/transactions/`.
4. **Deploy and run**:
   ```bash
   kubectl apply -k deploy/k8s/base
   kubectl apply -k deploy/argo
   argo submit -n banking-etl --from workflowtemplate/banking-etl --watch   # or wait for the 02:00 UTC CronWorkflow
   ```
   Single jobs can be run without Argo:
   ```bash
   kubectl apply -f deploy/k8s/sparkapplications/load-dim-branch.yaml
   kubectl get sparkapplication -n banking-etl -w
   kubectl apply -f deploy/k8s/sparkapplications/daily-transaction.yaml
   kubectl logs -n banking-etl daily-transaction-driver
   ```
   Re-running a `SparkApplication` requires deleting it first (`kubectl delete sparkapplication <name>`).

Credentials never live in the cluster: pods get AWS credentials through IRSA
(`WebIdentityTokenCredentialsProvider` for S3A, the default boto3 chain for Secrets Manager), and the
SQL Server login is fetched from Secrets Manager at runtime.

Without the Spark Operator, `k8s/jobs/spark-submit-local-job.yaml` runs a job with `spark-submit
--master local[*]` in a single pod, which is enough for this data volume.

## Databricks

The bundle at the repo root builds the wheel (`uv build --wheel`) and deploys two jobs:

- `banking-etl-warehouse`: the four ETL wheel tasks on a shared job cluster, `load_fact_transaction`
  depending on the three dimension tasks; daily 02:00 UTC schedule (paused by default).
- `banking-etl-analytics`: `daily_transaction` and `balance_per_customer` with job parameters
  `start_date`, `end_date`, `customer_name`.

Prerequisites: Unity Catalog catalog/schema (`banking.dwh`), the landing volume with the Excel/CSV
files, network access from the workspace to SQL Server, and an instance profile allowed to read the
Secrets Manager secret. Then:

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev --var="instance_profile_arn=arn:aws:iam::<acct>:instance-profile/<name>"
databricks bundle run -t dev banking_etl_warehouse
databricks bundle run -t dev banking_etl_analytics --params start_date=2024-01-18,end_date=2024-01-20,customer_name=shelly
```

The jobs run with `DWH_STORAGE=table`, so tables are created as `<catalog>.<schema>.dim_*` /
`fact_transaction`. `deploy/databricks/analytics_notebook.py` offers the same analytics interactively.
