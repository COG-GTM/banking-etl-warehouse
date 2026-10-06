"""Put ``databricks/`` on sys.path so job scripts can ``import common`` / ``import analytics``.

``spark_python_task`` only adds the script's own directory (``databricks/jobs``) to sys.path.
The top-level ``databricks`` name is not used as a package because it would shadow the
``databricks`` namespace package (databricks-sdk, dbutils) on Databricks clusters.
"""

import sys
from pathlib import Path

ROOT = str(Path(__file__).resolve().parents[1])
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
