from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]


def test_setup_job_runs_provision_notebook_with_settings_params():
    doc = yaml.safe_load((ROOT / "resources" / "setup.yml").read_text())
    job = doc["resources"]["jobs"]["banking_etl_setup"]
    (task,) = job["tasks"]
    nb = task["notebook_task"]
    assert (ROOT / "resources" / nb["notebook_path"]).resolve() == ROOT / "notebooks" / "setup" / "provision_uc.py"
    assert {"catalog", "schema_prefix", "create_catalog", "data_engineers", "jobs_principal"} <= set(nb["base_parameters"])
    # Serverless: no cluster spec (the dev workspace rejects classic job clusters).
    assert not ({"new_cluster", "job_cluster_key", "existing_cluster_id"} & set(task))
    assert set(doc["variables"]) == {"create_catalog", "data_engineers_group", "jobs_principal"}
