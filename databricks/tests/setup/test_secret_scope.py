import pytest

from banking_etl.setup.secrets import SECRET_KEYS, ensure_scope, env_var_for, grant_read, put_values, read_values


class FakeCli:
    def __init__(self, scopes):
        self.scopes = scopes
        self.calls = []

    def __call__(self, args, stdin=None, **kw):
        self.calls.append((list(args), stdin))
        if args[:2] == ["secrets", "list-scopes"]:
            return [{"name": s} for s in self.scopes]
        return None


def test_env_var_names_and_read_values():
    assert [env_var_for(k) for k in SECRET_KEYS] == [
        "BANKING_ETL_JDBC_HOST", "BANKING_ETL_JDBC_PORT", "BANKING_ETL_JDBC_DATABASE",
        "BANKING_ETL_JDBC_USER", "BANKING_ETL_JDBC_PASSWORD",
    ]
    env = {env_var_for(k): f"v-{k}" for k in SECRET_KEYS}
    assert read_values(env)["jdbc-password"] == "v-jdbc-password"
    del env["BANKING_ETL_JDBC_PASSWORD"]
    with pytest.raises(SystemExit, match="BANKING_ETL_JDBC_PASSWORD"):
        read_values(env)


def test_ensure_scope_is_idempotent():
    cli = FakeCli(["other"])
    assert ensure_scope("banking-etl-sqlserver", runner=cli) is True
    assert cli.calls[-1][0] == ["secrets", "create-scope", "banking-etl-sqlserver"]
    cli = FakeCli(["banking-etl-sqlserver"])
    assert ensure_scope("banking-etl-sqlserver", runner=cli) is False
    assert len(cli.calls) == 1


def test_values_go_through_stdin_not_argv():
    cli = FakeCli([])
    put_values("s", {"jdbc-password": "hunter2"}, runner=cli)
    args, stdin = cli.calls[0]
    assert args == ["secrets", "put-secret", "s", "jdbc-password"]
    assert stdin == "hunter2" and "hunter2" not in args
    grant_read("s", ["app-1"], runner=cli)
    assert cli.calls[-1][0] == ["secrets", "put-acl", "s", "app-1", "READ"]
