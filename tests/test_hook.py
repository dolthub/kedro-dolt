import os
import shlex
import shutil
import subprocess
import tempfile
import time

import pymysql.cursors
import pytest

from kedro_dolt import DoltHook


@pytest.fixture(scope="function")
def config():
    yield dict(
        host="localhost",
        port=3306,
        user="root",
        password="",
        database="foo",
    )


@pytest.fixture(scope="function")
def doltdb_path(config):
    d = tempfile.TemporaryDirectory()
    try:
        db_path = os.path.join(d.name, config["database"])
        os.makedirs(db_path)
        yield db_path
    finally:
        shutil.rmtree(d.name)


@pytest.fixture(scope="function")
def sql_server(doltdb_path):
    p = None
    starting_path = os.getcwd()
    try:
        os.chdir(doltdb_path)
        subprocess.run(shlex.split("dolt init"))
        subprocess.run(shlex.split("dolt checkout -b new"))
        subprocess.run(shlex.split("dolt checkout master"))
        p = subprocess.Popen(
            shlex.split("dolt sql-server --max-connections=10 -l=trace"),
        )
        time.sleep(0.06)
        yield p
    finally:
        os.chdir(starting_path)
        if p is not None:
            p.kill()


def new_connection(config):
    return pymysql.connect(
        cursorclass=pymysql.cursors.DictCursor,
        **config,
    )


@pytest.fixture
def conn(sql_server, config):
    connection = new_connection(config)
    with connection as con:
        with con.cursor() as cursor:
            yield cursor

def compare_active_branch(config, branch):
    with new_connection(config) as conn:
        with conn.cursor() as cursor:
            cursor.execute("select active_branch() as b")
            res = cursor.fetchone()["b"]
            assert res == branch

@pytest.fixture(scope="function")
def hook(config):
    yield DoltHook(**config)


def test_checkout_branch(hook, sql_server, config):
    branches = ["master", "new", "new2", "master"]
    for br in branches:
        hook._checkout_branch(branch=br)
        compare_active_branch(config, br)


def test_active_branch(hook, conn):
    branches = ["master", "new", "master"]
    for br in branches:
        conn.execute(f"select dolt_checkout('{br}')")
        conn.execute(f"set global dolt_sql_server_branch_ref = 'refs/heads/{br}'")
        assert br == hook._active_branch()


def test_commit_empty(hook, conn, config):
    database = config["database"]
    conn.execute(f"select @@{database}_head as head")
    starting_head = conn.fetchone()["head"]

    commit = hook._commit(message="test")
    assert commit is None

    conn.execute(f"select @@{database}_head as head")
    res = conn.fetchone()["head"]
    assert res == starting_head


def test_commit_ok(hook, conn, config):
    database = config["database"]
    conn.execute(f"select @@{database}_head as head")
    starting_head = conn.fetchone()["head"]

    conn.execute("create table test (a bigint)")

    commit = hook._commit(message="test")
    assert commit != starting_head

    with new_connection(config) as con:
        with con.cursor() as cursor:
            cursor.execute(f"select @@{database}_head as head")
            res = cursor.fetchone()["head"]
            assert res == commit


def test_before_pipeline_run(hook, conn, config):
    hook.before_pipeline_run(dict(extra_params=dict(branch=None)))
    assert hook._original_branch is None

    conn.execute("select active_branch() as b")
    res = conn.fetchone()["b"]
    assert res == "master"

    hook.before_pipeline_run(dict(extra_params=dict(branch="new")))
    assert hook._original_branch == "master"

    compare_active_branch(config, "new")

def test_after_pipeline_run_checkout(hook, conn, config):
    branches = ["master", "new", "new2"]
    for br in branches:
        hook._original_branch = br
        hook.after_pipeline_run(run_params=dict(run_id="f"))

        compare_active_branch(config, br)
        #conn.execute("select active_branch() as b")
        #res = conn.fetchone()["b"]
        #assert res == br


def test_after_pipeline_run_commit(hook, conn, config):
    database = config["database"]
    conn.execute(f"select @@{database}_head as head")
    starting_head = conn.fetchone()["head"]

    conn.execute("create table test (a bigint)")
    hook._original_branch = "new2"
    commit = hook.after_pipeline_run(run_params=dict(run_id="f"))
    assert commit != starting_head

    with new_connection(config) as con:
        with con.cursor() as cursor:
            cursor.execute(f"select @@{database}_head as head")
            res = cursor.fetchone()["head"]
            assert res == commit


def test_e2e_pipeline_run_commit(hook, conn, config):
    database = config["database"]
    conn.execute(f"select @@{database}_head as head")
    starting_head = conn.fetchone()["head"]

    conn.execute("select active_branch() as b")
    starting_branch = conn.fetchone()["b"]
    workflow_branch = "new2"
    assert starting_branch != workflow_branch

    hook.before_pipeline_run(dict(extra_params=dict(branch=workflow_branch)))
    compare_active_branch(config, workflow_branch)
    #conn.execute("select active_branch() as b")
    #mid_branch = conn.fetchone()["b"]
    #assert mid_branch == workflow_branch
    assert hook._original_branch == starting_branch

    with new_connection(config) as conn:
        with conn.cursor() as cursor:
            cursor.execute("create table test (a bigint)")
        conn.commit()

    commit = hook.after_pipeline_run(run_params=dict(run_id="f"))
    assert commit != starting_head

    with new_connection(config) as conn:
        with conn.cursor() as cursor:
            cursor.execute("select HASHOF(active_branch()) as b")
            master_head = cursor.fetchone()["b"]

    assert master_head == starting_head

    with new_connection(config) as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"select HASHOF('{workflow_branch}') as b")
            workflow_head = cursor.fetchone()["b"]
            assert workflow_head == commit


def test_exceptions(hook):
    hook._database = "noexist"
    res = hook._active_branch()
    assert res is None
