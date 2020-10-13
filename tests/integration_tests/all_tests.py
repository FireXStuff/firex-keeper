import os
import urllib.parse

import requests
from firexapp.engine.celery import app
from firexapp.submit.submit import get_log_dir_from_output
from firexapp.testing.config_base import FlowTestConfiguration, assert_is_good_run
from firexapp.events.model import RunStates
from firexapp.common import wait_until

from firex_keeper import task_query
from firex_keeper.persist import get_db_manager, task_by_uuid_exp
from firex_keeper.keeper_helper import get_keeper_url


@app.task()
def echo(arg_echo):
    return arg_echo


class KeepNoopData(FlowTestConfiguration):
    def initial_firex_options(self) -> list:
        return ["submit", "--chain", "echo", '--arg_echo', 'value']

    def assert_expected_firex_output(self, cmd_output, cmd_err):
        logs_dir = get_log_dir_from_output(cmd_output)
        tasks = task_query.tasks_by_name(logs_dir, 'echo')
        assert tasks[0].name == 'echo'
        assert tasks[0].state == RunStates.SUCCEEDED.value

        firex_id = os.path.basename(logs_dir)
        with get_db_manager(logs_dir) as db_manager:
            run_metadata = db_manager.query_run_metadata(firex_id)
            is_complete = wait_until(db_manager.is_keeper_complete, timeout=5, sleep_for=0.5)
            assert is_complete is True

        assert run_metadata.chain == 'echo'
        assert run_metadata.firex_id == firex_id
        assert run_metadata.logs_dir == logs_dir

        all_uuids = {t.uuid for t in task_query.all_tasks(logs_dir)}
        assert run_metadata.root_uuid in all_uuids

    def assert_expected_return_code(self, ret_value):
        assert_is_good_run(ret_value)


@app.task(bind=True)
def wait_before_query_on_self(self, uid):
    self_task = task_query.task_by_uuid(uid.logs_dir, self.request.id,
                                        wait_for_exp_exist=task_by_uuid_exp(self.request.id),
                                        error_on_wait_exceeded=True)
    assert self_task.uuid == self.request.id


class WaitOnSelfQueryTest(FlowTestConfiguration):
    def initial_firex_options(self) -> list:
        return ["submit", "--chain", "wait_before_query_on_self"]

    def assert_expected_firex_output(self, cmd_output, cmd_err):
        pass

    def assert_expected_return_code(self, ret_value):
        assert_is_good_run(ret_value)


class CompleteTest(FlowTestConfiguration):
    sync = False

    def initial_firex_options(self) -> list:
        return ["submit", "--chain", "nop"]

    def assert_expected_firex_output(self, cmd_output, cmd_err):
        logs_dir = get_log_dir_from_output(cmd_output)
        keeper_complete = task_query.wait_on_keeper_complete(logs_dir)
        assert keeper_complete, "Keeper database is not complete."

    def assert_expected_return_code(self, ret_value):
        assert_is_good_run(ret_value)


class KeeperWebserverTest(FlowTestConfiguration):
    sync = False

    def initial_firex_options(self) -> list:
        return ["submit", "--chain", "sleep", '--sleep', '10', '--disable_blaze', 'True']

    def assert_expected_firex_output(self, cmd_output, cmd_err):
        logs_dir = get_log_dir_from_output(cmd_output)
        keeper_url = get_keeper_url(logs_dir)
        assert requests.get(urllib.parse.urljoin(keeper_url, '/alive')).ok, "Expected keeper webserver to be alive."

        from firex_keeper import keeper_rest_client
        tasks = keeper_rest_client.all_tasks(logs_dir)
        print(tasks)
        assert any(t.name == 'sleep' for t in tasks)

    def assert_expected_return_code(self, ret_value):
        assert_is_good_run(ret_value)
