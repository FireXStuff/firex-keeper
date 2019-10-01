import os

from firexapp.engine.celery import app
from firexapp.submit.submit import get_log_dir_from_output
from firexapp.testing.config_base import FlowTestConfiguration, assert_is_good_run
from firexapp.events.model import RunStates

from firex_keeper import task_query
from firex_keeper.persist import get_db_manager


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
        assert run_metadata.chain == 'echo'
        assert run_metadata.firex_id == firex_id
        assert run_metadata.logs_dir == logs_dir

        all_uuids = {t.uuid for t in task_query.all_tasks(logs_dir)}
        assert run_metadata.root_uuid in all_uuids

    def assert_expected_return_code(self, ret_value):
        assert_is_good_run(ret_value)

