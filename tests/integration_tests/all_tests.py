from firexapp.engine.celery import app
from firexapp.submit.submit import get_log_dir_from_output
from firexapp.testing.config_base import FlowTestConfiguration, assert_is_good_run

from firex_keeper import task_query

@app.task()
def echo(arg_echo):
    return arg_echo


class KeepArgData(FlowTestConfiguration):
    def initial_firex_options(self) -> list:
        return ["submit", "--chain", "echo", '--arg_echo', 'value']

    def assert_expected_firex_output(self, cmd_output, cmd_err):
        logs_dir = get_log_dir_from_output(cmd_output)
        tasks = task_query.tasks_by_name(logs_dir, 'echo')
        assert tasks[0].name == 'echo'

    def assert_expected_return_code(self, ret_value):
        assert_is_good_run(ret_value)

