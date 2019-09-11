from firexapp.submit.submit import get_log_dir_from_output
from firexapp.testing.config_base import FlowTestConfiguration, assert_is_bad_run, assert_is_good_run

from firex_keeper import task_query


class KeepArgData(FlowTestConfiguration):
    def initial_firex_options(self) -> list:
        return ["submit", "--chain", "a_service_to_test", '--use_this_arg', 'value']

    def assert_expected_firex_output(self, cmd_output, cmd_err):
        logs_dir = get_log_dir_from_output(cmd_output)
        tasks = task_query.tasks_by_name(logs_dir, 'a_service_to_test')
        assert tasks[0].name == 'a_service_to_test'

    def assert_expected_return_code(self, ret_value):
        assert_is_good_run(ret_value)

