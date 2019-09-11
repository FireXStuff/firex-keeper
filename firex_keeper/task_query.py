from firexapp.events.model import TaskColumn, RunStates
from firex_keeper.db_model import firex_tasks
from firex_keeper.persist import get_db_manager


def _task_col_eq(task_col, val):
    return firex_tasks.c[task_col.value] == val


def _query_tasks(logs_dir, query):
    return get_db_manager(logs_dir).query_tasks(query)


def all_tasks(logs_dir):
    return _query_tasks(logs_dir, True)


def tasks_by_name(logs_dir, name):
    return _query_tasks(logs_dir, _task_col_eq(TaskColumn.NAME, name))


def task_by_uuid(logs_dir, uuid):
    tasks = _query_tasks(logs_dir, _task_col_eq(TaskColumn.UUID, uuid))
    if not tasks:
        raise Exception("Found no task with UUID %s" % uuid)
    return tasks[0]


def task_by_name_and_arg_pred(logs_dir, name, arg, pred):
    tasks_with_name = tasks_by_name(logs_dir, name)
    return [t for t in tasks_with_name if arg in t.firex_bound_args and pred(t.firex_bound_args[arg])]


def task_by_name_and_arg_value(logs_dir, name, arg, value):
    pred = lambda arg_value: arg_value == value
    return task_by_name_and_arg_pred(logs_dir, name, arg, pred)


def failed_tasks(logs_dir):
    return _query_tasks(logs_dir, _task_col_eq(TaskColumn.STATE, RunStates.FAILED.value))


def revoked_tasks(logs_dir):
    return _query_tasks(logs_dir, _task_col_eq(TaskColumn.STATE, RunStates.REVOKED.value))
