import logging

from typing import List

from firexapp.events.model import FireXTask, TaskColumn, RunStates
from firex_keeper.persist import get_db_manager
from firex_keeper.db_model import firex_tasks

logger = logging.getLogger(__name__)


class FireXTaskQueryException(Exception):
    pass


def _task_col_eq(task_col, val):
    return firex_tasks.c[task_col.value] == val


def _query_tasks(logs_dir, query, **kwargs) -> List[FireXTask]:
    with get_db_manager(logs_dir) as db_manager:
        return db_manager.query_tasks(query, **kwargs)


def all_tasks(logs_dir, **kwargs) -> List[FireXTask]:
    return _query_tasks(logs_dir, True, **kwargs)


def task_by_uuid(logs_dir, uuid, **kwargs) -> FireXTask:
    tasks = _query_tasks(logs_dir, _task_col_eq(TaskColumn.UUID, uuid), **kwargs)
    if not tasks:
        raise FireXTaskQueryException("Found no task with UUID %s" % uuid)
    return tasks[0]


def tasks_by_name(logs_dir, name, **kwargs) -> List[FireXTask]:
    return _query_tasks(logs_dir, _task_col_eq(TaskColumn.NAME, name), **kwargs)


def tasks_by_state(logs_dir, state, **kwargs) -> List[FireXTask]:
    return _query_tasks(logs_dir, _task_col_eq(TaskColumn.STATE, state), **kwargs)


def failed_tasks(logs_dir, **kwargs) -> List[FireXTask]:
    return tasks_by_state(logs_dir, RunStates.FAILED.value, **kwargs)


def revoked_tasks(logs_dir, **kwargs) -> List[FireXTask]:
    return tasks_by_state(logs_dir, RunStates.REVOKED.value, **kwargs)
