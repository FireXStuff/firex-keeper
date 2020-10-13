import logging
from collections import OrderedDict
import urllib.parse
from typing import List

import requests
from firexapp.events.model import FireXTask

from firex_keeper.persist import get_db_manager
from firex_keeper.keeper_helper import FireXTreeTask, get_keeper_url
from firex_keeper.keeper_web_app import TASKS_ROOT_URL


logger = logging.getLogger(__name__)


def get_keeper_path_url(logs_dir, path):
    keeper_url = get_keeper_url(logs_dir)
    return urllib.parse.urljoin(keeper_url, path)


def dict_to_firex_task(input_dict):
    return FireXTask(*input_dict.values())


def _task_dicts_to_firex_tasks(dicts: List[dict]) -> List[FireXTask]:
    return [dict_to_firex_task(d) for d in dicts]


def _http_get_keeper_path(logs_dir, path):
    if isinstance(path, list):
        path = '/'.join(path)
    return requests.get(get_keeper_path_url(logs_dir, path)).json(object_pairs_hook=OrderedDict)


def all_tasks(logs_dir, **kwargs) -> List[FireXTask]:
    task_dicts = _http_get_keeper_path(logs_dir, TASKS_ROOT_URL)
    return _task_dicts_to_firex_tasks(task_dicts)


def tasks_by_name(logs_dir, name, **kwargs) -> List[FireXTask]:
    # return _query_flame_tasks(logs_dir, _task_col_eq(TaskColumn.NAME, name), **kwargs)
    return None


def single_task_by_name(logs_dir, name, **kwargs) -> FireXTask:
    return None


def task_by_uuid(logs_dir, uuid, **kwargs) -> FireXTask:
    # TODO: handle 404.
    task_dict = _http_get_keeper_path(logs_dir, [TASKS_ROOT_URL, uuid])
    return dict_to_firex_task(task_dict)


def failed_tasks(logs_dir, **kwargs) -> List[FireXTask]:
    # RunStates.FAILED.value
    return None


def revoked_tasks(logs_dir, **kwargs) -> List[FireXTask]:
    # RunStates.REVOKED.value
    return None


def task_tree(logs_dir, root_uuid=None, **kwargs) -> FireXTreeTask:
    # with get_db_manager(logs_dir) as db_manager:
    #     if root_uuid is None:
    #         root_uuid = db_manager.query_single_run_metadata().root_uuid
    #
    #     # TODO: could avoid fetching all tasks by using sqlite recursive query.
    #     all_tasks_by_uuid = {t.uuid: t for t in db_manager.query_tasks(True, **kwargs)}
    #
    #     if root_uuid not in all_tasks_by_uuid:
    #         return None
    #
    # return _tasks_to_tree(root_uuid, all_tasks_by_uuid)
    return None


def wait_on_keeper_complete(logs_dir, timeout=15) -> bool:
    from firexapp.common import wait_until
    with get_db_manager(logs_dir) as db_manager:
        return wait_until(db_manager.is_keeper_complete, timeout=timeout, sleep_for=0.5)



