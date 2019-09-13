from typing import List

from firexapp.events.model import TaskColumn, RunStates, FireXTask, is_chain_exception, get_chain_exception_child_uuid
from firex_keeper.db_model import firex_tasks
from firex_keeper.persist import get_db_manager
from firex_keeper.keeper_helper import FireXTreeTask


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


def failed_tasks(logs_dir) -> List[FireXTask]:
    return _query_tasks(logs_dir, _task_col_eq(TaskColumn.STATE, RunStates.FAILED.value))


def revoked_tasks(logs_dir) -> List[FireXTask]:
    return _query_tasks(logs_dir, _task_col_eq(TaskColumn.STATE, RunStates.REVOKED.value))


def _index_by_parent_id(tasks_by_uuid):
    tasks_by_parent_id = {u: [] for u in tasks_by_uuid.keys()}

    for t in tasks_by_uuid.values():
        if t.parent_id is not None:
            tasks_by_parent_id[t.parent_id].append(t)

    return tasks_by_parent_id


def _tasks_to_tree(root_uuid, tasks_by_uuid) -> FireXTreeTask:
    tree_tasks_by_uuid = {u: FireXTreeTask(**{**t._asdict(), 'children': []}) for u, t in tasks_by_uuid.items()}
    tasks_by_parent_uuid = _index_by_parent_id(tree_tasks_by_uuid)

    root_task = tree_tasks_by_uuid[root_uuid]
    tasks_to_add = [root_task]

    while tasks_to_add:
        cur_task = tasks_to_add.pop()
        cur_task.children.extend(tasks_by_parent_uuid[cur_task.uuid])
        tasks_to_add += cur_task.children

    return root_task


def task_tree(logs_dir, root_uuid=None):
    db_manager = get_db_manager(logs_dir)
    if root_uuid is None:
        root_uuid = db_manager.query_single_run_metadata().root_uuid

    # TODO: could avoid fetching all tasks by using sqlite recursive query.
    all_tasks_by_uuid = {t.uuid: t for t in db_manager.query_tasks(True)}
    return _tasks_to_tree(root_uuid, all_tasks_by_uuid)


def flatten_tree(task_tree: FireXTreeTask) -> List[FireXTreeTask]:
    flat_tasks = []
    to_check = [task_tree]
    while to_check:
        cur_task = to_check.pop()
        to_check += cur_task.children
        flat_tasks.append(cur_task)

    return flat_tasks


def get_descendants(logs_dir, uuid) -> List[FireXTask]:
    subtree = task_tree(logs_dir, root_uuid=uuid)
    return [t for t in flatten_tree(subtree) if t.uuid != uuid]


def find_task_causing_chain_exception(task: FireXTreeTask):
    assert task.exception, "Expected exception, received: %s" % task.exception

    if not is_chain_exception(task) or not task.children:
        return task

    causing_uuid = get_chain_exception_child_uuid(task)
    causing_child = [c for c in task.children if c.uuid == causing_uuid]
    assert len(causing_child) == 1, \
        "Expected %s to have exactly one child with uuid %s, found %d." % (task.uuid, causing_uuid, len(causing_child))
    causing_child = causing_child[0]

    if not is_chain_exception(causing_child):
        return causing_child

    return find_task_causing_chain_exception(causing_child)
