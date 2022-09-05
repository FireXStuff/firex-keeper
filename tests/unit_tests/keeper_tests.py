import unittest
import tempfile
import os
import stat
from pathlib import Path

from firexkit.result import ChainInterruptedException
from firexapp.events.model import RunStates, FireXRunMetadata
from firex_keeper.keeper_event_consumer import TaskDatabaseAggregatorThread
from firex_keeper.persist import (
    task_by_uuid_exp, task_uuid_complete_exp, FireXWaitQueryExceeded, create_db_manager,
    get_db_file, get_db_manager, get_keeper_complete_file_path)
from firex_keeper import task_query
from firex_keeper.keeper_helper import can_any_write

from sqlalchemy.exc import OperationalError


def _write_events_to_db(logs_dir, events, cleanup=True):
    run_metadata = FireXRunMetadata('1', logs_dir, 'Noop', None)
    aggregator_thread = TaskDatabaseAggregatorThread(None, run_metadata)

    for e in events:
        aggregator_thread._on_celery_event(e)

    if cleanup:
        aggregator_thread._on_cleanup()

    return aggregator_thread


def chain_exception_str(uuid):
    return ChainInterruptedException(task_id=uuid).__repr__()

tree_events = [
    #           1
    #       2       3
    #              4 5
    {'uuid': '1', 'name': 'Root', 'parent_id': None, 'exception': chain_exception_str('3')},
    {'uuid': '2', 'name': 'Noop', 'parent_id': '1'},
    {'uuid': '3', 'name': 'Noop', 'parent_id': '1', 'exception': chain_exception_str('4')},
    {'uuid': '4', 'name': 'Noop', 'parent_id': '3', 'exception': "SomeNonChainException()"},
    {'uuid': '5', 'name': 'Noop', 'parent_id': '3'},
]


class FireXKeeperTests(unittest.TestCase):

    def test_query_by_name_and_uuid(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            logs_dir = str(tmpdirname)
            _write_events_to_db(logs_dir, [
                {'uuid': '1', 'name': 'Noop'},
                {'uuid': '2', 'name': 'Noop'},
                {'uuid': '3', 'name': 'Other'},
            ])

            tasks = task_query.tasks_by_name(logs_dir, 'Noop')
            self.assertEqual(2, len(tasks))
            self.assertEqual('Noop', tasks[0].name)

            other_task = task_query.task_by_uuid(logs_dir, '3')
            self.assertEqual('Other', other_task.name)

    def test_query_all(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            logs_dir = str(tmpdirname)
            _write_events_to_db(logs_dir, [
                {'uuid': '1', 'name': 'Noop'},
                {'uuid': '2', 'name': 'Other'},
                {'uuid': '3', 'name': 'Third'},
            ])
            self.assertEqual(3, len(task_query.all_tasks(logs_dir)))

    def test_query_failure_after_update(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            logs_dir = str(tmpdirname)
            _write_events_to_db(logs_dir, [
                {'uuid': '1', 'name': 'Noop'},
                {'uuid': '2', 'name': 'Noop'},
                {'uuid': '1', 'type': RunStates.FAILED.value},
            ])

            tasks = task_query.failed_tasks(logs_dir)
            self.assertEqual(1, len(tasks))
            self.assertEqual(RunStates.FAILED.value, tasks[0].state)

    def test_query_tree(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            logs_dir = str(tmpdirname)
            _write_events_to_db(logs_dir, tree_events)

            tree = task_query.task_tree(logs_dir, root_uuid='3')
            child_uuids = {t.uuid for t in tree.children}
            self.assertEqual({'4', '5'}, child_uuids)

    def test_query_tree_not_found(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            logs_dir = str(tmpdirname)
            _write_events_to_db(logs_dir, tree_events)

            tree = task_query.task_tree(logs_dir, root_uuid='not exists')
            self.assertIsNone(tree)

    def test_get_decendants(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            logs_dir = str(tmpdirname)
            _write_events_to_db(logs_dir, tree_events)

            tasks = task_query.get_descendants(logs_dir, '3')
            child_uuids = {t.uuid for t in tasks}
            self.assertEqual({'4', '5'}, child_uuids)

    def test_get_decendants_not_found(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            logs_dir = str(tmpdirname)
            _write_events_to_db(logs_dir, tree_events)

            tasks = task_query.get_descendants(logs_dir, 'not found UUID')
            self.assertEqual([], tasks)

    def test_find_task_causing_chain_exception(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            logs_dir = str(tmpdirname)
            _write_events_to_db(logs_dir, tree_events)

            tree = task_query.task_tree(logs_dir)
            causing_task = task_query.find_task_causing_chain_exception(tree)
            self.assertEqual('4', causing_task.uuid)

    def test_wait_for_task_exist(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            logs_dir = str(tmpdirname)
            _write_events_to_db(logs_dir, [
                {'uuid': '1', 'name': 'Noop'},
                {'uuid': '2', 'name': 'Noop'},
                {'uuid': '3', 'name': 'Other'},
            ])
            tasks = task_query.tasks_by_name(logs_dir, 'Noop', wait_for_exp_exist=task_by_uuid_exp('3'), max_wait=1,
                                             error_on_wait_exceeded=True)
            self.assertEqual(2, len(tasks))

    def test_wait_for_task_not_exist(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            logs_dir = str(tmpdirname)
            _write_events_to_db(logs_dir, [
                {'uuid': '1', 'name': 'Noop'},
                {'uuid': '2', 'name': 'Noop'},
                {'uuid': '3', 'name': 'Other'},
            ])

            self.assertRaises(FireXWaitQueryExceeded, task_query.tasks_by_name, logs_dir, 'Noop',
                              wait_for_exp_exist=task_by_uuid_exp('some fake uuid'), max_wait=1,
                              error_on_wait_exceeded=True)

    def test_wait_for_task_complete(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            logs_dir = str(tmpdirname)

            aggregator = _write_events_to_db(
                logs_dir,
                [
                    {'uuid': '1', 'name': 'Noop', 'type': RunStates.STARTED.value},
                    {'uuid': '2', 'name': 'Noop'},
                ],
                cleanup=False)
            self.assertTrue(can_any_write(get_db_file(logs_dir)))

            # Make sure that task 1 is not yet complete
            # self.assertRaises(FireXWaitQueryExceeded, task_query.tasks_by_name, logs_dir, 'Noop',
            #                   wait_for_exp_exist=task_uuid_complete_exp('1'), max_wait=1, error_on_wait_exceeded=True)

            # complete the task
            aggregator._on_celery_event({'uuid': '1', 'type': RunStates.FAILED.value})

            # expect result now that the task is complete, not an exception.
            tasks = task_query.tasks_by_name(logs_dir, 'Noop', wait_for_exp_exist=task_uuid_complete_exp('1'),
                                             max_wait=1, error_on_wait_exceeded=True)
            self.assertEqual(2, len(tasks))

            aggregator._on_cleanup()

            # Verify that after a run is complete, the DB is no longer writeable.
            # TODO: Enable after verifying this doesn't harm cleanup.
            # self.assertFalse(can_any_write(get_db_file(logs_dir)))

    def test_int_in_json_column(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            logs_dir = str(tmpdirname)
            _write_events_to_db(logs_dir, [{'uuid': '1', 'name': 'Noop', 'firex_result': 0}])

            task = task_query.single_task_by_name(logs_dir, 'Noop')
            self.assertEqual(0, task.firex_result)

    def test_query_running(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            logs_dir = str(tmpdirname)
            _write_events_to_db(logs_dir, [
                {'uuid': '1', 'name': 'run1', 'type': RunStates.STARTED.value},
                {'uuid': '2', 'name': 'run2', 'type': RunStates.UNBLOCKED.value},
                {'uuid': '3', 'name': 'done', 'firex_result': 0, 'type': RunStates.FAILED.value},
            ])

            tasks = task_query.running_tasks(logs_dir)
            self.assertEqual(2, len(tasks))

    def test_task_table_exists(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            db_mgr = create_db_manager(tmpdirname)
            self.assertTrue(db_mgr.task_table_exists())

            db_dir = os.path.dirname(get_db_file(tmpdirname))
            orig_db_dir_mode = os.stat(db_dir).st_mode
            # Make dir not writeable
            os.chmod(db_dir, orig_db_dir_mode & ~stat.S_IWUSR)

            # expect dotfile locking to fail
            self.assertRaises(OperationalError, db_mgr.task_table_exists)

            # Restore write permissions.
            os.chmod(db_dir, orig_db_dir_mode)
            db_mgr.close()

            # expect keeper complete to cause no-locking DB access,
            # so reads should work despite the dir being non-writeable.
            Path(get_keeper_complete_file_path(tmpdirname)).touch()

            # re-disable-write.
            os.chmod(db_dir, orig_db_dir_mode & ~stat.S_IWUSR)
            with get_db_manager(tmpdirname) as db_mgr:
                self.assertTrue(db_mgr.task_table_exists())

    def test_task_table_not_exists(self):
        from firex_keeper.persist import _db_connection_str, FireXRunDbManager
        from sqlalchemy import create_engine

        with tempfile.NamedTemporaryFile() as db_file:
            engine = create_engine(_db_connection_str(db_file.name, read_only=False))
            db_manager = FireXRunDbManager(engine.connect())
            self.assertFalse(db_manager.task_table_exists())
