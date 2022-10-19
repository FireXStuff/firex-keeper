import unittest
import tempfile
from time import sleep
from multiprocessing import Process, Queue
import os

from firexkit.result import ChainInterruptedException
from firexapp.events.model import RunStates, FireXRunMetadata
from firexapp.common import wait_until
from firex_keeper.keeper_event_consumer import KeeperThreadedEventWriter
from firex_keeper.persist import (
    task_by_uuid_exp, task_uuid_complete_exp, FireXWaitQueryExceeded, create_db_manager,
    get_db_file, get_db_file_path)
from firex_keeper import task_query
from firex_keeper.keeper_helper import can_any_write, remove_write_permissions
from firex_keeper.db_model import firex_tasks


def __write_events(logs_dir, events):
    event_writer = KeeperThreadedEventWriter(
        FireXRunMetadata('1', logs_dir, 'Noop', None),
    )

    for e in events:
        event_writer.queue_celery_event(e)

    return event_writer


def _write_and_wait_for_stop(logs_dir, events, q):
    event_writer = __write_events(logs_dir, events)

    while True:
        maybe_event = q.get() # block till something on the queue
        if maybe_event is None:
            event_writer.stop()
            break
        else:
            event_writer.queue_celery_event(maybe_event)


class ProcessedKeeperWriter:
    def __init__(self, proc, q):
        self.p = proc
        self.q = q

    def stop(self):
        self.q.put(None)
        self.p.join()

    def queue_event(self, event):
        self.q.put(event)


def _write_events_to_db(logs_dir, events, stop=True):
    if stop:
        writer = __write_events(logs_dir, events)
        writer.stop()
        return None
    else:
        # can't have writing/querying in same process from different threads,
        # so run data writing in seperate process for tests that want to query
        # "in-progress" keeper DBs.
        q = Queue()
        p = Process(target=_write_and_wait_for_stop, args=(logs_dir, events, q))
        p.start()
        wait_until(
            lambda: os.path.isfile(get_db_file_path(logs_dir)),
            timeout=5,
            sleep_for=0.1)
        return ProcessedKeeperWriter(p, q)


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

            stopper = _write_events_to_db(
                logs_dir,
                [
                    {'uuid': '1', 'name': 'Noop', 'type': RunStates.STARTED.value},
                    {'uuid': '2', 'name': 'Noop'},
                ],
                stop=False)
            self.assertTrue(can_any_write(get_db_file(logs_dir)))

            # Make sure that task 1 is not yet complete
            self.assertRaises(FireXWaitQueryExceeded, task_query.tasks_by_name, logs_dir, 'Noop',
                              wait_for_exp_exist=task_uuid_complete_exp('1'), max_wait=1, error_on_wait_exceeded=True)

            stopper.stop()

            # expect result now that the task is complete, not an exception.
            tasks = task_query.tasks_by_name(logs_dir, 'Noop', wait_for_exp_exist=task_uuid_complete_exp('1'),
                                             max_wait=3, error_on_wait_exceeded=True)
            self.assertEqual(2, len(tasks))

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
            stopper = _write_events_to_db(
                logs_dir,
                [
                    {'uuid': '1', 'name': 'run1', 'type': RunStates.STARTED.value},
                    {'uuid': '2', 'name': 'run2', 'type': RunStates.UNBLOCKED.value},
                    {'uuid': '3', 'name': 'done', 'firex_result': 0, 'type': RunStates.FAILED.value},
                ],
                stop=False,
            )

            tasks = task_query.running_tasks(
                logs_dir, wait_for_exp_exist=task_by_uuid_exp('3'), max_wait=2, error_on_wait_exceeded=True)
            self.assertEqual(2, len(tasks))

            stopper.stop()

            # Stopping keeper sets all incomplete tasks to a non-celery
            # terminal run state.
            tasks = task_query.running_tasks(logs_dir)
            self.assertEqual(0, len(tasks))

    def test_task_table_exists(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            db_mgr = create_db_manager(tmpdirname)
            self.assertTrue(db_mgr.task_table_exists())

            db_file = get_db_file(tmpdirname)
            # Make file not writeable
            remove_write_permissions(db_file)
            self.assertTrue(db_mgr.task_table_exists())

    def test_query_by_failed_by(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            logs_dir = str(tmpdirname)
            _write_events_to_db(logs_dir, [
                {'uuid': '1', 'name': 'Noop',},
                {'uuid': '2', 'name': 'FailedByChild', 'exception_cause_uuid': '3'},
                {'uuid': '3', 'name': 'Fail', 'type': RunStates.FAILED.value},
            ])

            tasks = task_query.failed_by_tasks(logs_dir, '3')
            self.assertEqual(1, len(tasks))
            self.assertEqual('FailedByChild', tasks[0].name)
            self.assertEqual('2', tasks[0].uuid)

    def test_event_after_task_completed(self):
        """
            Receiving an event for a completed task is a special case
            in the aggregator since the task record needs to be read
            from the DB. This is because completed tasks are removed
            from memory to keep memory use lower.
        """
        with tempfile.TemporaryDirectory() as tmpdirname:
            logs_dir = str(tmpdirname)

            writer_proc = _write_events_to_db(
                logs_dir,
                [
                    {'uuid': '1', 'name': 'Noop', 'type': RunStates.FAILED.value},
                ],
                stop=False)

            # Make sure that task 1 is not yet complete
            tasks = task_query.tasks_by_name(
                logs_dir, 'Noop',
                wait_for_exp_exist=task_by_uuid_exp('1'),
                max_wait=2,
                error_on_wait_exceeded=True)
            self.assertEqual(1, len(tasks))

            writer_proc.queue_event(
                {'uuid': '1', 'type': RunStates.STARTED.value},
            )

            tasks = task_query.tasks_by_name(
                logs_dir, 'Noop',
                wait_for_exp_exist=firex_tasks.c.state == RunStates.STARTED.value,
                max_wait=2,
                error_on_wait_exceeded=True)
            self.assertEqual(1, len(tasks))
            self.assertEqual(RunStates.STARTED.value, tasks[0].state)

            writer_proc.stop()

            # expect result now that the task is complete, not an exception.
            tasks = task_query.tasks_by_name(logs_dir, 'Noop')
            self.assertEqual(1, len(tasks))

    def test_task_table_not_exists(self):
        from firex_keeper.persist import _db_connection_str, FireXRunDbManager
        from sqlalchemy import create_engine

        with tempfile.NamedTemporaryFile() as db_file:
            engine = create_engine(_db_connection_str(db_file.name, read_only=False))
            db_manager = FireXRunDbManager(engine.connect())
            self.assertFalse(db_manager.task_table_exists())
