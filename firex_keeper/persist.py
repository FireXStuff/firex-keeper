import logging
import os
from typing import List
from contextlib import contextmanager
from time import perf_counter

from sqlalchemy import create_engine
from sqlalchemy.sql import select

from celery import current_task
from firexapp.events.model import FireXTask, FireXRunMetadata, get_task_data
from firexapp.common import wait_until
from firex_keeper.db_model import metadata, firex_run_metadata, firex_tasks

logger = logging.getLogger(__name__)


WAIT_FOR_CURRENT_UUID = 'use global current uuid'


def connect_db(db_file):
    create_schema = not os.path.exists(db_file)
    engine = create_engine('sqlite:///' + db_file)
    if create_schema:
        logger.info("Creating schema for %s" % db_file)
        metadata.create_all(engine)
    return engine.connect()


def get_db_file_path(logs_dir, new=False):
    parent = os.path.join(logs_dir, 'debug', 'keeper')
    db_file = os.path.join(parent, 'firex_run.db')
    if new:
        assert not os.path.exists(db_file), "Cannot create new DB file, it already exists: %s" % db_file
        os.makedirs(parent, exist_ok=True)
    else:
        assert os.path.exists(db_file), "DB file does not exist: %s" % db_file
    return db_file


def create_db_manager(logs_dir):
    return FireXRunDbManager(connect_db(get_db_file_path(logs_dir, new=True)))


def wait_before_query(query_task_by_uuid_fn, max_wait, wait_for_uuid, error_on_wait_exceeded):
    start_wait_time = perf_counter()
    # TODO: could generalize this function to also wait on entire keeper process being complete.
    if wait_for_uuid == WAIT_FOR_CURRENT_UUID:
        wait_for_uuid = current_task.request.id

    uuid_exists = wait_until(query_task_by_uuid_fn, max_wait, 1, wait_for_uuid)
    wait_duration = perf_counter() - start_wait_time
    logger.debug("Keeper query waited %.2f secs for task %s to exist." % (wait_duration, wait_for_uuid))
    if not uuid_exists:
        msg = "Waited %d seconds for task %s, but it still doesn't exist." % (max_wait, wait_for_uuid)
        if error_on_wait_exceeded:
            raise Exception(msg)
        else:
            logger.warning(msg)


@contextmanager
def get_db_manager(logs_dir):
    db_manager = FireXRunDbManager(connect_db(get_db_file_path(logs_dir, new=False)))
    try:
        yield db_manager
    finally:
        db_manager.close()


def _row_to_run_metadata(row):
    # The first 4 columns from the table make up a FireXRunMetadata.
    return FireXRunMetadata(*row[:4])


class FireXRunDbManager:

    def __init__(self, db_conn):
        self.db_conn = db_conn

    def insert_run_metadata(self, run_metadata: FireXRunMetadata) -> None:
        self.db_conn.execute(firex_run_metadata.insert().values(**run_metadata._asdict()))

    def _set_root_uuid(self, root_uuid) -> None:
        self.db_conn.execute(firex_run_metadata.update().values(root_uuid=root_uuid))

    def set_keeper_complete(self):
        self.db_conn.execute(firex_run_metadata.update().values(keeper_complete=True))

    def insert_or_update_tasks(self, new_task_data_by_uuid, root_uuid):
        for uuid, new_task_data in new_task_data_by_uuid.items():
            persisted_keys_new_task_data = get_task_data(new_task_data)
            if persisted_keys_new_task_data:
                # The UUID is only changed for the very first event for that UUID, by definition.
                is_uuid_new = 'uuid' in persisted_keys_new_task_data
                if is_uuid_new:
                    self._insert_task(persisted_keys_new_task_data)
                    if uuid == root_uuid:
                        self._set_root_uuid(uuid)
                else:
                    # The UUID hasn't changed, but it's still needed to do the update since it's the task primary key.
                    self._update_task(uuid, persisted_keys_new_task_data)

    def _insert_task(self, task) -> None:
        self.db_conn.execute(firex_tasks.insert().values(**task))

    def _update_task(self, uuid, task) -> None:
        self.db_conn.execute(firex_tasks.update().where(firex_tasks.c.uuid == uuid).values(**task))

    def does_task_uuid_exist(self, task_uuid):
        query = select([firex_tasks.c.uuid]).where(firex_tasks.c.uuid == task_uuid)
        return self.db_conn.execute(query).scalar() is not None

    def query_tasks(self, exp, wait_for_uuid=None, max_wait=15, error_on_wait_exceeded=False) -> List[FireXTask]:
        if wait_for_uuid:
            wait_before_query(self.does_task_uuid_exist, max_wait, wait_for_uuid, error_on_wait_exceeded)

        result = self.db_conn.execute(select([firex_tasks]).where(exp))
        return [FireXTask(*row) for row in result]

    def query_run_metadata(self, firex_id) -> List[FireXRunMetadata]:
        result = self.db_conn.execute(select([firex_run_metadata]).where(firex_run_metadata.c.firex_id == firex_id))
        if not result:
            raise Exception("Found no run data for %s" % firex_id)
        # The first 4 columns from the table make up a FireXRunMetadata.
        return [_row_to_run_metadata(row) for row in result][0]

    def _query_single_run_metadata_row(self):
        result = self.db_conn.execute(select([firex_run_metadata]))
        rows = [r for r in result]
        if len(rows) != 1:
            raise Exception("Expected exactly one firex_run_metadata, but found %d" % len(rows))
        return rows[0]

    def is_keeper_complete(self) -> bool:
        return self._query_single_run_metadata_row()['keeper_complete']

    def query_single_run_metadata(self) -> FireXRunMetadata:
        return _row_to_run_metadata(self._query_single_run_metadata_row())

    def close(self):
        self.db_conn.close()
