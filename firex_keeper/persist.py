import json
import logging
import os
from typing import List
from contextlib import contextmanager
from time import perf_counter

from firexapp.submit.uid import Uid
from sqlalchemy import create_engine
from sqlalchemy.sql import select, and_

from firexapp.events.model import FireXTask, FireXRunMetadata, get_task_data, COMPLETE_RUNSTATES
from firexapp.common import wait_until
from firex_keeper.db_model import metadata, firex_run_metadata, firex_tasks, TASKS_TABLENAME

logger = logging.getLogger(__name__)


class FireXWaitQueryExceeded(Exception):
    pass


def task_by_uuid_exp(task_uuid):
    return firex_tasks.c.uuid == task_uuid


def task_uuid_complete_exp(task_uuid):
    return and_(firex_tasks.c.uuid == task_uuid,
                firex_tasks.c.state.in_(COMPLETE_RUNSTATES))


def cur_task_by_uuid_exp():
    from celery import current_task
    if not current_task:
        return False
    return task_by_uuid_exp(current_task.request.id)


def _custom_json_loads(*args, **kwargs):
    # JSON columns can still store ints in sqlite, so pass these values along even though they can't be decoded as
    # JSON.
    if isinstance(args[0], int):
        return args[0]
    return json.loads(*args, **kwargs)


def set_sqlite_WAL_pragma(engine):
    dbapi_connection = engine.raw_connection()
    try:
        cursor = dbapi_connection.cursor()
        pragma_cmd = "PRAGMA journal_mode=WAL"
        cursor.execute(pragma_cmd)
        cursor.close()
        logger.info("Executed: " + pragma_cmd)
        dbapi_connection.commit()
    finally:
        dbapi_connection.close()


def _db_connection_str(db_file, read_only, dotfile_locking=True):
    db_conn_str = 'sqlite:///'
    if read_only:
        db_conn_str += 'file:'

    db_conn_str += db_file
    params = {}
    if read_only:
        params['mode'] = 'ro'
        params['uri'] = 'true'

    if dotfile_locking:
        params['vfs'] = 'unix-dotfile'

    if params:
         db_conn_str += '?' + '&'.join(
            [f'{k}={v}' for k, v in params.items()]
         )

    return db_conn_str


def connect_db(db_file, read_only=False, metadata_to_create=metadata):
    create_schema = not os.path.exists(db_file)
    engine = create_engine(
        _db_connection_str(db_file, read_only),
        json_deserializer=_custom_json_loads)
    if create_schema:
        logger.info("Creating schema for %s" % db_file)
        metadata_to_create.create_all(engine)
        #  WAL should not be used while concurrent read+write NFS access is still possible. Once all reads go through
        #   keeper process for in-progress runs, WAL is likely preferable for in-progress runs, then after the DB
        #   should be read-only and therefore safe for direct NFS access.
        # set_sqlite_WAL_pragma(engine)
        logger.info("Schema creation complete for %s" % db_file)

    return engine.connect()


def get_db_file_path(logs_dir):
    return os.path.join(logs_dir, Uid.debug_dirname, 'keeper', 'firex_run.db')


def get_db_file(logs_dir, new=False):
    db_file = get_db_file_path(logs_dir)
    if new:
        assert not os.path.exists(db_file), "Cannot create new DB file, it already exists: %s" % db_file
        db_file_parent = os.path.dirname(db_file)
        os.makedirs(db_file_parent, exist_ok=True)
    else:
        assert os.path.exists(db_file), "DB file does not exist: %s" % db_file
    return db_file


def create_db_manager(logs_dir):
    conn = connect_db(get_db_file(logs_dir, new=True), read_only=False)
    logger.info("Created DB connection.")
    return FireXRunDbManager(conn)


@contextmanager
def get_db_manager(logs_dir, read_only=False):
    db_manager = FireXRunDbManager(connect_db(get_db_file(logs_dir, new=False), read_only=read_only))
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
        with self.db_conn.begin():
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

    def does_task_whereclause_exist(self, whereclause):
        query = select([firex_tasks.c.uuid]).where(whereclause)
        return self.db_conn.execute(query).scalar() is not None

    def wait_before_query(self, whereclause, max_wait, error_on_wait_exceeded):
        if not self.is_keeper_complete():
            start_wait_time = perf_counter()
            exists = wait_until(self.does_task_whereclause_exist, max_wait, 0.5, whereclause)
            wait_duration = perf_counter() - start_wait_time
            logger.debug("Keeper query waited %.2f secs for wait query to exist." % wait_duration)
        else:
            exists = self.does_task_whereclause_exist(whereclause)

        if not exists:
            msg = f"Wait exceeded {max_wait} seconds for {whereclause} to exist, but it still does not."
            if error_on_wait_exceeded:
                raise FireXWaitQueryExceeded(msg)
            else:
                logger.warning(msg)

    def query_tasks(self, exp, wait_for_exp_exist=None, max_wait=15, error_on_wait_exceeded=False) -> List[FireXTask]:
        if wait_for_exp_exist is not None:
            self.wait_before_query(wait_for_exp_exist, max_wait, error_on_wait_exceeded)

        db_result = self.db_conn.execute(select([firex_tasks]).where(exp))
        result_tasks = []
        for row in db_result:
            try:
                result_tasks.append(FireXTask(*row))
            except Exception as e:
                logger.error(f"Failed transforming {row[0]}")
                logger.exception(e)
                raise
        return result_tasks

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

    def task_table_exists(self):
        result = self.db_conn.execute(f'SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name="{TASKS_TABLENAME}");')
        return bool([r for r in result][0][0])

    def close(self):
        self.db_conn.close()
