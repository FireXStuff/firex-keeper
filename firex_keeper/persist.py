import json
import logging
import os
from contextlib import contextmanager
from time import perf_counter, sleep
from contextlib import contextmanager, nullcontext
from typing import Optional, Any

from firexapp.submit.uid import Uid
from sqlalchemy import create_engine, event
from sqlalchemy.sql import select, and_
from sqlalchemy.sql.selectable import Select
from sqlalchemy.exc import OperationalError
from sqlalchemy.engine import Engine, Connection
from sqlite3 import OperationalError as SqlLiteOperationalError

from firexapp.events.model import FireXTask, FireXRunMetadata, COMPLETE_RUNSTATES
from firexapp.common import wait_until
from firex_keeper.db_model import metadata, firex_run_metadata, firex_tasks
import pysqlite3

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


def _get_pragmas(use_wal):
    pragmas = [ 'page_size = 4096' ]
    if use_wal:
        pragmas += [
            'journal_mode=WAL', 'synchronous=NORMAL',
        ]
    return pragmas


def execute_pragmas(engine, use_wal=False):
    dbapi_connection = engine.raw_connection()
    try:
        cursor = dbapi_connection.cursor()
        for pragma in _get_pragmas(use_wal):
            cmd = 'PRAGMA ' + pragma
            logger.debug(f"Executing: {cmd}")
            cursor.execute(cmd)
        cursor.close()
        dbapi_connection.commit()
    finally:
        dbapi_connection.close()


def _db_connection_str(db_file, read_only, is_run_complete=False):
    db_conn_str = f'sqlite:///file:{db_file}'

    params = {'uri': 'true'}

    if is_run_complete:
        params['immutable'] = '1'
        read_only = True

    if read_only:
        params['mode'] = 'ro'

    db_conn_str += '?' + '&'.join(
        [f'{k}={v}' for k, v in params.items()]
    )

    return db_conn_str


def create_db_engine(
    db_file,
    read_only=False,
    metadata_to_create=metadata,
    is_run_complete=False,
) -> Engine:
    engine = create_engine(
        _db_connection_str(db_file, read_only, is_run_complete=is_run_complete),
        json_deserializer=_custom_json_loads,
        module=pysqlite3,
    )

    if not os.path.exists(db_file):
        #  WAL should not be used while concurrent read+write NFS access is still possible. Once all reads go through
        #   keeper process for in-progress runs, WAL is likely preferable for in-progress runs, then after the DB
        #   should be read-only and therefore safe for direct NFS access.
        execute_pragmas(engine, use_wal=False)

        logger.info(f"Creating schema for {db_file}")
        metadata_to_create.create_all(engine)
        logger.info(f"Schema creation complete for {db_file}")

    return engine


def connect_db(db_file, read_only=False, metadata_to_create=metadata, is_run_complete=False) -> Connection:
    keeper_engine = create_db_engine(
        db_file,
        read_only=read_only,
        metadata_to_create=metadata_to_create,
        is_run_complete=is_run_complete,
    )

    return keeper_engine.connect()


def get_db_file_dir_path(logs_dir: str) -> str:
    return os.path.join(logs_dir, Uid.debug_dirname, 'keeper')


def get_keeper_complete_file_path(logs_dir):
    # A way for checking if the keeper DB is complete without inspecting the DB
    # file. This can be used to inform connection decisions, like if the DB
    # is not expected to change.
    return os.path.join(get_db_file_dir_path(logs_dir), '.keeper_complete')


def is_keeper_db_complete(logs_dir):
    return os.path.isfile(get_keeper_complete_file_path(logs_dir))


def get_keeper_query_ready_file_path(logs_dir: str) -> str:
    return os.path.join(get_db_file_dir_path(logs_dir), '.keeper_query_ready')


def is_keeper_db_query_ready(logs_dir: str) -> bool:
    return os.path.isfile(
        get_keeper_query_ready_file_path(logs_dir)
    )


def get_db_file(logs_dir: str, new=False) -> str:
    db_file = os.path.join(get_db_file_dir_path(logs_dir), 'firex_run.db')
    if new:
        assert not os.path.exists(db_file), f"Cannot create new DB file, it already exists: {db_file}"
        db_file_parent = os.path.dirname(db_file)
        os.makedirs(db_file_parent, exist_ok=True)
    else:
        assert os.path.isfile(db_file), f"DB file does not exist: {db_file}"
    return db_file


@contextmanager
def get_db_manager(logs_dir):
    "Get a query-only DB manager for an existing keeper DB file."

    conn = create_db_engine(
        get_db_file(logs_dir),
        read_only=True,
        is_run_complete=is_keeper_db_complete(logs_dir),
    )
    db_manager = FireXRunDbManager(conn)
    try:
        yield db_manager
    finally:
        db_manager.close()


def _row_to_run_metadata(run_metadata_dict: dict[str, Any]) -> Any:
    # The first 4 columns from the table make up a FireXRunMetadata.
    return FireXRunMetadata(
        **{
            k: run_metadata_dict.get(k)
            for k in FireXRunMetadata._fields
        }
    )

RETRYING_DB_EXCEPTIONS = (OperationalError, SqlLiteOperationalError)
DEFAULT_MAX_RETRY_ATTEMPTS = 20

def retry(exceptions, max_attempts: int=DEFAULT_MAX_RETRY_ATTEMPTS, retry_delay: int=1):
    def retry_decorator(func):
        def retrying_wrapper(*args, **kwargs):
            attempt = 0
            while attempt < max_attempts:
                attempt += 1
                try:
                    return func(*args, **kwargs)
                except exceptions:
                    if attempt >= max_attempts:
                        raise
                    sleep(retry_delay)

        return retrying_wrapper
    return retry_decorator


class FireXRunDbManager:
    """
    Read-only operations on a keeper DB connection.
    """

    def __init__(self, db_engine):
        self.db_engine : Engine = db_engine

    def _does_task_whereclause_exist(self, whereclause):
        query = select(firex_tasks.c.uuid).where(whereclause)
        with self.execute_statement(query) as result:
            return result.scalar() is not None

    @contextmanager
    def execute_statement(self, statement, existing_tx: Optional[Connection]=None):
        if existing_tx:
            tx_ctx_mgr = nullcontext(existing_tx)
        else:
            tx_ctx_mgr = self.db_engine.begin()

        with tx_ctx_mgr as db_tx:
            yield db_tx.execute(statement)

    def wait_before_query(self, whereclause, max_wait, error_on_wait_exceeded):
        if not self._is_keeper_complete():
            start_wait_time = perf_counter()
            # important that each whereclause check is done in seperate TX
            # so future checks can observe newly written data.
            exists = wait_until(self._does_task_whereclause_exist, max_wait, 0.5, whereclause)
            if not exists:
                msg = f"Wait exceeded {max_wait} seconds for {whereclause} to exist, but it still does not."
                if error_on_wait_exceeded:
                    raise FireXWaitQueryExceeded(msg)
                else:
                    logger.warning(msg)
            else:
                wait_duration = perf_counter() - start_wait_time
                logger.debug(f"Keeper query waited {wait_duration:.2f} secs for wait query to exist.")

    def query_tasks_no_wait(self, exp, existing_tx=None) -> list[FireXTask]:

        if isinstance(exp, Select):
            select_stmt = exp
        else:
            select_stmt = select(firex_tasks).where(exp)

        result_tasks = []
        with self.execute_statement(select_stmt, existing_tx) as db_result:
            for row in db_result:
                try:
                    result_tasks.append(FireXTask(*row))
                except TypeError as e:
                    logger.error(f"Failed transforming {row[0]}")
                    logger.exception(e)
                    raise

        return result_tasks

    @retry(RETRYING_DB_EXCEPTIONS)
    def query_tasks(self, exp, wait_for_exp_exist=None, max_wait=15, error_on_wait_exceeded=False) -> list[FireXTask]:
        if wait_for_exp_exist is not None:
            self.wait_before_query(wait_for_exp_exist, max_wait, error_on_wait_exceeded)
        return self.query_tasks_no_wait(exp)

    def query_run_metadata(self, firex_id) -> FireXRunMetadata:
        metadata_dict = self._query_single_run_metadata_row(
            exp=firex_run_metadata.c.firex_id == firex_id,
        )
        return _row_to_run_metadata(metadata_dict)

    @retry(RETRYING_DB_EXCEPTIONS)
    def _query_single_run_metadata_row(self, exp=True) -> dict[str, Any]:
        query = select(firex_run_metadata).where(exp)
        with self.execute_statement(query) as result:
            maybe_single_runmetadata = result.mappings().fetchone()
            if maybe_single_runmetadata is None:
                raise Exception("No RunMetadata found.")

            return dict(maybe_single_runmetadata)

    def _is_keeper_complete(self) -> bool:
        return self._query_single_run_metadata_row()['keeper_complete']

    @retry(RETRYING_DB_EXCEPTIONS)
    def query_single_run_metadata(self) -> FireXRunMetadata:
        return _row_to_run_metadata(self._query_single_run_metadata_row())

    def close(self):
        self.db_engine.dispose()

