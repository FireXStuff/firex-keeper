import logging
import os
from typing import List

from sqlalchemy import create_engine
from sqlalchemy.sql import select

from firexapp.events.model import FireXTask, FireXRunMetadata, get_task_data
from firex_keeper.db_model import metadata, firex_run_metadata, firex_tasks

logger = logging.getLogger(__name__)


def connect_db(db_file):
    create_schema = not os.path.exists(db_file)
    logger.info("Creating engine for %s" % db_file)
    engine = create_engine('sqlite:///' + db_file)
    if create_schema:
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


def get_db_manager(logs_dir):
    return FireXRunDbManager(connect_db(get_db_file_path(logs_dir, new=False)))


class FireXRunDbManager:

    def __init__(self, db_conn):
        self.db_conn = db_conn

    def insert_run_metadata(self, run_metadata: FireXRunMetadata) -> None:
        self.db_conn.execute(firex_run_metadata.insert().values(**run_metadata._asdict()))

    def _set_root_uuid(self, root_uuid) -> None:
        self.db_conn.execute(firex_run_metadata.update().values(root_uuid=root_uuid))

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

    def query_tasks(self, exp) -> List[FireXTask]:
        result = self.db_conn.execute(select([firex_tasks]).where(exp))
        return [FireXTask(*row) for row in result]

    def query_run_metadata(self, firex_id) -> List[FireXRunMetadata]:
        result = self.db_conn.execute(select([firex_run_metadata]).where(firex_run_metadata.c.firex_id == firex_id))
        if not result:
            raise Exception("Found no run data for %s" % firex_id)
        return [FireXRunMetadata(*row) for row in result][0]

    def query_single_run_metadata(self) -> FireXRunMetadata:
        result = self.db_conn.execute(select([firex_run_metadata]))
        metadatas = [FireXRunMetadata(*row) for row in result]
        if len(metadatas) != 1:
            raise Exception("Expected exactly one firex_run_metadata, but found %d" % len(metadatas))
        return metadatas[0]

    def close(self):
        self.db_conn.close()
