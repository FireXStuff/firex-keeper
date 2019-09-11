import logging
import os
from typing import List

from sqlalchemy import create_engine
from sqlalchemy.sql import select

from firexapp.events.model import FireXTask
from firex_keeper.db_model import metadata, RunMetadata, firex_run_metadata, firex_tasks

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

    def insert_run_metadata(self, run_metadata: RunMetadata):
        self.db_conn.execute(firex_run_metadata.insert().values(
            firex_id=run_metadata.firex_id,
            logs_dir=run_metadata.logs_dir,
            chain=run_metadata.chain,
            # Root UUID is not available during initialization. Populated by first task event from celery.
            root_uuid=None,
        ))

    def insert_task(self, task):
        self.db_conn.execute(firex_tasks.insert().values(**task))

    def set_root_uuid(self, root_uuid):
        self.db_conn.execute(firex_run_metadata.update().values(root_uuid=root_uuid))

    def update_task(self, uuid, task):
        logger.info("Updating: %s" % task)
        self.db_conn.execute(firex_tasks.update().where(firex_tasks.c.uuid == uuid).values(**task))

    def query_tasks(self, exp) -> List[FireXTask]:
        result = self.db_conn.execute(select([firex_tasks]).where(exp))
        return [FireXTask(*row) for row in result]

    def close(self):
        self.db_conn.close()
