from collections import namedtuple
import logging

from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, Boolean, Float, Text
from sqlalchemy.types import JSON

from firexapp.events.model import RunMetadataColumn, TaskColumn

logger = logging.getLogger(__name__)


UUID_LEN = 37


RunMetadata = namedtuple('RunMetadata', [RunMetadataColumn.FIREX_ID.value,
                                         RunMetadataColumn.LOGS_DIR.value,
                                         RunMetadataColumn.CHAIN.value])


metadata = MetaData()
firex_run_metadata = Table(
    'firex_run_metadata',
    metadata,
    Column(RunMetadataColumn.FIREX_ID.value, String(50), primary_key=True, nullable=False),
    Column(RunMetadataColumn.LOGS_DIR.value, String(300), nullable=False),
    Column(RunMetadataColumn.CHAIN.value, String(70), nullable=False),
    Column(RunMetadataColumn.ROOT_UUID.value, String(UUID_LEN), nullable=True),
)


COLS_TO_SQLALCHEMY_CONFIG = {
    # TODO: consider making uuid an integer, converted before & after write.
    TaskColumn.UUID: {'kwargs': {'type_': String(UUID_LEN), 'primary_key': True}},
    TaskColumn.FIREX_ID: {'kwargs': {'type_': None}, 'args': [ForeignKey('firex_run_metadata.firex_id')]},
    TaskColumn.CHAIN_DEPTH: {'kwargs': {'type_': Integer}},
    TaskColumn.BOUND_ARGS: {'kwargs': {'type_': JSON, 'default': None}},
    TaskColumn.RESULTS: {'kwargs': {'type_': JSON, 'default': None}},
    TaskColumn.DEFAULT_BOUND_ARGS: {'kwargs': {'type_': JSON, 'default': None}},
    TaskColumn.FROM_PLUGIN: {'kwargs': {'type_': Boolean, 'default': None}},
    TaskColumn.HOSTNAME: {'kwargs': {'type_': String(40), 'default': None}},
    TaskColumn.LOGS_URL: {'kwargs': {'type_': String(200), 'default': None}},
    TaskColumn.LONG_NAME: {'kwargs': {'type_': String(100)}},
    TaskColumn.NAME: {'kwargs': {'type_': String(30)}},
    TaskColumn.ACTUAL_RUNTIME: {'kwargs': {'type_': Float, 'default': None}},
    TaskColumn.FIRST_STARTED: {'kwargs': {'type_': Float, 'default': None}},
    TaskColumn.PARENT_ID: {'kwargs': {'type_': String(UUID_LEN)}},
    TaskColumn.RETRIES: {'kwargs': {'type_': Integer, 'default': None}},
    TaskColumn.STATE: {'kwargs': {'type_': String(15), 'default': None}},
    TaskColumn.TASK_NUM: {'kwargs': {'type_': Integer, 'default': None}},
    TaskColumn.UTCOFFSET: {'kwargs': {'type_': Integer, 'default': None}},
    TaskColumn.EXCEPTION: {'kwargs': {'type_': String(200), 'default': None}},
    TaskColumn.TRACEBACK: {'kwargs': {'type_': Text, 'default': None}},
}

# Note SQL columns must be in same order as FireXTask namedtuple fields. The TaskColumn enum is the
# authority on column/field order.
COLUMNS = [Column(tc.value, *COLS_TO_SQLALCHEMY_CONFIG[tc].get('args', []), **COLS_TO_SQLALCHEMY_CONFIG[tc]['kwargs'])
           for tc in TaskColumn]

firex_tasks = Table(
    'firex_tasks', metadata,
    *COLUMNS,
)

