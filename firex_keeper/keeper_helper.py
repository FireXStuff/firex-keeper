"""
    Utility functions for the firex_keeper package.
"""
from collections import namedtuple
import gzip
import json
import os

from firexapp.submit.uid import Uid
from firexapp.events.event_aggregator import FireXEventAggregator
from firexapp.events.model import FireXTask


FireXTreeTask = namedtuple('FireXTreeTask', FireXTask._fields + ('children', 'parent'))


def get_keeper_dir(logs_dir):
    return os.path.join(logs_dir, Uid.debug_dirname, 'keeper')


def load_event_file(db_manager, event_file):
    event_aggregator = FireXEventAggregator()

    real_rec = os.path.realpath(event_file)
    if real_rec.endswith('.gz'):
        with gzip.open(real_rec, 'rt', encoding='utf-8') as rec:
            event_lines = rec.readlines()
    else:
        with open(event_file) as rec:
            event_lines = rec.readlines()

    for event_line in event_lines:
        if not event_line:
            continue
        event = json.loads(event_line)
        new_task_data_by_uuid = event_aggregator.aggregate_events([event])
        db_manager.insert_or_update_tasks(new_task_data_by_uuid,
                                          event_aggregator.root_uuid)
