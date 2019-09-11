"""
Process events from Celery.
"""

import logging

from firexapp.events.broker_event_consumer import BrokerEventConsumerThread
from firexapp.events.event_aggregator import FireXEventAggregator
from firexapp.events.model import RunMetadata, get_task_data

from firex_keeper.persist import create_db_manager


logger = logging.getLogger(__name__)


class TaskDatabaseAggregatorThread(BrokerEventConsumerThread):
    """"""

    def __init__(self, celery_app, run_metadata: RunMetadata, max_retry_attempts: int = None,
                 receiver_ready_file: str = None):
        super().__init__(celery_app, max_retry_attempts, receiver_ready_file)
        # TODO: always aggregating events isn't necessary, could clear events once tasks are complete.
        self.event_aggregator = FireXEventAggregator()
        self.run_db_manager = create_db_manager(run_metadata.logs_dir)
        self.run_db_manager.insert_run_metadata(run_metadata)

    def _is_root_complete(self):
        return self.event_aggregator.is_root_complete()

    def _on_celery_event(self, event):
        new_task_data_by_uuid = self.event_aggregator.aggregate_events([event])
        for uuid, new_task_data in new_task_data_by_uuid.items():
            persisted_keys_new_task_data = get_task_data(new_task_data)
            if persisted_keys_new_task_data:
                # The UUID is only changed for the very first event for that UUID, by definition.
                is_uuid_new = 'uuid' in persisted_keys_new_task_data
                if is_uuid_new:
                    self.run_db_manager.insert_task(persisted_keys_new_task_data)
                    if uuid == self.event_aggregator.root_uuid:
                        self.run_db_manager.set_root_uuid(uuid)
                else:
                    # The UUID hasn't changed, but it's still needed to do the update since it's the task primary key.
                    self.run_db_manager.update_task(uuid, persisted_keys_new_task_data)

    def _on_cleanup(self):
        self.run_db_manager.close()
