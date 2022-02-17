"""
Process events from Celery.
"""

import logging

from firexapp.events.broker_event_consumer import BrokerEventConsumerThread
from firexapp.events.event_aggregator import FireXEventAggregator
from firexapp.events.model import FireXRunMetadata

from firex_keeper.persist import create_db_manager


logger = logging.getLogger(__name__)


class TaskDatabaseAggregatorThread(BrokerEventConsumerThread):
    """Captures Celery events and stores the FireX datamodel in an SQLite DB."""

    def __init__(self, celery_app, run_metadata: FireXRunMetadata, max_retry_attempts: int = None,
                 receiver_ready_file: str = None):
        super().__init__(celery_app, max_retry_attempts, receiver_ready_file)
        # TODO: keeping all aggregated events in memory isn't necessary, could clear events once tasks are complete.
        self.event_aggregator = FireXEventAggregator()
        self.run_db_manager = create_db_manager(run_metadata.logs_dir)
        # Root UUID is not available during initialization. Populated by first task event from celery.
        self.run_db_manager.insert_run_metadata(run_metadata)

    def _is_root_complete(self):
        return self.event_aggregator.is_root_complete()

    def _all_tasks_complete(self):
        return self.event_aggregator.are_all_tasks_complete()

    def _on_celery_event(self, event):
        new_task_data_by_uuid = self.event_aggregator.aggregate_events([event])
        self.run_db_manager.insert_or_update_tasks(new_task_data_by_uuid,
                                                   self.event_aggregator.root_uuid)

    def _on_cleanup(self):
        for e in self.event_aggregator.generate_incomplete_events():
            self._on_celery_event(e)
        self.run_db_manager.set_keeper_complete()
        self.run_db_manager.close()
