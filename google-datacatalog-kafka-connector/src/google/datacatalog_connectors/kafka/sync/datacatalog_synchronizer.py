import logging
import uuid

from confluent_kafka import Consumer

from google.datacatalog_connectors.commons.cleanup \
    import datacatalog_metadata_cleaner
from google.datacatalog_connectors.commons.ingest \
    import datacatalog_metadata_ingestor
from google.datacatalog_connectors.commons.monitoring \
    import metrics_processor
from google.datacatalog_connectors.kafka import prepare


class DataCatalogSynchronizer:
    """Orchestrate the Scrape/Prepare/Ingest steps."""

    def __init__(self,
                 project_id,
                 location_id,
                 entry_group_id,
                 kafka_host,
                 connection_config,
                 metadata_scraper,
                 enable_monitoring=None):
        self.__entry_group_id = entry_group_id
        self.__metadata_scraper = metadata_scraper
        self.__project_id = project_id
        self.__location_id = location_id
        self.__kafka_host = kafka_host
        self.__connection_config = connection_config
        self.__task_id = uuid.uuid4().hex[:8]
        self.__metrics_processor = metrics_processor.MetricsProcessor(
            project_id, location_id, entry_group_id, enable_monitoring,
            self.__task_id)

    def run(self):
        """Runs the Scrape, Prepare and Ingest modules
        :return: task_id
        """
        self._before_run()
        logging.info('\n\n==============Scrape metadata===============')

        consumer = self._create_consumer()
        metadata = self.__metadata_scraper(consumer).get_metadata()

        self._log_metadata(metadata)

        logging.info('\n\n==============Prepare metadata===============')

        tag_templates = self.__create_tag_templates()
        prepared_entries = self.__prepare_datacatalog_entries(metadata)

        self._log_entries(prepared_entries)

        logging.info('\n==============Ingest metadata===============')

        self.__delete_obsolete_metadata(prepared_entries)

        self.__ingest_metadata(prepared_entries, tag_templates)

        logging.info('\n============End %s-to-datacatalog============',
                     self.__entry_group_id)
        self._after_run()

        return self.__task_id

    def _create_consumer(self):
        consumer = Consumer(self.__connection_config)
        return consumer

    def __prepare_datacatalog_entries(self, metadata):
        entry_factory = self.__create_assembled_entry_factory()
        prepared_entries = entry_factory. \
            make_entries_from_cluster_metadata(
                metadata)
        return prepared_entries

    def __delete_obsolete_metadata(self, prepared_entries):
        # Since we can't rely on search returning the ingested entries,
        # we clean up the obsolete entries before ingesting.
        cleaner = datacatalog_metadata_cleaner.DataCatalogMetadataCleaner(
            self.__project_id, self.__location_id, self.__entry_group_id)
        cleaner.delete_obsolete_metadata(
            prepared_entries, 'system={}'.format(self.__entry_group_id))

    def __ingest_metadata(self, prepared_entries, tag_templates_dict):
        logging.info('\nStarting to ingest custom metadata...')
        ingestor = datacatalog_metadata_ingestor.DataCatalogMetadataIngestor(
            self.__project_id, self.__location_id, self.__entry_group_id)
        ingestor.ingest_metadata(prepared_entries,
                                 tag_templates_dict=tag_templates_dict)

    def __create_tag_templates(self):
        tag_template_factory = self._get_tag_template_factory()(
            self.__project_id, self.__location_id, self.__entry_group_id)

        cluster_tag_template_id, cluster_tag_template = \
            tag_template_factory.create_tag_template_for_cluster_metadata()

        topic_tag_template_id, topic_tag_template = \
            tag_template_factory.create_tag_template_for_topic_metadata()

        tag_templates_dict = \
            {cluster_tag_template_id: cluster_tag_template,
             topic_tag_template_id: topic_tag_template}

        return tag_templates_dict

    # Create factories
    def __create_assembled_entry_factory(self):
        return self._get_assembled_entry_factory()(
            self.__entry_group_id, self.__create_entry_factory())

    def __create_entry_factory(self):
        return self._get_entry_factory()(self.__project_id, self.__location_id,
                                         self.__kafka_host,
                                         self.__entry_group_id)

    # Begin extension methods
    def _before_run(self):
        logging.info('\n============Start %s-to-datacatalog===========',
                     self.__entry_group_id)

    def _after_run(self):
        self.__metrics_processor.process_elapsed_time_metric()

    def _log_entries(self, prepared_entries):
        entries_len = len(prepared_entries)
        self.__metrics_processor.process_entries_length_metric(entries_len)

    def _log_metadata(self, metadata):
        self.__metrics_processor.process_metadata_payload_bytes_metric(
            metadata)
        logging.info('\n%s topics ready to be ingested...',
                     len(metadata['topics']))

    @classmethod
    def _get_assembled_entry_factory(cls):
        return prepare.assembled_entry_factory.AssembledEntryFactory

    @classmethod
    def _get_entry_factory(cls):
        return prepare.datacatalog_entry_factory.DataCatalogEntryFactory

    @classmethod
    def _get_tag_template_factory(cls):
        return prepare.datacatalog_tag_template_factory. \
            DataCatalogTagTemplateFactory
