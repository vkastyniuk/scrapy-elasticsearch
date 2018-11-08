# -*- coding: utf-8 -*-

import hashlib
import logging

from elasticsearch import Elasticsearch, helpers
from scrapy.exceptions import DropItem


class ElasticSearchPipeline(object):

    def __init__(self, settings) -> None:
        super().__init__()

        es_timeout = settings.get('ELASTICSEARCH_TIMEOUT', 60)
        es_servers = settings.get('ELASTICSEARCH_SERVERS', 'localhost:9200')
        es_servers = es_servers if isinstance(es_servers, list) else [es_servers]

        es_settings = dict()
        es_settings['hosts'] = es_servers
        es_settings['timeout'] = es_timeout

        if 'ELASTICSEARCH_USERNAME' in settings and 'ELASTICSEARCH_PASSWORD' in settings:
            es_settings['http_auth'] = (
                settings['ELASTICSEARCH_USERNAME'],
                settings['ELASTICSEARCH_PASSWORD']
            )

        if 'ELASTICSEARCH_CA' in settings:
            es_settings['port'] = 443
            es_settings['use_ssl'] = True
            es_settings['ca_certs'] = settings['ELASTICSEARCH_CA']['CA_CERT']
            es_settings['client_key'] = settings['ELASTICSEARCH_CA']['CLIENT_KEY']
            es_settings['client_cert'] = settings['ELASTICSEARCH_CA']['CLIENT_CERT']

        self.settings = settings
        self.es = Elasticsearch(**es_settings)


class ItemIdMixin(object):

    def __init__(self, settings) -> None:
        self.settings = settings

    def get_item_id(self, item):
        unique_key = self.settings['ELASTICSEARCH_UNIQ_KEY']
        if not unique_key:
            return None

        if isinstance(unique_key, list):
            item_unique_key = '-'.join(map(lambda x: str(item[x]), unique_key))
        else:
            item_unique_key = str(item[unique_key])

        return hashlib.sha1(item_unique_key.encode('utf-8')).hexdigest()


class ElasticSearchItemPipeline(ElasticSearchPipeline, ItemIdMixin):

    def __init__(self, settings) -> None:
        ElasticSearchPipeline.__init__(self, settings)
        ItemIdMixin.__init__(self, settings)

        self.items_buffer = []

    @classmethod
    def from_crawler(cls, crawler):
        for required_setting in ('ELASTICSEARCH_INDEX', 'ELASTICSEARCH_TYPE'):
            if not crawler.settings[required_setting]:
                raise ValueError('%s is not defined' % required_setting)

        return cls(crawler.settings)

    def index_item(self, item):
        index_action = {
            '_index': self.settings['ELASTICSEARCH_INDEX'],
            '_type': self.settings['ELASTICSEARCH_TYPE'],
            '_source': dict(item)
        }

        item_id = self.get_item_id(item)
        if item_id:
            index_action['_id'] = item_id
            logging.debug('Generated unique key %s' % item_id)

        self.items_buffer.append(index_action)
        if len(self.items_buffer) >= self.settings.get('ELASTICSEARCH_BUFFER_LENGTH', 500):
            self.send_items()

    def send_items(self):
        helpers.bulk(self.es, self.items_buffer)
        self.items_buffer = []

    def process_item(self, item, spider):
        self.index_item(item)
        logging.debug('Item sent to Elastic Search %s' % self.settings['ELASTICSEARCH_INDEX'])
        return item

    def close_spider(self, spider):
        if len(self.items_buffer):
            self.send_items()


class ElasticSearchFilterPipeline(ElasticSearchPipeline, ItemIdMixin):

    def __init__(self, settings) -> None:
        ElasticSearchPipeline.__init__(self, settings)
        ItemIdMixin.__init__(self, settings)

    @classmethod
    def from_crawler(cls, crawler):
        for required_setting in ('ELASTICSEARCH_INDEX', 'ELASTICSEARCH_TYPE', 'ELASTICSEARCH_UNIQ_KEY'):
            if not crawler.settings[required_setting]:
                raise ValueError('%s is not defined' % required_setting)

        return cls(crawler.settings)

    def process_item(self, item, spider):
        item_id = self.get_item_id(item)
        if item_id and self.es.exists(
                self.settings['ELASTICSEARCH_INDEX'],
                self.settings['ELASTICSEARCH_TYPE'],
                item_id):
            raise DropItem("Item already exists: %s" % item)

        return item
