# -*- coding: utf-8 -*-

import hashlib
import logging
import types

from elasticsearch import Elasticsearch, helpers
from six import string_types


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


class ElasticSearchItemPipeline(ElasticSearchPipeline):

    def __init__(self, settings) -> None:
        super().__init__(settings)
        self.items_buffer = []

    @classmethod
    def from_crawler(cls, crawler):
        for required_setting in ('ELASTICSEARCH_INDEX', 'ELASTICSEARCH_TYPE'):
            if not crawler.settings[required_setting]:
                raise ValueError('%s is not defined' % required_setting)

        return cls(crawler.settings)

    def get_id(self, item):
        unique_key = self.settings['ELASTICSEARCH_UNIQ_KEY']
        if not unique_key:
            return None

        if isinstance(unique_key, list):
            item_unique_key = '-'.join(map(lambda x: str(item[x]), unique_key))
        else:
            item_unique_key = item[unique_key]

        if isinstance(item_unique_key, string_types):
            item_unique_key = item_unique_key.encode('utf-8')
        else:
            raise Exception('unique key must be str or unicode')

        return hashlib.sha1(item_unique_key).hexdigest()

    def index_item(self, item):
        index_action = {
            '_index': self.settings['ELASTICSEARCH_INDEX'],
            '_type': self.settings['ELASTICSEARCH_TYPE'],
            '_source': dict(item)
        }

        item_id = self.get_id(item)
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
        if isinstance(item, types.GeneratorType) or isinstance(item, list):
            for each in item:
                self.process_item(each, spider)
        else:
            self.index_item(item)
            logging.debug('Item sent to Elastic Search %s' % self.settings['ELASTICSEARCH_INDEX'])
            return item

    def close_spider(self, spider):
        if len(self.items_buffer):
            self.send_items()
