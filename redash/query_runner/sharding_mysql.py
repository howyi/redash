import logging
import os

from redash.query_runner import *
from redash.utils import json_dumps, json_loads
from redash.settings import parse_boolean
from redash.query_runner.mysql import Mysql

logger = logging.getLogger(__name__)
types_map = {
    0: TYPE_FLOAT,
    1: TYPE_INTEGER,
    2: TYPE_INTEGER,
    3: TYPE_INTEGER,
    4: TYPE_FLOAT,
    5: TYPE_FLOAT,
    7: TYPE_DATETIME,
    8: TYPE_INTEGER,
    9: TYPE_INTEGER,
    10: TYPE_DATE,
    12: TYPE_DATETIME,
    15: TYPE_STRING,
    16: TYPE_INTEGER,
    246: TYPE_FLOAT,
    253: TYPE_STRING,
    254: TYPE_STRING,
}


class ShardingMysql(Mysql):
    @classmethod
    def name(cls):
        return "MySQL (Sharding)"

    @classmethod
    def type(cls):
        return 'sharding_mysql'

    @classmethod
    def enabled(cls):
        return True

    @classmethod
    def annotate_query(cls):
        return True

    @classmethod
    def configuration_schema(cls):
        show_ssl_settings = parse_boolean(os.environ.get('MYSQL_SHOW_SSL_SETTINGS', 'true'))

        schema = {
            'type': 'object',
            'properties': {
                'params': {
                    'type': 'string',
                    'default': 'shard1, shard2, shard3',
                    'title': 'Shard Parameter (Replace on {param})'
                },
                'show_params': {
                    'type': 'boolean',
                    'title': 'Show parameter in results.'
                },
                'host': {
                    'type': 'string',
                    'default': '127.0.0.1'
                },
                'user': {
                    'type': 'string'
                },
                'passwd': {
                    'type': 'string',
                    'title': 'Password'
                },
                'db': {
                    'type': 'string',
                    'title': 'Database name',
                    'default': 'test_{param}'
                },
                'port': {
                    'type': 'string',
                    'default': '3306',
                }
            },
            "order": ['params', 'show_params', 'host', 'port', 'user', 'passwd', 'db'],
            'required': ['db'],
            'secret': ['passwd']
        }

        if show_ssl_settings:
            schema['properties'].update({
                'use_ssl': {
                    'type': 'boolean',
                    'title': 'Use SSL'
                },
                'ssl_cacert': {
                    'type': 'string',
                    'title': 'Path to CA certificate file to verify peer against (SSL)'
                },
                'ssl_cert': {
                    'type': 'string',
                    'title': 'Path to client certificate file (SSL)'
                },
                'ssl_key': {
                    'type': 'string',
                    'title': 'Path to private key file (SSL)'
                }
            })

        return schema

    def run_query(self, query, user):
        import MySQLdb

        params = self.configuration['params'].split(',')

        all_columns = []
        all_rows = []

        for param in params:
            param = param.strip()
            connection = None
            try:
                config = {
                    'host': self.configuration.get('host', ''),
                    'db': self.configuration['db'],
                    'port': self.configuration.get('port', '3306'),
                    'user': self.configuration.get('user', ''),
                    'passwd': self.configuration.get('passwd', '')
                }

                for key in config:
                    if key == 'port':
                        config[key] = int(config[key].replace('{param}', param))
                    else:
                        config[key] = config[key].replace('{param}', param)

                logger.info(config)

                connection = MySQLdb.connect(host=config['host'],
                                             user=config['user'],
                                             passwd=config['passwd'],
                                             db=config['db'],
                                             port=config['port'],
                                             charset='utf8', use_unicode=True,
                                             ssl=self._get_ssl_parameters(),
                                             connect_timeout=60)
                cursor = connection.cursor()
                logger.debug("MySQL running query: %s", query)
                cursor.execute(query)

                sharding_data = cursor.fetchall()

                while cursor.nextset():
                    sharding_data = cursor.fetchall()

                if cursor.description is not None:
                    columns = self.fetch_columns([(i[0], types_map.get(i[1], None)) for i in cursor.description])
                    rows = [dict(zip((c['name'] for c in columns), row)) for row in sharding_data]

                    sharding_data = {'columns': columns, 'rows': rows}
                    sharding_json_data = json_dumps(sharding_data)
                    error = None
                else:
                    sharding_json_data = None
                    error = "No data was returned."

                cursor.close()
            except MySQLdb.Error as e:
                sharding_json_data = None
                error = e.args[1]
            except KeyboardInterrupt:
                cursor.close()
                error = "Query cancelled by user."
                sharding_json_data = None
            finally:
                if connection:
                    connection.close()

            sharding_data = json_loads(sharding_json_data)

            all_columns = sharding_data['columns']
            if self.configuration.get('show_params'):
                all_columns.insert(0, {"type": "string", "friendly_name": "database", "name": "database"})

            for row in sharding_data['rows']:
                if self.configuration.get('show_params'):
                    row['database'] = param
                all_rows.append(row)

        data = {'columns': all_columns, 'rows': all_rows}
        json_data = json_dumps(data)

        return json_data, error


register(ShardingMysql)
