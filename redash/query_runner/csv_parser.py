import csv as csv
import json

from redash.query_runner import BaseQueryRunner, register


class CsvParser(BaseQueryRunner):
    @classmethod
    def configuration_schema(cls):
        return {
            'type': 'object',
            'properties': {
                'delimiter': {
                    'type': 'string',
                    'title': 'Delimiter'
                }
            }
        }

    @classmethod
    def annotate_query(cls):
        return False

    def __init__(self, configuration):
        super(CsvParser, self).__init__(configuration)

    def test_connection(self):
        pass

    def run_query(self, query, user):
        data = {
            'columns': [],
            'rows': [],
        }

        delimiter = str(self.configuration.get('delimiter'))

        for row in csv.DictReader(query.strip().splitlines(), delimiter=delimiter):
            if len(data['columns']) == 0:
                for key in row.keys():
                    data['columns'].append({'name': key, 'friendly_name': key})

            data['rows'].append(row)

        return json.dumps(data), None


register(CsvParser)
