import argparse
import csv


class UnknownDataTypeException(Exception):
    pass


class DataLoader:
    def __init__(self):
        self.data_type_mapping = {
            'INT': 'INTEGER',
            'INTEGER': 'INTEGER',

            'CHAR': 'TEXT',
            'VARCHAR': 'TEXT',
            'TEXT': 'TEXT',

            'BOOLEAN': 'NUMERIC'
        }

    def _create_table(self, table_name='schema', schema=''):
        table_create_statement = '''CREATE TABLE IF NOT EXISTS %s (%s)'''

    def _process_schema(self, filename='schema.csv'):
        # read in csv file and match it against the data_type_mapping
        data_list = []
        data_type_keys = self.data_type_mapping.keys()
        with open(filename, mode='r') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            for row in csv_reader:
                if row['datatype'].upper() not in data_type_keys:
                    raise UnknownDataTypeException(
                        "Data type not supported: %s" % row['datatype'])
                data_list.append((row['field name'], row['width'], row['datatype']))

        return data_list

    def _schema_parser(self):
        """Create a mapping of possible data types => sqlite"""
        pass

def store():
    pass


def main():
    """ Process Command Line Arguments """
    table_name = 'schema'
    parser = argparse.ArgumentParser(description='Create DB table and store data')
    parser.add_argument('-d', '--droptable', action='store_true',
                        help='Drop existing table')
    parser.add_argument('--tablename', help='Table name used to create schema')
    # parser.add_argument('--directory', help='Specify directory where to look for files')

    args = parser.parse_args()
    print(args.tablename)
    print(args.droptable)
    try:
        dl = DataLoader()
        dl._process_schema('schema.csv')
    except UnknownDataTypeException as e:
        print("An exception occurred: %s" % e)

main()
