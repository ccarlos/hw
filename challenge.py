import argparse
import csv


class UnknownDataTypeException(Exception):
    pass


class DataLoader:
    def __init__(self, table_name='schema', drop_table=False):
        self.table_name = table_name
        self.drop_table = drop_table
        self.data_type_mapping = {
            'INT': 'INTEGER',
            'INTEGER': 'INTEGER',

            'CHAR': 'TEXT',
            'VARCHAR': 'TEXT',
            'TEXT': 'TEXT',

            'BOOLEAN': 'NUMERIC'
        }

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
                data_list.append(
                    (row['field name'], row['width'],
                     self.data_type_mapping[row['datatype'].upper()]))

        return data_list

    def _create_table(self, data_list):
        table_create_placeholder = '''CREATE TABLE IF NOT EXISTS %s (%s);'''
        processed_data_list = self._process_data_type_list(data_list)
        table_create_statement = table_create_placeholder % \
                                 (self.table_name, processed_data_list)
        # run command to create table
        print(table_create_statement)

    def _process_data_type_list(self, data_list):
        """Not all databases will use the field width column"""
        return (",".join([field_name + " " + data_type
                          for field_name, _, data_type in data_list]))



def main():
    """ Process Command Line Arguments """
    parser = argparse.ArgumentParser(
        description='Create DB table and load data')
    parser.add_argument('-d', '--droptable', action='store_true',
                        help='Drop existing table')
    parser.add_argument('--tablename', help='Table name used to create schema')

    # todo:
    # parser.add_argument('--directory',
    # help='Specify directory where to look for files')

    args = parser.parse_args()

    table_name = 'schema'
    drop_table = False
    if args.tablename:
        table_name = args.tablename
    if args.droptable:
        drop_table = True

    try:
        dl = DataLoader(table_name=table_name, drop_table=drop_table)
        data_list = dl._process_schema('schema.csv')
        dl._create_table(data_list)
    except UnknownDataTypeException as e:
        print("An exception occurred: %s" % e)


main()
