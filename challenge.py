import argparse
import csv
import sqlite3


class UnknownDataTypeException(Exception):
    pass


# todo: implement an abc class, derive from base class according to db.
class DataLoader:
    def __init__(self, table_name='schema', drop_table=False):
        self.table_name = table_name
        # todo: run drop table command inside _create_table if True
        self.drop_table = drop_table
        self.data_type_mapping = {
            'INT': 'INTEGER',
            'INTEGER': 'INTEGER',

            'CHAR': 'TEXT',
            'VARCHAR': 'TEXT',
            'TEXT': 'TEXT',

            'BOOLEAN': 'NUMERIC'
        }
        self.field_names = []
        self.quotation_fields = []
        self.needs_quotes = ['TEXT']

        # database connection details
        conn = sqlite3.connect(table_name + ".db")
        self.c = conn.cursor()

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
                self.field_names.append(row['field name'])
                if self.data_type_mapping[row['datatype'].upper()] in \
                        self.needs_quotes:
                    self.quotation_fields.append(row['field name'])

        return data_list

    def _create_table(self, data_list):
        table_create_placeholder = '''CREATE TABLE IF NOT EXISTS %s (%s);'''
        processed_data_list = self._process_data_type_list(data_list)
        table_create_statement = table_create_placeholder % \
                                 (self.table_name, processed_data_list)
        # run command to create table
        self.c.executescript(table_create_statement)

    def _process_data_type_list(self, data_list):
        """Not all databases will use the field width column"""
        return (",".join([field_name + " " + data_type
                          for field_name, _, data_type in data_list]))

    def _requires_quotations(self, field):
        if field in self.quotation_fields:
            return True
        return False

    def _load_data(self, filename='data.csv'):
        data_list = []
        with open(filename, mode='r') as csv_file:
            csv_reader = csv.DictReader(csv_file, fieldnames=self.field_names)
            for row in csv_reader:
                # todo: create a row processor, since some data fields need to
                # be placed inside quotes or they must be converted
                row_list = []
                for field in self.field_names:
                    if self._requires_quotations(field):
                        row_list.append('"%s"' % row[field])
                    else:
                        row_list.append(row[field])
                data_list.append(','.join(row_list))
        return ["(" + data + ")" for data in data_list]

    def _load_table(self, data_load):
        table_load_placeholder = """INSERT INTO %s (%s) VALUES %s;"""
        table_load_statement = table_load_placeholder % \
            (self.table_name, ",".join(self.field_names), ",".join(data_load))
        # run command to create table
        self.c.executescript(table_load_statement)

    def create_and_load_table(self):
        data_list = self._process_schema('schema.csv')
        self._create_table(data_list)

        data_load = self._load_data('data.csv')
        self._load_table(data_load)

        # todo: add verbose option
        # print('Data inside: %s' % self.table_name)
        # print('fields: %s' % self.field_names)
        # for row in self.c.execute("select * from %s" % self.table_name):
        #     print(row)

def main():
    """ Process Command Line Arguments """
    parser = argparse.ArgumentParser(
        description='Create DB table and load data')
    parser.add_argument('-d', '--droptable', action='store_true',
                        help='Drop existing table')
    parser.add_argument('--tablename', help='Table name used to create schema')

    # todo: add an option to specify directory of dropped files
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
        dl.create_and_load_table()
    except UnknownDataTypeException as e:
        print("An exception occurred: %s" % e)
    except sqlite3.OperationalError as e:
        print("Sql error: %s" % e)
    except Exception as e:
        print("An uncaught exception occurred, please catch: %s" % type(e))


main()
