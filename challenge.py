import argparse


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

main()
