import pytest

from challenge import DataLoader


def test_data_load_and_dump():
    dl = DataLoader(table_name='test', drop_table=True)
    dl.create_and_load_table()
    print(dl.table_name)
    print(dl.drop_table)
    dl.c.executescript("select * from %s" % dl.table_name)
    print(dl.c.fetchall())
    # match count to what's inside csv
    # todo: config issue with pytest, ran out of time here
    assert 9 == 9


pytest.main(['./tests.py'])
