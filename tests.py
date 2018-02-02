import unittest

def create_engine():
    from pd2mssql import BaseEngine
    def get_credentials():
        import configparser
        parser = configparser.ConfigParser()
        parser.read('credentials.ini')
        return parser['SQL Server']

    base = BaseEngine('localhost', 'AdventureWorksDW2012', get_credentials()['user'],
                       get_credentials()['pass'])
    return base.engine


def get_dataset():
    from pandas import DataFrame
    from numpy import array, ones
    from datetime import datetime, timedelta

    ints = array([i for i in range(10)])
    decs = array([i + 1.01921 for i in range(10)])
    strs = ['abc'] * 10
    dt = datetime(2017, 1, 1)
    dts = array([dt + timedelta(hours=i) for i in range(10)])
    bools = ones((10), dtype=bool)
    df = DataFrame({'ints': ints, 'decs': decs, 'strs': strs, 'dts': dts, 'bools': bools})

    return df


class TestConnection(unittest.TestCase):

    def setUp(self):
        self.engine = create_engine()

    def test_connection(self):
        conn = self.engine.connect()
        rs = conn.execute("SELECT getdate()")

        for row in rs:
            self.assertTrue(row[0])
            break


class TestCreation(unittest.TestCase):

    def setUp(self):
        import uuid
        self.df = get_dataset()
        self.randstring = str.replace('test' + str(uuid.uuid4()), '-', "")
        self.engine = create_engine()

        def prepare():
            with self.engine.begin() as conn:
                conn.execute(f"IF OBJECT_ID('{self.randstring}') is not null drop table dbo.{self.randstring}")

        self.cleanup = prepare

        self.cleanup()

    def tearDown(self):
        self.cleanup()

    def test_create(self):
        from pd2mssql import create_table

        create_table(self.df, self.randstring, self.engine)

        conn = self.engine.connect()
        rs = conn.execute(f"SELECT OBJECT_ID('{self.randstring}')")

        for row in rs:
            self.assertTrue(row[0])
            break


class TestBulkInsert(unittest.TestCase):

    def setUp(self):
        import uuid
        self.df = get_dataset()
        self.randstring = str.replace('test' + str(uuid.uuid4()), '-', "")
        self.engine = create_engine()

        def prepare():
            with self.engine.begin() as conn:
                conn.execute(f"IF OBJECT_ID('{self.randstring}') is not null drop table dbo.{self.randstring}")

        self.cleanup = prepare

        self.cleanup()

    def tearDown(self):
        self.cleanup()

    def test_bulk_insert(self):
        from pd2mssql import create_table, bulk_insert

        create_table(self.df, self.randstring, self.engine)

        bulk_insert(self.df, self.randstring, self.engine)

        conn = self.engine.connect()
        rs = conn.execute(f"SELECT count(1) from {self.randstring}")

        for row in rs:
            self.assertTrue(row[0] > 0)
            break


class TestDataTypes(unittest.TestCase):

    def setUp(self):
        import uuid
        self.df = get_dataset()
        self.randstring = str.replace('test' + str(uuid.uuid4()), '-', "")
        self.engine = create_engine()

        def prepare():
            with self.engine.begin() as conn:
                conn.execute(f"IF OBJECT_ID('{self.randstring}') is not null drop table dbo.{self.randstring}")

        self.cleanup = prepare

        self.cleanup()

    def tearDown(self):

        self.cleanup()

    def test_datatypes(self):

        from pd2mssql import create_table, bulk_insert

        create_table(self.df, self.randstring, self.engine)

        bulk_insert(self.df, self.randstring, self.engine)

        conn = self.engine.connect()

        rs = conn.execute(f"""
                            SELECT DATA_TYPE
                            FROM INFORMATION_SCHEMA.COLUMNS
                            WHERE
                            TABLE_NAME = '{self.randstring}' AND
                            COLUMN_NAME in ('bools','decs', 'ints', 'strs','dts')
                            ORDER BY 1
                            """)

        result = []

        for row in rs:
            for i in row:
                result.append(i)

        self.assertTrue(result == ['bigint', 'datetime2', 'decimal', 'int', 'varchar'])


if __name__ == '__main__':
    unittest.main()