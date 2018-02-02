"""
This module uses SQL Server Bulk Insert to load data from Pandas DataFrames to SQL Server tables.


"""

# MAPPING: Dictionary containing mapping between Numpy(pandas) and SQL Server data types
MAPPING = {'float64': 'decimal({},{})', \
           'int64': 'bigint', \
           'object': 'varchar({})', \
           'datetime64[ns]': 'datetime2', \
           'int32': 'int', \
           'int16': 'smallintint', \
           'bool': 'bit'}

# RESERVED: List containing allocated (reserved) symbols that cannot be used in column names.
RESERVED = ['(', ')', ' ']

# DIR: Directory that both current OS user and SQL Server user have access to materialize CSV
# to be bulk inserted.
DIR = 'C:\\Users\\MSSQLSERVER'


class BaseEngine:
    """
    Wrapper object for the Engine to be used for the database connection.
    Takes server, database, user and password when creating an instance.
    """

    def __init__(self, server, database, user, password):
        import urllib.parse
        from sqlalchemy import create_engine

        self._params = urllib.parse.quote(
            "DRIVER={SQL Server Native Client 11.0};" +
            f"SERVER={server};DATABASE={database};UID={user};PWD={password}")

        self.engine = create_engine(f"mssql+pyodbc:///?odbc_connect={self._params}")


def build_command(dataframe, name):
    """
    Auxiliary function. Builds SQL command with table definition for creation

    :param dataframe: pandas DataFrame to use as base for definition
    :param name: name of the SQL Server table to created
    :return: string , containing command
    """

    precision = 14
    scale = 4
    padding = 2

    mapping = MAPPING

    mapping['float64'] = mapping['float64'].format(precision, scale)

    string_columns = {}

    for column in dataframe.columns:
        if str(dataframe[column].dtype) == 'object':
            string_columns[column] = min(max(dataframe[column].map(len).max() * padding, 128), 4096)
        if str(dataframe[column].dtype) == 'bool':
            dataframe[column] = dataframe[column].apply(lambda x: 1 if x else 0)

    command = 'create table {} ('

    for i, column in enumerate(dataframe.columns):

        dtype = mapping[str(dataframe[column].dtype)] if str(dataframe[column].dtype) != 'object' \
            else mapping[str(dataframe[column].dtype)].format(string_columns[column])
        command += ''.join([i for i in column if i not in RESERVED]) + ' ' + dtype
        if i < len(dataframe.columns) - 1:
            command += ', '
        if i == len(dataframe.columns) - 1:
            command += ')'
    command = command.format(name)

    return command


def create_table(dataframe, name, engine, replace=False):
    """
    Creates a SQL Server table based on a pandas DataFrame, given a sqlalchemy SQL Server engine

    :param dataframe: pandas DataFrame to create table from
    :param name: name for the table to be created
    :param engine: (sqlalchemy/pyodbc) engine to use
    :param replace: replace or exception if table exists
    :return: void
    """

    create = build_command(dataframe, name)
    if not replace:
        if check_existence(name, engine):
            raise Exception('Table already exists')
        else:
            with engine.begin() as conn:
                conn.execute(create)
    else:
        if check_existence(name, engine):
            with engine.begin() as conn:
                dropifexists = f"if object_id('{name}') is not null drop table {name};"
                conn.execute(dropifexists + create)
        else:
            with engine.begin() as conn:
                conn.execute(create)


def check_existence(name, engine):
    """
    Check if given SQL Server table exists.
    :param name: Table name to check
    :param engine: Engine to use for connection
    :return: bool, True if exists, False if not
    """

    conn = engine.connect()

    result = conn.execute(f"SELECT object_id('{name}')")

    for row in result:
        return bool(row[0])


def is_empty(name, engine):
    """
    Checks if a given table is empty

    :param name: Table name to check
    :param engine: Engine to use for connection
    :return: bool - True if empty, False is table has data
    """

    conn = engine.connect()

    result = conn.execute(f"SELECT 1 from {name}")

    for row in result:
        return bool(row[0])


def bulk_insert(dataframe, name, engine, append=True):
    """
    Uses the SQL Server bulk insert to load data from the CSV created by the pandas DataFrame
    to the SQL Server table.

    :param dataframe: pandas DataFrame to be loaded to SQL Server
    :param name: name of the SQL Server table to load the data to
    :param engine: engine to be used for the connection to the database
    :param append: append or exception if table has data
    :return: void
    """
    from tempfile import NamedTemporaryFile
    from os import remove

    if not check_existence(name, engine):
        raise Exception("Table does not exist!")

    if not is_empty(name, engine) and not append:
        raise Exception("The table is not empty!")

    command = """

   BULK INSERT {}  
   FROM '{}'  
   WITH  
     (  
        FIELDTERMINATOR =',',  
        ROWTERMINATOR = '\n',
        FIRSTROW = 2
      );  

    """
    with NamedTemporaryFile(dir=DIR, delete=True) as temp:
        try:
            temp.name = temp.name + '.csv'

            print(temp.name)
            dataframe.to_csv(temp.name, index=False)
            temp.close()

            with engine.begin() as conn:
                conn.execute(command.format(name, temp.name))
        finally:
            remove(temp.name)
