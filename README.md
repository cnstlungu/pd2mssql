# pd2mssql
Pandas DataFrame to SQL Server tables

## What is pd2mssql

pd2mssql is an utility enabling fast import of Pandas DataFrames into SQL Server tables. 

## Rationale

The built-in solution, <b>pandas to_sql</b> is slow in its current implementation, taking too long even for a modest dataframe.

Discussions with further details are available [here](https://stackoverflow.com/questions/29706278/python-pandas-to-sql-with-sqlalchemy-how-to-speed-up-exporting-to-ms-sql).


## How it works

In a nutshell, pd2mssql exports the dataframe into a temporary file and bulk inserts into SQL Server using the builtin SQL Server Bulk insert functionality.

## Requirements

Given the fact that the package is dependent on SQL Server Bulk insert, the following conditions have to be met:
* The account connecting to SQL Server should have the bulk permission on that particular server.
* The SQL Server instance/user have to have access to CSV destination

See [requirements](https://github.com/cnstlungu/pd2mssql/blob/master/requirements.txt) for package requirements.

## Quick start


### Installation

```bash
cd pd2mssql

pip install .
```
### Usage

```python
import pd2mssql

#Set directory for temporary file. Should be accessible to SQL user.
pd2mssql.DIR = 'C://Users/MSSQLServer'

base = pd2mssql.BaseEngine('yourserver','yourdatabase','youruser','yourpassword')

engine = base.engine

#Creates the table. 
pd2mssql.create_table(dataframe, table_name, engine)

#Bulk inserts into the table
pd2mssql.bulk_insert(dataframe, table_name, engine)


```

## Further reading

Further information is available in the [documentation](https://github.com/cnstlungu/pd2mssql/blob/master/documentation.html).


