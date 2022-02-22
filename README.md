# DBT-iPy

Run DBT commands inside a IPython session.

## Installation

`pip install dbt-ipy`

## Tutorial

You can access the [tutorial notebook here](https://github.com/jmriego/dbt-ipy/blob/master/tutorial/Tutorial.ipynb)


## Quickstart Guide

#### Load the extension
`%load_ext dbt-ipy`

#### Connect to DBT RPC
The next thing you'll probably want to do is to run a DBT RPC server in the background. That will let you run queries, compile SQL code among other things:

`%dbt rpc <args>` (the args will be passed directly to dbt as command line arguments)

The two most important parameters would be:

- ``--port [port_number]`` Passing this parameter will run the DBT RPC on that port. Default is 8580
- ``--existing`` This will skip creating a new DBT RPC and instead it will connect to an existing one on the specified port


## Custom magic commands

### Compiling queries

The sql query in the cell will be compiled with the DBT RPC server and IPython will output the text::

```
%%compile_sql
SELECT ...
```

### Running queries

The sql query in the cell will be run on the DBT RPC server and IPython will output the agate table. Also, it will run its ``.print_table()`` method::

```
%%run_sql
SELECT ...
```

### Copying to clipboard

It is also possible to copy the last result returned by dbt to the clipboard. If that's a `%%compile_sql` compiled query it will return the SQL text but if it's a `%%run_sql` table, it will transform it to CSV and copy that to the clipboard. You can then paste the contents of the clipboard into a spreadsheet as usual.

```
%dbt_clipboard
```
