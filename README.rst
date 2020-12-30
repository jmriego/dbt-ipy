#######
dbt-ipy
#######

Run DBT commands inside a IPython session.

So far only the following IPython magics are implemented:

-----------------
Quickstart Guide:
-----------------

The first thing you'll probably want to do is to run a DBT RPC server in the background. That will let you run queries, compile SQL code among other things::

  %dbt rpc <args>
  (the args will be passed directly to dbt as command line arguments)

**Compiling queries:**

The sql query in the cell will be compiled with the DBT RPC server and IPython will output the text::

  %%compile_sql

**Running queries:**

The sql query in the cell will be run on the DBT RPC server and IPython will output the agate table. Also, it will run its ``.print_table()`` method::

  %%run_sql
