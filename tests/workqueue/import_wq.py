from dnpcsql.schema import init_sql

from dnpcsql.workqueue import import_all

connection = init_sql()

import_all(connection, "./transaction_log")
