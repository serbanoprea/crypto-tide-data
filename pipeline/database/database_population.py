import luigi
import abc
import pyodbc


class _DatabaseQuery(luigi.Task):
    @abc.abstractproperty
    def database(self):
        pass

    @abc.abstractproperty
    def table(self):
        pass

    @abc.abstractproperty
    def sql(self):
        pass

    @property
    def connection(self):
        connection_string = '''
            DRIVER={ODBC Driver 17 for SQL Server};SERVER=test;DATABASE=test;UID=user;PWD=password
        '''
        return pyodbc.connect(connection_string)
