import pyodbc
import random


class SqlDataReader:
    '''Testdata handler class, loads data from a sql database (list of tuples),
    which can be accessed by get_testdata()'''
    current_index = {}
    datasets = {}

    def __init__(self, driver, server, database, username, password):
        self.connection = pyodbc.connect(
            f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}"
        )

    def load_dataset(self, statement, tag, shuffle=False):
        '''Execute statement and store returned rows as a list of tuples marked with a tag.
        Shuffle for randomized data sequence.
        Row number limits should be included in the statement'''
        with self.connection, self.connection.cursor() as cur:
            cur.execute(statement)
            rows = cur.fetchall()
            dataset = rows.copy()
            if shuffle:
                random.shuffle(dataset)
            self.datasets.update({tag: dataset})
            self.current_index.update({tag: 0})

    def get_testdata(self, tag, wrap=True):
        '''get next tuple per tag, wrap around if the end of the list is reached and wrap=True'''
        if not self.datasets[tag] or not self.datasets[tag].__len__():
            return None

        if self.current_index[tag] >= self.datasets[tag].__len__():
            if wrap:
                self.current_index[tag] = 0
            else:
                return None

        res = self.datasets[tag][self.current_index[tag]]
        self.current_index[tag] += 1
        return res
