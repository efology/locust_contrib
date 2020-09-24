from locust import TaskSet, task, constant_pacing
from locust.contrib.fasthttp import FastHttpUser
import json
import configparser
from tools import sql_data_reader

config = configparser.ConfigParser()
config.read("example.conf")

dl = sql_data_reader.SqlDataReader(
    config.get('testdb', 'driver'),
    config.get('testdb', 'host'),
    config.get('testdb', 'db'),
    config.get('testdb', 'user'),
    config.get('testdb', 'pwd')
)

ROWS_PER_SLAVE = 1000000
# first data set, users
dl.load_dataset(f"select top {ROWS_PER_SLAVE} UserId, FirstName, LastName from Users;", "users")
# second data set, products, for example
dl.load_dataset(f"select top {ROWS_PER_SLAVE} ProductId, ArticleNo, ProductName from Products;", "prd")


class FooBar(TaskSet):

    @task
    def foo(self):
        # get next row
        tup = dl.get_testdata("users", wrap=True)
        userId = tup[0]
        firstName = tup[1]
        lastName = tup[2]
        body = {
            "userId": userId,
            "firstName": firstName,
            "lastName": lastName
        }
        headers = {"Content-Type": "application/json"}
        self.client.post("/api/users", json.dumps(body), headers=headers, name="api/users")
        
    @task
    def bar(self):
        # get a row from the other dataset
        tup = dl.get_testdata("prd", wrap=True)
        productId = tup[0]
        articleNo = tup[1]
        productName = tup[2]
        body = {
            "productId": productId,
            "articleNo": articleNo,
            "productName": productName
        }
        headers = {"Content-Type": "application/json"}
        self.client.post("/api/products", json.dumps(body), headers=headers, name="api/product")


class FooBarLocust(FastHttpUser):
    tasks = {FooBar: 1}
    wait_time = constant_pacing(1.0)
