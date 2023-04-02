import mysql.connector
from decouple import config

from ccao_tools import setup

DATABASE_NAME = "ccao_project_05"

cnx = mysql.connector.connect(user=config('DB_USER'), password=config('DB_PASSWORD'), host=config('DB_HOST'))

setup.create_schema(cnx, DATABASE_NAME, drop_existing=True)
