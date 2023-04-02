"""
Quick script to add questions to the db.
"""
import mysql.connector
from decouple import config

DATABASE_NAME = "ccao_project_05"

QUESTIONS = [
    "Is the content related to the context?",
    "Is the content positive wrt the context?",
    "Is the content negative wrt the context?",
    "Does the content contain sarcasm?"]

SQL_INSERT_QUESTION = """
    INSERT INTO question (text)
    VALUES ("{}");
"""

cnx = mysql.connector.connect(user=config('DB_USER'),
                              password=config('DB_PASSWORD'),
                              host=config('DB_HOST'),
                              database=DATABASE_NAME)
cur = cnx.cursor()

for q in QUESTIONS:
    cur.execute(SQL_INSERT_QUESTION.format(q))
    cnx.commit()
