import mysql.connector
from decouple import config
import csv

DIR_OUT = "data/test_data_pipeline"
FN_QUESTIONS = "questions_01.csv"
FN_CONTEXTS  = "contexts_01.csv"
FN_CONTENT_ANSWERS   = "content_answers_01.csv"

SQL_QUESTIONS = """
    SELECT * FROM question
    WHERE question_id in (5, 6, 7, 8, 10, 11, 12);
"""

SQL_CONTEXTS = """
    SELECT context_id, context_query FROM context
    WHERE context.context_id IN (206, 211, 209, 205, 207, 208)
"""

SQL_ROWS = """
    SELECT 
        context_comment.context_id as 'context_id', 
        content_comment.comment_id as 'comment_id',
        question.question_id       as 'question_id', 
        answer_comment.answer      as 'answer', 
        content_comment.text       as 'content'
    FROM answer_comment
    JOIN question        on question.question_id               = answer_comment.question
    JOIN context_comment on context_comment.cc_pair_comment_id = answer_comment.ccp_comment_id
    JOIN content_comment on content_comment.comment_id         = context_comment.content_comment_id
    WHERE
        answer_comment.answer != 3 AND
        question.question_id IN (5, 6, 7, 8, 10, 11, 12) AND
        context_comment.context_id IN (206, 211, 209, 205, 207, 208)
    ORDER BY RAND()
    LIMIT 4000;
"""


DATABASE_NAME = "ccao_project_05"
cnx = mysql.connector.connect(user=config('DB_USER'),
                              password=config('DB_PASSWORD'),
                              host=config('DB_HOST'),
                              database=DATABASE_NAME)
cur = cnx.cursor()

with open(f"{DIR_OUT}/{FN_QUESTIONS}", 'w') as f:
    cur.execute(SQL_QUESTIONS)
    rows = cur.fetchall()
    writer = csv.writer(f)
    writer.writerow(['question_id', 'question_text'])
    writer.writerows(rows)

with open(f"{DIR_OUT}/{FN_CONTEXTS}", 'w') as f:
    cur.execute(SQL_CONTEXTS)
    rows = cur.fetchall()
    writer = csv.writer(f)
    writer.writerow(['context_id', 'context_text'])
    writer.writerows(rows)

with open(f"{DIR_OUT}/{FN_CONTENT_ANSWERS}", 'w') as f:
    cur.execute(SQL_ROWS)
    rows = cur.fetchall()
    writer = csv.writer(f)
    writer.writerow(['context_id', 'comment_id', 'question_id', 'answer', 'content_text'])
    writer.writerows(rows)
