"""
I'm going to call a csv with content-to-be-labeled, containing proper column names and all, with target task
label columns, a "task". That's one file ready to be labeled by a human.

We're going to assume we'll always include all the questions.

Inputs
    Content Type - (Tweet, Post, Comment)
    Context ID   - (<CTX_ID, None) -> If none, use 'all', if there is one, filter to just that context.
    Questions    - Ignore for now, just fetch ALL questions.
    Filename     - What to call the result csv file.
    DB Name      - Name of the DB to actually use.
    DB Credentials - Should just stay in a .env file, like usual
Return
    Nothing

Side Effects
    Create a csv file named <Filename>.csv in the working directory.

CSV Format
0:  _        |  _        |  _        |  question1_id    | ... |  questionN_id
1: 'pair_id' | 'context' | 'content' | 'question1_text' | ... | 'questionN_text'
2:    NNN    |  text     |  text     |  empty, to label | ... | empty, to label.
... for all entries.

y for yes, n for no.  Also take 0/1, Y/N
"""
import mysql.connector
from decouple import config

DATABASE_NAME = "ccao_project_05"

# ANNOTATORS = ['reizas', 'john', 'sunny']  # IDs: [1,2,3]
ANNOTATORS = ['lopes', 'eckert', 'irviana']  # IDs: [4,5,6]
CONTENT_TYPES = ['comment']  # Can be 'tweet', 'post', or 'comment'
SHEET_SIZE = 60
NUM_SHEETS = 2

SHEET_NUM = "2"
DIR_OUT = "data/task_sheets_02"

SQL_LABELERS = ""
# SQL_QUESTIONS = "SELECT question_id, text FROM question;"
SQL_QUESTIONS = """
    SELECT * FROM ccao_project_05.question
    WHERE question_id in (5, 6, 7, 8, 10, 11, 12);
"""

# Note - Updated to exclude previously answered pairs.
SQL_ROWS_TO_LABEL = """
    SELECT pair.cc_pair_{0}_id, context.context_query, content_{0}.text
    FROM context_{0} as pair
    JOIN 
        context ON context.context_id = pair.context_id 
    JOIN 
        content_{0} ON content_{0}.{0}_id = pair.content_{0}_id
    WHERE
        context.context_id IN (206, 211, 209, 205, 207, 208)
        AND
        pair.cc_pair_comment_id NOT IN (
            SELECT ccp_comment_id FROM answer_comment
        )
    ORDER BY RAND()
    LIMIT {1};
"""

cnx = mysql.connector.connect(user=config('DB_USER'),
                              password=config('DB_PASSWORD'),
                              host=config('DB_HOST'),
                              database=DATABASE_NAME)
cur = cnx.cursor()

cur.execute(SQL_QUESTIONS)
questions = cur.fetchall()


def write_sheet_csv(annotator, c_type, b_id, data):
    fn_out = f"{DIR_OUT}/{annotator}_{c_type}_{b_id}.csv"
    with open(fn_out, 'w') as f:
        question_id_str = "_,_,_,"
        question_id_str += ",".join([f"{q[0]}" for q in questions])
        question_id_str += "\n"
        f.write(question_id_str)

        # Column Label Row
        col_headers_str = "pair_id,context,content,"
        col_headers_str += ",".join([f"\"{q[1]}\"" for q in questions])
        col_headers_str += "\n"
        f.write(col_headers_str)

        for row in data:
            row_str = f"{row[0]},{row[1]},\"{row[2]}\","
            row_str += ",".join(["" for q in questions])
            row_str += "\n"
            f.write(row_str)


cur.execute(SQL_ROWS_TO_LABEL.format("comment", SHEET_SIZE * NUM_SHEETS))
rows = cur.fetchall()

for annotator in ANNOTATORS:
    write_sheet_csv(annotator, "comments", SHEET_NUM, rows)






