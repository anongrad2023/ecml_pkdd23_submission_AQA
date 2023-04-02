"""
Script to process a single students task sheet.

Needs to be sure that there is a labeler in the system, with appropriate ID.
"""
import csv
import mysql.connector
from decouple import config

# ANNOTATOR = ['reizas', 1]
# ANNOTATOR = ['john',   2]
ANNOTATOR = ['sunny',  3]
# ANNOTATOR = ['lopes',  4]
# ANNOTATOR = ['irviana',  6]
# ANNOTATOR = ['small',  7]
# ANNOTATOR = ['dupont',  8]


DATABASE_NAME = "ccao_project_05"

ANNOTATOR_ID = ANNOTATOR[1]
DIR_SHEETS = "data/task_sheets_by_anno"
FN_ANNOTATED = f"{ANNOTATOR[0]}/annotated_{ANNOTATOR[0]}_comments_10.csv"

QUESTION_IDS = [5, 6, 7, 8, 10, 11, 12]
QUESTION_COL_OS = 3

SQL_INSERT_ANSWER = """
    INSERT IGNORE INTO `ccao_project_05`.`answer_comment`
        (`question`, `ccp_comment_id`, `answer`, `labeler`)
    VALUES
        ({}, {}, {}, {}); 
"""

cnx = mysql.connector.connect(user=config('DB_USER'),
                              password=config('DB_PASSWORD'),
                              host=config('DB_HOST'),
                              database=DATABASE_NAME)
cur = cnx.cursor()


def process_answer(s):
    if s == '':
        return 3
    return int(s)


bugged_lines = 0
with open(f"{DIR_SHEETS}/{FN_ANNOTATED}", 'r') as f_csv:
    print(f"processing: {FN_ANNOTATED}")
    csv = csv.reader(f_csv)
    q_id_line = next(csv)
    next(csv)  # advance past header row.
    for row in csv:
        try:
            if row[0] == "": break
            answers = list(map(process_answer, row[QUESTION_COL_OS:]))
            for q_id, val in zip(QUESTION_IDS, answers):
                query = SQL_INSERT_ANSWER.format(q_id, row[0], val, ANNOTATOR_ID)
                cur.execute(query)
            cnx.commit()
        except Exception as e:
            bugged_lines += 1

print(f"bugged lines: {bugged_lines}")
