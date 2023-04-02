import os
import mysql.connector
from decouple import config

# ANNOTATOR_GROUPS = [['A', [['reizas', 1], ['john', 2],    ['sunny', 3]]],
#                     ['B', [['lopes', 4],  ['irviana', 6], ['dong', 12]]]]

# ANNOTATOR_GROUPS = [['C', [['irviana', 6], ['sunny', 3]]]]
#
# ANNOTATOR_GROUPS = [['D', [['snipes', 7], ['vardanyan', 8], ['small', 9]]],
#                     ['E', [['tariku', 10], ['vaughn', 11], ['dupont', 12]]]]


ANNOTATOR_GROUPS = [['F', [['irviana', 6], ['dong', 12]]]]

DATABASE_NAME = "ccao_project_05"

# BATCH_IDS = ['03', '04', '05', '06', '07', '08', '09', '10']
# BATCH_IDS = ['11', '12', '13', '14', '15', '16', '17', '18', '19', '20']
# BATCH_IDS = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10']

BATCH_IDS = [f"{i}" for i in range(10)]

DATA_DIR_ROOT = "data/task_sheets_by_anno"

CONTENT_TYPES = ['comment']  # Can be 'tweet', 'post', or 'comment'
SHEET_SIZE = 60
NUM_SHEETS = 2

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


def write_sheet_csv(annot, c_type, dir_out, pair_g_id, b_id, data):

    if not os.path.exists(dir_out):
        os.mkdir(dir_out)

    fn_out = f"{dir_out}/{annot}_{c_type}_{pair_g_id}{b_id}.csv"
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


def write_batch_pair_list(dir_out, pair_id, b_id, data):
    with open(f"{dir_out}/{pair_id}{b_id}_pair_ids.txt", 'w') as f_out:
        for row in data:
            f_out.write(f"{row[0]}\n")


for batch_id in BATCH_IDS:

    for pair_group, annotators in ANNOTATOR_GROUPS:
        cur.execute(SQL_ROWS_TO_LABEL.format("comment", SHEET_SIZE * NUM_SHEETS))
        rows = cur.fetchall()
        write_batch_pair_list(f"{DATA_DIR_ROOT}/pair_lists", pair_group, batch_id, rows)

        for anno_name, anno_id in annotators:
            write_sheet_csv(anno_name, "comments", f"{DATA_DIR_ROOT}/{anno_name}", pair_group, batch_id, rows)






