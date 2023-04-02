import pandas as pd
import mysql.connector
from decouple import config

FN_RAW = "data/full_answer_set_20221220_fixed_encoding.csv"
DATABASE_NAME = "ccao_project_05"

DIR_OUT = "data/dec_exp_data"

SQL_ALL_ANSWERS = """
SELECT ac.ccp_comment_id, ctxc.context_id, cntc.comment_id, ac.labeler, ac.question, ac.answer, cntc.text 
FROM answer_comment as ac
JOIN context_comment as ctxc on ctxc.cc_pair_comment_id=ac.ccp_comment_id
JOIN content_comment as cntc on cntc.comment_id=ctxc.content_comment_id;
"""

cnx = mysql.connector.connect(user=config('DB_USER'), password=config('DB_PASSWORD'),
                              host=config('DB_HOST'), database=DATABASE_NAME)

df = pd.read_sql(SQL_ALL_ANSWERS, cnx)
df = df.astype(dtype={'ccp_comment_id': int,
                      'context_id': int,
                      'comment_id': str,
                      'labeler': int,
                      'question': int,
                      'answer': int,
                      'text': str})

# Now we need to cut this up into datasets.
df_answers = df.groupby(by=['ccp_comment_id', 'question'], group_keys=True)

raws_1 = []
raws_2 = []
raws_3 = []
raws_4 = []
for gid in df_answers.groups:
    g = df_answers.get_group(gid)
    match len(g):
        case 1:
            raws_1.append(g)
        case 2:
            raws_2.append(g)
        case 3:
            raws_3.append(g)
        case 4:
            raws_4.append(g)

print("size of each 'coverage' group - labeled by N annotators")
print(f"1: {len(raws_1)}")
print(f"2: {len(raws_2)}")
print(f"3: {len(raws_3)}")
print(f"4: {len(raws_4)}")

gs_2 = 0
a2_gs = []
for g in raws_2:
    ans = set(g['answer'].values)
    if len(ans) == 1:
        gs_2 += 1
        a2_gs.append(g)

gs_3 = 0
con_3 = 0
a3_gs = []
a3_con = []
for g in raws_3:
    ans = set(g['answer'].values)
    if len(ans) == 1:
        gs_3 += 1
        a3_gs.append(g)
    if len(ans) == 2:
        con_3 += 1
        a3_con.append(g)

gs_4 = 0
con_4 = 0
a4_gs = []
a4_con = []
for g in raws_4:
    ans = set(g['answer'].values)
    if len(ans) == 1:
        gs_4 += 1
        a4_gs.append(g)
    if len(ans) == 3:
        con_4 += 1
        a4_con.append(g)

print("gs/consensus stats")
print("-- 2 Annotators --")
print(f"       gs: {gs_2}/{len(raws_2)} = {gs_2/len(raws_2)}")
print("-- 3 Annotators --")
print(f"       gs: {gs_3}/{len(raws_3)} = {gs_3/len(raws_3)}")
print(f"consensus: {con_3}/{len(raws_3)} = {con_3/len(raws_3)}")
print("-- 4 Annotators --")
print(f"       gs: {gs_4}/{len(raws_4)} = {gs_4/len(raws_4)}")
print(f"consensus: {con_4}/{len(raws_4)} = {con_4/len(raws_4)}")

columns = ['context_id', 'comment_id', 'question_id', 'answer', 'content_text']

# A1 - Only have a gold standard
raws_a1_gs = []
for g in raws_1:
    i = g.values[0]
    raws_a1_gs.append([i[1], i[2], i[4], i[5], i[6]])

# A2 - GS
raws_a2_gs = []
for g in raws_2:
    ans_1 = g.values[0][5]
    ans_2 = g.values[1][5]
    if ans_1 == ans_2:
        i = g.values[0]
        raws_a2_gs.append([i[1], i[2], i[4], i[5], i[6]])

# A3 - GS and Consensus
raws_a3_gs  = []
raws_a3_con = []
for g in raws_3:
    ans_1 = g.values[0][5]
    ans_2 = g.values[1][5]
    ans_3 = g.values[2][5]

    i = g.values[0]
    match len(set([ans_1, ans_2, ans_3])):
        case 1:
            raws_a3_gs.append([i[1], i[2], i[4], i[5], i[6]])
        case 2:
            # aab, baa, aba
            if ans_1 == ans_2:
                ans = ans_1
            elif ans_2 == ans_3:
                ans = ans_2
            else:
                ans = ans_1
            raws_a3_con.append([i[1], i[2], i[4], ans, i[6]])

df_a1_gs = pd.DataFrame(raws_a1_gs, columns=columns)
df_a2_gs = pd.DataFrame(raws_a2_gs, columns=columns)
df_a3_gs = pd.DataFrame(raws_a3_gs, columns=columns)
df_a3_con = pd.DataFrame(raws_a3_con, columns=columns)

df_a1_gs.to_csv(f"{DIR_OUT}/a1_gs.csv")
df_a2_gs.to_csv(f"{DIR_OUT}/a2_gs.csv")
df_a3_gs.to_csv(f"{DIR_OUT}/a3_gs.csv")
df_a3_con.to_csv(f"{DIR_OUT}/a3_con.csv")
