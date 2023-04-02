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

# At this point we have groups for each question-context-content triple
# in this, we will have labeler ids.

labeler_counts = dict() # indexed/keyed by labeler-id, points to a count of 'agrees', and 'total' and then a dict of
                        # context_ids where each entry is ('agrees', 'total')
                        # and a dict keyed by question id, where each entry is ('agrees', 'total')

for g in raws_3:
    ctx_id = g['context_id'].values[0]
    cmt_id = g['comment_id'].values[0]
    q_id   = g['question'].values[0]
    maj_ans = g['answer'].mode().values[0]

    for l_idx in range(len(g)):
        l_row = g.values[l_idx]
        l_id  = l_row[3]
        l_ans = l_row[5]

        if l_id not in labeler_counts:
            labeler_counts[l_id] = {
                'all_agree': 0,
                'all_total': 0,
                'per_ctx': dict(),
                'per_q': dict()
            }

        if ctx_id not in labeler_counts[l_id]['per_ctx']:
            labeler_counts[l_id]['per_ctx'][ctx_id] = {
                'total': 0,
                'agree': 0
            }

        if q_id not in labeler_counts[l_id]['per_q']:
            labeler_counts[l_id]['per_q'][q_id] = {
                'total': 0,
                'agree': 0
            }

        labeler_counts[l_id]['all_total'] += 1
        labeler_counts[l_id]['per_ctx'][ctx_id]['total'] += 1
        labeler_counts[l_id]['per_q'][q_id]['total'] += 1

        if l_ans == maj_ans:
            labeler_counts[l_id]['all_agree'] += 1
            labeler_counts[l_id]['per_ctx'][ctx_id]['agree'] += 1
            labeler_counts[l_id]['per_q'][q_id]['agree'] += 1

CTX_IDS = list(set(df['context_id'].values))
Q_IDS   = list(set(df['question'].values))

# Labeler ID, ctx_ids...
#   N,  %, %, %
per_ctx_rows = []
per_q_rows = []
per_ctx_abs_rows = []
per_q_abs_rows = []
for labeler in labeler_counts:
    ctx_row = [labeler]
    ctx_abs_row = [labeler]
    q_row   = [labeler]
    q_abs_row = [labeler]
    labeler = labeler_counts[labeler]
    for ctx_id in CTX_IDS:
        if ctx_id in labeler['per_ctx']:
            p_ctx = labeler['per_ctx'][ctx_id]
            ctx_row.append(p_ctx['agree']/p_ctx['total'])
            ctx_abs_row.append(p_ctx['agree'])
            ctx_abs_row.append(p_ctx['total'])
        else:
            ctx_row.append(-1)
            ctx_abs_row.append(0)
            ctx_abs_row.append(0)
    per_ctx_rows.append(ctx_row)
    per_ctx_abs_rows.append(ctx_abs_row)

    for q_id in Q_IDS:
        if q_id in labeler['per_q']:
            p_q = labeler['per_q'][q_id]
            q_row.append(p_q['agree']/p_q['total'])
            q_abs_row.append(p_q['agree'])
            q_abs_row.append(p_q['total'])
        else:
            q_row.append(-1)
            q_abs_row.append(0)
            q_abs_row.append(0)
    per_q_rows.append(q_row)
    per_q_abs_rows.append(q_abs_row)

CTX_COLS = ['labeler_id']
CTX_COLS.extend(CTX_IDS)
Q_COLS   = ['labeler_id']
Q_COLS.extend(Q_IDS)

print(CTX_COLS, Q_COLS)

df_anno_ctx = pd.DataFrame(per_ctx_rows, columns=CTX_COLS)
df_anno_q   = pd.DataFrame(per_q_rows, columns=Q_COLS)

df_anno_ctx.to_csv("data/dec_exp_data/anno_ctx.csv", index=False)
df_anno_q.to_csv("data/dec_exp_data/anno_q.csv",     index=False)

CTX_ABS_COLS = ['labeler_id']
Q_ABS_COLS   = ['labeler_id']

for ctx_id in CTX_IDS:
    CTX_ABS_COLS.append(f"{ctx_id} Agree")
    CTX_ABS_COLS.append(f"{ctx_id} Total")

for q_id in Q_IDS:
    Q_ABS_COLS.append(f"{q_id} Agree")
    Q_ABS_COLS.append(f"{q_id} Total")

df_anno_ctx_abs = pd.DataFrame(per_ctx_abs_rows, columns=CTX_ABS_COLS)
df_anno_q_abs = pd.DataFrame(per_q_abs_rows, columns=Q_ABS_COLS)

df_anno_ctx_abs.to_csv("data/dec_exp_data/anno_ctx_abs.csv", index=False)
df_anno_q_abs.to_csv("data/dec_exp_data/anno_q_abs.csv",     index=False)