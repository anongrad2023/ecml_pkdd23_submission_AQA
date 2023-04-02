import mysql.connector
from decouple import config

from ccao_tools import setup, gather, util

DATABASE_NAME = "ccao_project_05"
EXP_ID = "initial_00"  # Ideally, we have a document explaining what each of these are 'for' somewhere in gdrive.

LOG_DIR = "data/logs/03_run_basic_gather_pipeline"
LOG_FN = f"2022-09-19_{EXP_ID}.txt"
log = util.Logger(LOG_DIR, LOG_FN, overwrite=True)
log.write_str("testing gather and initial logging utility.")

MAX_RESULTS_PER_CTX = 2000
RAW_CONTEXTS = {  # The 'name' of the context and its related full text query/statement/search-term.
    "vaccines":   "the effectiveness of vaccines",
    "drones":     "the USA's use of military drones",
    "world_usa":  "how the world views the USA",
    "pres_trump": "the presidency of donald trump",
    "pres_biden": "the presidency of joe biden",
    "russia_ukraine": "russia's role in the war in ukraine",
    "mil_op_us": "the us military in other countries.",
    "mil_op_russia": "the russian military in other countries."
}

cnx = mysql.connector.connect(user=config('DB_USER'), password=config('DB_PASSWORD'), host=config('DB_HOST'))
setup.create_schema(cnx, DATABASE_NAME, drop_existing=True)

# cnx = mysql.connector.connect(user=config('DB_USER'), password=config('DB_PASSWORD'), host=config('DB_HOST'),
#                               database=DATABASE_NAME)

print("adding contexts to DB and saving IDs.")
contexts = []
for context in RAW_CONTEXTS:
    query = RAW_CONTEXTS[context]
    ctx_id = gather.create_context(cnx, EXP_ID, context, query)
    contexts.append((ctx_id, context, query))
log.write_list(contexts, label="---contexts: (ctx_id, label, query)")

print("gathering content.")
for ctx_id, context_label, query in contexts:
    print(f"context: {context_label}; '{query}'")
    gather.basic_query_post(cnx, ctx_id, query, max_results=MAX_RESULTS_PER_CTX)
    gather.basic_query_comment(cnx, ctx_id, query, max_results=MAX_RESULTS_PER_CTX)
    gather.basic_query_tweet(cnx, ctx_id, query, max_results=MAX_RESULTS_PER_CTX)
