import mysql.connector
from decouple import config

from ccao_tools import setup, gather

DATABASE_NAME = "ccao_project_00"
EXP_ID = "test_00"

CONTEXTS = {  # The 'name' of the context and its related full text query/statement/search-term.
    "vaccines":   "the effectiveness of vaccines",
    "drones":     "the USA's use of military drones",
    "world_usa":  "how the world views the USA",
    "pres_trump": "the presidency of donald trump",
    "pres_biden": "the presidency of joe biden",
    "russia_ukraine": "russia's role in the war in ukraine",
}

cnx = mysql.connector.connect(user=config('DB_USER'), password=config('DB_PASSWORD'), host=config('DB_HOST'))
setup.create_schema(cnx, DATABASE_NAME, drop_existing=True)

# cnx = mysql.connector.connect(user=config('DB_USER'), password=config('DB_PASSWORD'), host=config('DB_HOST'),
#                               database=DATABASE_NAME)

for context in CONTEXTS:
    query = CONTEXTS[context]
    ctx_id = gather.create_context(cnx, EXP_ID, context, query)

    gather.basic_query_post(cnx, ctx_id, query)
    gather.basic_query_comment(cnx, ctx_id, query)
    gather.basic_query_tweet(cnx, ctx_id, query)
