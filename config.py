from elasticsearch import Elasticsearch

CSV_OUTPUT_DIR = "output_csv"
JSON_OUTPUT_DIR = "output_json"

SHOULD_OUTPUT_CSV = True
SHOULD_OUTPUT_JSON = False

es_client = Elasticsearch("http://localhost:9200")
index_name = "products-v1-1"
expected_columns = []  # Choose columns. Leave empty array to choose all the columns.
query = {
    "_source": expected_columns,
    "query": {
        "match_all": {}
    }
}

# query = {
#   "query": {
#     "term": {
#       "id": {
#         "value": "40"
#       }
#     }
#   }
# }