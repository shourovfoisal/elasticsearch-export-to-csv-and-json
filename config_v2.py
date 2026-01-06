import os
import sys
from elasticsearch import Elasticsearch
from dotenv import load_dotenv
from typing import Dict, Any, List

load_dotenv()

CSV_OUTPUT_DIR = "output_csv"
JSON_OUTPUT_DIR = "output_json"

SHOULD_OUTPUT_CSV = True
SHOULD_OUTPUT_JSON = False

es_client = Elasticsearch(os.getenv("ES_INDEX_ADDRESS"))
index_name = os.getenv("ES_INDEX_NAME")

columns_str = os.getenv("ES_EXPECTED_COLUMNS", "").strip()
if not columns_str:
    sys.exit("ERROR: ES_EXPECTED_COLUMNS is missing or empty!")


expected_columns: List[str] = columns_str.split(",")
query: Dict[str, Any] = {
    "_source": expected_columns,
    "query": {
        "match_all": {}
    }
}