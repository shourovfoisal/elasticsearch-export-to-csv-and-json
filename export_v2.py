from elasticsearch.helpers import scan
import pandas as pd
from pandas import DataFrame
import os
from config_v2 import (
    es_client,
    index_name,
    expected_columns,
    query,
    CSV_OUTPUT_DIR,
    JSON_OUTPUT_DIR,
    SHOULD_OUTPUT_CSV,
    SHOULD_OUTPUT_JSON
)
from typing import Dict, Any, List, cast

csv_export_path=""
json_export_path=""

def main():
    create_output_directories()
    
    flags: Dict[str, Any] = {"is_first_batch": True}
    output_chunk_size = 5000
    buffer: List[Dict[str, Any]] = []
    
    count: int = 0
    for doc in read_the_index():
        if count % 1000 == 0:
            print(f"Collecting data {count}")
        count+=1
            
        source = flatten_arrays(doc["_source"])
        buffer.append(source)
        
        # Write in chunks
        if len(buffer) >= output_chunk_size:
            print(f"Writing data {count}")
            produce_output(buffer=buffer, flags=flags)
    
    if buffer: produce_output(buffer=buffer, flags=flags)    # Write remaining

def read_the_index():
    return scan(
        client=es_client,
        query=query,
        index=index_name,
        scroll='2m',
        size=1000,
        preserve_order=True,
    )

def create_output_directories():
    global csv_export_path, json_export_path
    if SHOULD_OUTPUT_CSV:
        os.makedirs(CSV_OUTPUT_DIR, exist_ok=True)
        csv_export_path = os.path.join(CSV_OUTPUT_DIR, "export.csv")
    if SHOULD_OUTPUT_JSON:
        os.makedirs(JSON_OUTPUT_DIR, exist_ok=True)
        json_export_path = os.path.join(JSON_OUTPUT_DIR, "export.json")


# This method takes each object, and converts its array fields from
# Python array representation "[-0.48734865, 0.18669638, 0.13392454, -0.17082427, 0.25744757]" to
# CSV friendly array representation "-0.48734865, 0.18669638, 0.13392454, -0.17082427, 0.25744757"
def flatten_arrays(obj: Dict[str, Any]):
    for k, v in obj.items():
        if isinstance(v, list):
            v_list = cast(list[Any], v)
            obj[k] = ", ".join(map(str, v_list))
    return obj

# This method ensures that
# The DataFrame has exactly the columns you want
# In the order you want
# If a column name from the columns list doesn't exist
# It still gets included in the output, with all values set to NaN
def rearrange_columns(df: DataFrame):
    return df.reindex(columns=expected_columns)
    

def write_to_file(
        df: DataFrame, 
        is_first_batch: bool, 
        should_output_csv: bool = True, 
        should_output_json: bool = True
    ):
    if is_first_batch:
        if should_output_csv: df.to_csv(csv_export_path, index=False, mode="w")
        if should_output_json: df.to_json(json_export_path, orient="records", lines=True, mode="w") # type: ignore
    else:
        if should_output_csv: df.to_csv(csv_export_path, index=False, mode="a", header=False)
        if should_output_json: df.to_json(json_export_path, orient="records", lines=True, mode="a") # type: ignore


def produce_output(
        buffer: List[Dict[str, Any]], 
        flags: Dict[str, Any]
    ):
    df = rearrange_columns(df=pd.DataFrame(buffer))
    write_to_file(df, flags["is_first_batch"], SHOULD_OUTPUT_CSV, SHOULD_OUTPUT_JSON)
    buffer.clear()
    flags["is_first_batch"] = False


if __name__ == "__main__":
    main()