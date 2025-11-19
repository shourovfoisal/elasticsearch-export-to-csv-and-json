from elasticsearch.helpers import scan
import pandas as pd
import os
from config import (
    es_client,
    index_name,
    expected_columns,
    query,
    CSV_OUTPUT_DIR,
    JSON_OUTPUT_DIR,
    SHOULD_OUTPUT_CSV,
    SHOULD_OUTPUT_JSON
)

csv_export_path=""
json_export_path=""

def main():
    create_output_directories()
    
    flags = {"is_first_batch": True}
    output_chunk_size = 5000
    buffer = []
    
    all_columns: set[str] = set()
    count: int = 0
    for doc in read_the_index():
        if count % 1000 == 0:
            print(f"Collecting columns {count}")
        all_columns.update(doc["_source"].keys())
        count+=1
        
    final_list_of_columns: list[str] = list(all_columns)
    
    count = 0
    for doc in read_the_index():
        if count % 1000 == 0:
            print(f"Collecting data {count}")
        count+=1
            
        source = flatten_arrays(doc["_source"])
        buffer.append(source)
        
        # Write in chunks
        if len(buffer) >= output_chunk_size:
            print(f"Writing data {count}")
            produce_output(buffer=buffer, flags=flags, all_columns=final_list_of_columns)
    
    if buffer: produce_output(buffer=buffer, flags=flags, all_columns=final_list_of_columns)    # Write remaining

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
def flatten_arrays(obj):
    for k, v in obj.items():
        if isinstance(v, list):
            obj[k] = ", ".join(map(str, v))
    return obj

# This method ensures that
# The DataFrame has exactly the columns you want
# In the order you want
# If a column name from the columns list doesn't exist
# It still gets included in the output, with all values set to NaN
def rearrange_columns(df, all_columns):
    return (
        df.reindex(columns=expected_columns)
            if len(expected_columns) > 0
            else df.reindex(columns=all_columns)
    )
    

def write_to_file(df, is_first_batch, should_output_csv = True, should_output_json = True):
    if is_first_batch:
        if should_output_csv: df.to_csv(csv_export_path, index=False, mode="w")
        if should_output_json: df.to_json(json_export_path, orient="records", lines=True, mode="w")
    else:
        if should_output_csv: df.to_csv(csv_export_path, index=False, mode="a", header=False)
        if should_output_json: df.to_json(json_export_path, orient="records", lines=True, mode="a")


def produce_output(buffer, flags, all_columns):
    df = rearrange_columns(df=pd.DataFrame(buffer), all_columns=all_columns)
    write_to_file(df, flags["is_first_batch"], SHOULD_OUTPUT_CSV, SHOULD_OUTPUT_JSON)
    buffer.clear()
    flags["is_first_batch"] = False


if __name__ == "__main__":
    main()