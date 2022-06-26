import logging as log

from pyarrow import csv
from pyarrow import parquet as pq


def convert_parquet(inp_file, out_file):
    table = csv.read_csv(inp_file)
    pq.write_table(table, out_file)
    log.info(f"Parquet written to {out_file}")


def convert_spark_df(inp_df):
    data_lst = []
    for i in inp_df.collect():
        data_lst.append(tuple(i))
    return data_lst
