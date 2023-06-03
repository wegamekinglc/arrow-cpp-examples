import numpy as np
import pandas as pd
import pyarrow as pa
import polars as pl
import time
import sqlite3
import psutil
import os


def get_mem():
    return psutil.Process(os.getpid()).memory_info().rss >> 20


def generate_data(n: int, df_type="pandas"):
    sid_samples = [f"sid_{i}" for i in range(1, 1001)]
    ins_samples = [f"ins_{i}" for i in range(1, 2001)]
    bool_samples = [True, False]

    sids = np.random.choice(sid_samples, n)
    instruments = np.random.choice(ins_samples, n)
    d_values = np.random.randn(3, n)
    b_values = np.random.choice(bool_samples, n)

    orig_df = pd.DataFrame(
        dict(StretegyID=sids, InstrumentID=instruments, IndCode1=d_values[0], IndCode2=d_values[1], IndCode3=d_values[2], Flag=b_values)
    )
    schema = pa.schema([pa.field("StretegyID", pa.string()),
                        pa.field("InstrumentID", pa.string()),
                        pa.field("IndCode1", pa.float64()),
                        pa.field("IndCode2", pa.float64()),
                        pa.field("IndCode3", pa.float64()),
                        pa.field("Flag", pa.bool_())])
    
    if df_type == "pandas":
        return orig_df
    elif df_type == "arrow":
        return pa.Table.from_pandas(orig_df, schema=schema)
    elif df_type == "polars":
        return pl.from_arrow(pa.Table.from_pandas(orig_df, schema=schema))


def serialize_arrow(df: pa.Table, compression=False):
    sink = pa.BufferOutputStream()
    schema = df.schema
    if compression:
        options = pa.ipc.IpcWriteOptions(compression="lz4")
    else:
        options = None
    with pa.ipc.new_stream(sink, schema, options=options) as writer:
        writer.write_table(df)
    return sink.getvalue()


def deserialize_arrow(buff):
    with pa.ipc.open_stream(buff) as reader:
      return reader.read_all()
    

def create_db(db_name, rank):
    if rank == 0:
        import os
        if os.path.exists(db_name):
            os.unlink(db_name)
        db = sqlite3.connect(db_name)
        cur = db.cursor()
        cur.execute("create TABLE stats (event, round, rank, size, scale, time);")
        cur.execute("select 1;")
        db.commit()
        db.close()
    time.sleep(10)
    db = sqlite3.connect(db_name)
    cur = db.cursor()
    return cur, db

