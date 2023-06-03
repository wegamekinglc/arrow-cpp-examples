import time
import pyarrow as pa
from arrow_ex.utils import generate_data, get_mem

if __name__ == "__main__":
    df = generate_data(50, df_type="arrow")
    df = df.add_column(0, "round", [[0.0] * len(df)])

    with pa.OSFile('bigfile.arrow', 'wb') as sink:
        i = 0.0
        with pa.ipc.new_stream(sink, df.schema, options=pa.ipc.IpcWriteOptions(compression="lz4")) as writer:
            while True:
                writer.write_table(df)
                i += 1.0
                df = df.set_column(0, "round", [[i] * len(df)])
                sink.flush()
                time.sleep(1.0)
