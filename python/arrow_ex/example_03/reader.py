import time
import pyarrow as pa
from arrow_ex.utils import generate_data, get_mem

if __name__ == "__main__":
    df = generate_data(50, df_type="arrow")
    df = df.add_column(0, "round", [[0.0] * len(df)])

    with pa.OSFile('bigfile.arrow', 'rb') as source:
        i = 0.0
        with pa.ipc.open_stream(source) as reader:
            while True:
                try:
                    df = reader.read_next_batch()
                except StopIteration:
                    time.sleep(1.0)
                    continue
                print(df.to_pandas())