import pyarrow as pa
from arrow_ex.utils import generate_data, get_mem


if __name__ == "__main__":
    df = generate_data(5000000, df_type="arrow")

    print(f"# 1 memory: {get_mem()}")
    with pa.OSFile('bigfile.arrow', 'wb') as sink:
        with pa.ipc.new_file(sink, df.schema) as writer:
            writer.write_table(df)

    print(f"# 2 memory: {get_mem()}")
    with pa.OSFile('bigfile.arrow', 'rb') as source:
        with pa.ipc.open_file(source) as reader:
            df2 = reader.read_all()

    print(f"# 3 memory: {get_mem()}")

    with pa.memory_map('bigfile.arrow', 'rb') as source:
        with pa.ipc.open_file(source) as reader:
            df3 = reader.read_all()

    print(f"# 4 memory: {get_mem()}")

    with pa.OSFile('bigfile2.arrow', 'wb') as sink:
        with pa.ipc.new_file(sink, df.schema, options=pa.ipc.IpcWriteOptions(compression="lz4")) as writer:
            writer.write_table(df)

    print(f"# 5 memory: {get_mem()}")
    with pa.OSFile('bigfile2.arrow', 'rb') as source:
        with pa.ipc.open_file(source) as reader:
            df4 = reader.read_all()

    print(f"# 6 memory: {get_mem()}")

    with pa.memory_map('bigfile2.arrow', 'rb') as source:
        with pa.ipc.open_file(source) as reader:
            df5 = reader.read_all()

    print(f"# 7 memory: {get_mem()}")


