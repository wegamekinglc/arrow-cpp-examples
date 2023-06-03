if __name__ == "__main__":
    import datetime as dt
    import numpy as np
    import pyarrow as pa
    import pyarrow.flight

    client = pa.flight.connect("grpc://127.0.0.1:8815")

    # Upload a new dataset
    data_table = pa.table(
        [np.random.randn(1000000), np.random.randn(1000000)],
        names=["values1", "values2"]
    )
    upload_descriptor = pa.flight.FlightDescriptor.for_path("data.parquet")
    writer, _ = client.do_put(upload_descriptor, data_table.schema)
    writer.write_table(data_table)
    writer.close()

    # Retrieve metadata of newly uploaded dataset
    flight = client.get_flight_info(upload_descriptor)
    descriptor = flight.descriptor
    print("Path:", descriptor.path[0].decode('utf-8'), "Rows:", flight.total_records, "Size:", flight.total_bytes)
    print("=== Schema ===")
    print(flight.schema)
    print("==============")

    # Read content of the dataset
    now = dt.datetime.now()
    for i in range(100):
        reader = client.do_get(flight.endpoints[0].ticket)
        read_table = reader.read_all()
    print(dt.datetime.now() - now)

    _ = client.do_action(pa.flight.Action("drop_dataset", "uploaded.parquet".encode('utf-8')))

    # List existing datasets.
    for flight in client.list_flights():
        descriptor = flight.descriptor
        print("Path:", descriptor.path[0].decode('utf-8'), "Rows:", flight.total_records, "Size:", flight.total_bytes)
        print("=== Schema ===")
        print(flight.schema)
        print("==============")
        print("")
