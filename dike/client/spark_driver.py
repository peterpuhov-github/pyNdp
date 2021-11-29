import pickle
import dike.client.spark_driver
import dike.client.tpch

from pyspark.serializers import write_with_length, write_int, read_long, read_bool, \
    write_long, read_int, SpecialLengths, UTF8Deserializer, PickleSerializer, \
    BatchedSerializer

def ndp_reader(split_index, iterator):
    fname = '/tpch-test-parquet-1g/lineitem.parquet/' \
            'part-00000-badcef81-d816-44c1-b936-db91dae4c15f-c000.snappy.parquet'

    q14 = dike.client.tpch.TpchQ14(fname, 0)
    return q14


class DeSerializer:
    def load_stream(self, infile=None):
        pass


class Serializer:
    def dump_stream(self, out_iter, outfile):
        out_iter.to_spark(outfile)


def create_spark_worker_command():
    func = ndp_reader
    deser = DeSerializer()
    ser = Serializer()

    command = (func, None, deser, ser)  # Format should be (func, profiler, deserializer, serializer)
    pickle_protocol = pickle.HIGHEST_PROTOCOL
    return pickle.dumps(command, pickle_protocol)


if __name__ == '__main__':
    command = dike.client.spark_driver.create_spark_worker_command()
    print(command.hex())