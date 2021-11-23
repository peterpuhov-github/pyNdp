import pickle
import dike.client.spark_driver

from pyspark.serializers import write_with_length, write_int, read_long, read_bool, \
    write_long, read_int, SpecialLengths, UTF8Deserializer, PickleSerializer, \
    BatchedSerializer

def ndp_reader(split_index, iterator):
    output = 'Hello from Spark NDP reader!'
    return output


class DeSerializer:
    def load_stream(self, infile=None):
        pass


class Serializer:
    def dump_stream(self, out_iter, outfile):
        write_with_length(out_iter.encode("utf-8"), outfile)


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