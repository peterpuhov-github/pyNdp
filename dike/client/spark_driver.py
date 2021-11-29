import pickle
import json
import dike.client.spark_driver
import dike.client.tpch

from pyspark.serializers import write_with_length, write_int, read_long, read_bool, \
    write_long, read_int, SpecialLengths, UTF8Deserializer, PickleSerializer, \
    BatchedSerializer


def ndp_reader(split_index, dag_json):
    dag = json.loads(dag_json)
    print(dag['NodeArray'])
    input_node = [n for n in dag['NodeArray'] if n['Type'] =='_INPUT'][0]

    q14 = dike.client.tpch.TpchQ14(input_node['File'], int(input_node['RowGroup']))

    return q14

utf8_deserializer = UTF8Deserializer()

class DeSerializer:
    def load_stream(self, infile):
        dag = utf8_deserializer.loads(infile)
        return dag


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