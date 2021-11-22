import sys
import pickle


def ndp_reader(split_index, iterator):
    output = 'Hello!'
    return output


class DeSerializer:
    def load_stream(self, infile=None):
        pass

class Serializer:
    def dump_stream(self, out_iter, outfile):
        #  outfile.write(out_iter.encode())
        print(out_iter)
        outfile.write(len(out_iter))
        outfile.write(out_iter.encode("utf-8"))


def create_spark_worker_command():
    #   Format should be ( func, profiler, deserializer, serializer)
    command = (ndp_reader, None, DeSerializer(), Serializer())
    pickle_protocol = pickle.HIGHEST_PROTOCOL
    return pickle.dumps(command, pickle_protocol)

if __name__ == '__main__':
    command = create_spark_worker_command()
    print(command)
    print(list(command), '\n', len(command))
    func, profiler, deserializer, serializer = pickle.loads(command, encoding='bytes')
    iterator = deserializer.load_stream(None)
    out_iter = func(0, iterator)


    class Writer:
        def write(self, data):
            print(f'{data}')

    serializer.dump_stream(out_iter, Writer())
