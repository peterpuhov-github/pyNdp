import ray
ray.init()


@ray.remote
def f(x):
    return x * x

@ray.remote
class Counter(object):
    def __init__(self):
        self.n = 0

    def increment(self):
        self.n += 1

    def read(self):
        return self.n


if __name__ == '__main__':
    futures = [f.remote(i) for i in range(4)]
    print(ray.get(futures))

    counters = [Counter.remote() for i in range(4)]
    [c.increment.remote() for c in counters]
    futures = [c.read.remote() for c in counters]
    print(ray.get(futures))