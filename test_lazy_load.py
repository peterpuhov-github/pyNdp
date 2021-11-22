import functools
import pandas as pd
import numpy as np


class DikeDataFrame:
    def __init__(self, *args, **kwargs):
        self.pdf = pd.DataFrame(*args, **kwargs)
        self.chain = []

    def _decorator(self, f, item):
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            self.chain.append((item, args, kwargs))
            return f(*args, **kwargs)

        return wrapper

    def __getattr__(self, item):
        # TODO check if item is column name
        value = object.__getattribute__(self, 'wrapper')
        decorator = object.__getattribute__(self, '_decorator')
        return decorator(value, item)

    def wrapper(self, *args, **kwargs):
        return self

    def _compute(self):
        expr = ''
        for f in self.chain:
            expr += 'pdf.'
            expr += f'{f[0]}('
            for arg in f[1]:
                if isinstance(arg, str):
                    expr += f'\'{arg}\','
                else:
                    expr += f'{arg},'
            for kwarg in f[2]:
                expr += f'{kwarg[0]}={kwarg[1]},'
            expr += f')\n'

        print('Creating Logical plan for ...')
        print(expr)
        print('...')
        exec(expr, {'pdf': self.pdf})

    def head(self, count):
        self._compute()
        return self.pdf.head(count)


if __name__ == '__main__':
    df = DikeDataFrame({'A': [1, 1, 2, 2],
                       'B': [1, 2, 3, 4],
                        'C': np.random.randn(4)})

    df = df.groupby('A').agg(['min', 'max'])
    print(df.head(3))
