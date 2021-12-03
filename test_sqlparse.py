import sqlparse


if __name__ == '__main__':
    query = "SELECT l_partkey, l_extendedprice, l_discount, l_shipdate FROM arrow WHERE l_shipdate >= '1995-09-01' AND l_shipdate < '1995-10-01'"
    res = sqlparse.parse(query)[0]
    # print(type(res), res)
    # for t in res.tokens:
    #     print(type(t), t)

    flat = res.flatten()
    # print(type(flat), flat)
    columns = set()
    for t in flat:
        # print(t.ttype)
        if t.ttype in [ sqlparse.tokens.Token.Name]:  # sqlparse.tokens.Token.Literal.String.Single
            # print(t.value)
            columns.add(t.value)

    print(columns)

