from pyspark import SparkContext, SparkConf
import sys

N = 4847571

def mapper(from_, rank, to):
    yield (from_, 0)
    if len(to) > 0:
        to_rank = rank / len(to)
        for to_link in to:
            yield (to_link, to_rank)


def reducer(from_, to_ranks, hang_pr, alpha):
    global N
    rank = 0
    for r in to_ranks:
        rank += r
    rank = (1.0 - alpha) / N + alpha * (rank + hang_pr / N)
    return (from_, rank)


def main(input, output, alpha, iters):
    SparkContext.setSystemProperty('spark.executor.memory', '3g')
    conf = SparkConf().setAppName("SparkPageRank")
    sc = SparkContext(conf=conf)

    lines = sc.textFile(input).filter(lambda x: len(x) and x[0] != '#')
    vertexes = lines.flatMap(lambda x: map(int, x.strip().split())) \
                    .distinct().map(lambda x: [x, None])

    edges = lines.map(lambda x: map(int, x.strip().split())).groupByKey()

    t = vertexes.leftOuterJoin(edges).map(lambda x: (x[0], x[1][1]))

    pr = vertexes.map(lambda x: (x[0], 1 / N))
    hang_pr = t.join(pr).filter(lambda x: x[1][0] is None).map(lambda x: x[1][1]).sum()

    for i in range(iters):
        emits = t.join(pr).flatMap(lambda x: mapper(x[0], x[1][1], x[1][0]))
        pr = emits.groupByKey().map(lambda x: reducer(x[0], x[1], hang_pr, alpha)).cache()
        hang_pr = t.join(pr).filter(lambda x: x[1][0] is None).map(lambda x: x[1][1]).sum()

    pr = pr.sortBy(lambda x: x[1], False)
    pr.saveAsTextFile(output)


if __name__ =='__main__':
    input = sys.argv[1]
    output = sys.argv[2]
    alpha = float(sys.argv[3])
    iters = int(sys.argv[4])

    main(input, output, alpha, iters)
