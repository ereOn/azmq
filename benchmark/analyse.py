"""
Analyse benchmark results.
"""

import os
import ujson as json
import sys
import numpy

from mrjob.job import MRJob
from mrjob.protocol import JSONProtocol


def minmax(values):
    mi = ma = next(values)
    count = 1

    for v in values:
        count += 1

        if v < mi:
            mi = v
        elif v > ma:
            ma = v

    return mi, ma, count


class SummaryJob(MRJob):
    INPUT_PROTOCOL = JSONProtocol
    OUTPUT_PROTOCOL = JSONProtocol

    def mapper(self, key, value):
        yield 'latencies', value['latency']
        yield 'sent', value['sent']
        yield 'received', value['received']
        yield 'size', value['size']

    def reducer(self, key, values):
        if key == 'latencies':
            values = list(values)
            yield 'average_latency', numpy.average(values)
            yield 'median_latency', numpy.median(values)
            yield '95th_percentile_latency', numpy.percentile(values, 95)
        elif key == 'sent':
            first, last, count = minmax(values)
            yield 'count', count
            yield 'sending_first', first
            yield 'sending_last', last
            yield 'sending_overhead', (last - first) / (count - 1)
            yield 'sending_throughput', (count - 1) / (last - first)
        elif key == 'received':
            first, last, count = minmax(values)
            yield 'receiving_first', first
            yield 'receiving_last', last
            yield 'receiving_overhead', (last - first) / (count - 1)
            yield 'receiving_throughput', (count - 1) / (last - first)


if __name__ == '__main__':
    if hasattr(os, 'symlink'):
        delattr(os, 'symlink')

    SummaryJob.run()
