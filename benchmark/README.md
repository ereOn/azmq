Benchmark
=========

This folder contains scripts that allow to benchmark **AZMQ**.

Installation
------------

Just do:

    pip install -r requirements.txt

To install the prerequisites.

Running
-------

You can either run the benchmark on the same computer or on two computers over
a network.

1. Launch the broker, with the following command:

    python benchmark.py broker

You may get more information about the supported parameters by adding `--help`
at the end.

2. Launch the client, with the following command:

    python benchmark.py client

Again, you may get more information about the supported parameters by adding
`--help` at the end.

The client outputs raw results for each message sent on the standard output.
You may redirect those to a file for later processing (append `> results.txt`
to the command), or pipe the program directly into the analyse script to get an
immediate summary:

    python benchmark.py client | python analyse.py -q

Analysing
---------

Here is a recap of the different metrics:

Metric | Meaning
------ | -------
average_latency | The average time from the sending of a message to its reception.
median_latency | The median time from the sending of a message to its reception.
95th_percentile_latency | The 95th percentile time from the sending of a message to its reception.
receiving_first | The timestamp at which the first message was received.
receiving_last | The timestamp at which the last message was received.
receiving_overhead | The average time between the reception of two messages.
receiving_throughput | The number of messages received over a second.
count | The count of messages sent.
sending_first | The timestamp at which the first message was sent.
sending_last | The timestamp at which the last message was sent.
sending_overhead | The average time between the sending of two messages.
sending_throughput | The number of messages sent over a second.
