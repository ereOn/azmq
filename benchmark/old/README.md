Benchmark
=========

This folder contains the script necessary to generate benchmarks and graphs.

Installation
------------

To generate the graph, you'll need the `plotly` and `numpy` python libraries
and an online account for `plotly`.

Running the benchmark
---------------------

1. First run `python benchmark-server.py tcp://0.0.0.0:<port>` on one or
   several servers.
2. Then, on the benchmark machine, run `python benchmark-client.py
   tcp://<server-host>:<server-port>...`.
3. Wait. Once it is over, the webpage with the graph will show in your default
   browser.
