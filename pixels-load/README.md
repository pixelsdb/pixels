# pixels-load

This is the command-line tool for macro-benchmark evaluations (e.g., TPC-H).
We can use it to load data from csv files into Pixels, copy the data, compact the small files, 
and run the benchmark queries.

It is named pixels-load as its earliest functionality was to load data for the evaluations.
Note that this is only the batch data loading. Streaming and real-time loading will be supported
by a new module in the future.

