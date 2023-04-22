# pixels-executor

This is the executor to be embedded in a process or serverless worker.
It is responsible for executing the relational operations, such as projection, filtering,
join, and aggregation, on a data segment (e.g., a split of the table to scan, or a subset of
the partitions in a partitioned hash join).