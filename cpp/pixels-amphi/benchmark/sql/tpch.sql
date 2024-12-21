CREATE VIEW customer AS SELECT * FROM '/home/ubuntu/data/tpch/customer/*.parquet';
CREATE VIEW lineitem AS SELECT * FROM '/home/ubuntu/data/tpch/lineitem/*.parquet';
CREATE VIEW nation AS SELECT * FROM '/home/ubuntu/data/tpch/nation/*.parquet';
CREATE VIEW orders AS SELECT * FROM '/home/ubuntu/data/tpch/orders/*.parquet';
CREATE VIEW part AS SELECT * FROM '/home/ubuntu/data/tpch/part/*.parquet';
CREATE VIEW partsupp AS SELECT * FROM '/home/ubuntu/data/tpch/partsupp/*.parquet';
CREATE VIEW region AS SELECT * FROM '/home/ubuntu/data/tpch/region/*.parquet';
CREATE VIEW supplier AS SELECT * FROM '/home/ubuntu/data/tpch/supplier/*.parquet';