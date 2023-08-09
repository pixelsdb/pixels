Pixels Correctness Experiment

# Introduction

In this page, we validate the correctness of pixels reader. We test our correctness on TPCH 1 and TPCH 300.

For TPCH 1, we have the standard query result answer. For TPCH 300, we compare the result with parquet. 

# TPCH 1 validation

Switch to duckdb work directory and run the command

pixels:
```
build/release/benchmark/benchmark_runner "benchmark/tpch/pixels/tpch_1_small_endian/.*"
```

pixels encoding:
```
build/release/benchmark/benchmark_runner "benchmark/tpch/pixels/tpch_1_encoding/.*"
```

This test would automatically compare the result of duckdb + pixels with the correct answer. 

# TPCH 300 validation

Switch to duckdb work directory and run
```
./build/release/duckdb
```

The command and result:

pixels create view

```
CREATE VIEW lineitem AS SELECT * FROM '/data/s1725-1/liyu/pixels_data/pixels-tpch-300-small-endian/lineitem/v-0-order/*.pxl';
CREATE VIEW nation AS SELECT * FROM '/data/s1725-1/liyu/pixels_data/pixels-tpch-300-small-endian/nation/v-0-order/*.pxl';
CREATE VIEW region AS SELECT * FROM '/data/s1725-1/liyu/pixels_data/pixels-tpch-300-small-endian/region/v-0-order/*.pxl';
CREATE VIEW supplier AS SELECT * FROM '/data/s1725-1/liyu/pixels_data/pixels-tpch-300-small-endian/supplier/v-0-order/*.pxl';
CREATE VIEW customer AS SELECT * FROM '/data/s1725-1/liyu/pixels_data/pixels-tpch-300-small-endian/customer/v-0-order/*.pxl';
CREATE VIEW part AS SELECT * FROM '/data/s1725-1/liyu/pixels_data/pixels-tpch-300-small-endian/part/v-0-order/*.pxl';
CREATE VIEW partsupp AS SELECT * FROM '/data/s1725-1/liyu/pixels_data/pixels-tpch-300-small-endian/partsupp/v-0-order/*.pxl';
CREATE VIEW orders AS SELECT * FROM '/data/s1725-1/liyu/pixels_data/pixels-tpch-300-small-endian/orders/v-0-order/*.pxl';
```

pixels encoding create view

```
CREATE VIEW lineitem AS SELECT * FROM '/data/s1725-1/liyu/pixels_data/pixels-tpch-300-encoding-small-endian/lineitem/v-0-order/*.pxl';
CREATE VIEW nation AS SELECT * FROM '/data/s1725-1/liyu/pixels_data/pixels-tpch-300-encoding-small-endian/nation/v-0-order/*.pxl';
CREATE VIEW region AS SELECT * FROM '/data/s1725-1/liyu/pixels_data/pixels-tpch-300-encoding-small-endian/region/v-0-order/*.pxl';
CREATE VIEW supplier AS SELECT * FROM '/data/s1725-1/liyu/pixels_data/pixels-tpch-300-encoding-small-endian/supplier/v-0-order/*.pxl';
CREATE VIEW customer AS SELECT * FROM '/data/s1725-1/liyu/pixels_data/pixels-tpch-300-encoding-small-endian/customer/v-0-order/*.pxl';
CREATE VIEW part AS SELECT * FROM '/data/s1725-1/liyu/pixels_data/pixels-tpch-300-encoding-small-endian/part/v-0-order/*.pxl';
CREATE VIEW partsupp AS SELECT * FROM '/data/s1725-1/liyu/pixels_data/pixels-tpch-300-encoding-small-endian/partsupp/v-0-order/*.pxl';
CREATE VIEW orders AS SELECT * FROM '/data/s1725-1/liyu/pixels_data/pixels-tpch-300-encoding-small-endian/orders/v-0-order/*.pxl';
```


parquet create view
```
CREATE VIEW lineitem AS SELECT * FROM parquet_scan('/data/s1725-2/liyu/parquet-tpch-300g/lineitem/*');
CREATE VIEW nation AS SELECT * FROM parquet_scan('/data/s1725-2/liyu/parquet-tpch-300g/nation/*');
CREATE VIEW region AS SELECT * FROM parquet_scan('/data/s1725-2/liyu/parquet-tpch-300g/region/*');
CREATE VIEW supplier AS SELECT * FROM parquet_scan('/data/s1725-2/liyu/parquet-tpch-300g/supplier/*');
CREATE VIEW customer AS SELECT * FROM parquet_scan('/data/s1725-2/liyu/parquet-tpch-300g/customer/*');
CREATE VIEW part AS SELECT * FROM parquet_scan('/data/s1725-2/liyu/parquet-tpch-300g/part/*');
CREATE VIEW partsupp AS SELECT * FROM parquet_scan('/data/s1725-2/liyu/parquet-tpch-300g/partsupp/*');
CREATE VIEW orders AS SELECT * FROM parquet_scan('/data/s1725-2/liyu/parquet-tpch-300g/orders/*');
```

tpch q01:
```
SELECT
    l_returnflag,
    l_linestatus,
    sum(l_quantity) AS sum_qty,
    sum(l_extendedprice) AS sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
    avg(l_quantity) AS avg_qty,
    avg(l_extendedprice) AS avg_price,
    avg(l_discount) AS avg_disc,
    count(*) AS count_order
FROM
    lineitem
WHERE
    l_shipdate <= CAST('1998-09-02' AS date)
GROUP BY
    l_returnflag,
    l_linestatus
ORDER BY
    l_returnflag,
    l_linestatus;

```

pixels: 

```
┌──────────────┬──────────────┬────────────────┬───┬────────────────────┬─────────────────────┬─────────────┐
│ l_returnflag │ l_linestatus │    sum_qty     │ … │     avg_price      │      avg_disc       │ count_order │
│   varchar    │   varchar    │ decimal(38,2)  │   │       double       │       double        │    int64    │
├──────────────┼──────────────┼────────────────┼───┼────────────────────┼─────────────────────┼─────────────┤
│ A            │ F            │ 11326496070.00 │ … │  38236.50044804907 │ 0.05000206283958664 │   444178988 │
│ N            │ F            │   295553717.00 │ … │ 38240.985309890144 │  0.0500091127792277 │    11590317 │
│ N            │ O            │ 22305732548.00 │ … │  38237.47675056649 │ 0.04999911717914543 │   874730129 │
│ R            │ F            │ 11326615383.00 │ … │  38236.91884705748 │ 0.05000013577927881 │   444176759 │
├──────────────┴──────────────┴────────────────┴───┴────────────────────┴─────────────────────┴─────────────┤
│ 4 rows                                                                               10 columns (6 shown) │
└───────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

pixels encoding:

```
┌──────────────┬──────────────┬────────────────┬───┬────────────────────┬─────────────────────┬─────────────┐
│ l_returnflag │ l_linestatus │    sum_qty     │ … │     avg_price      │      avg_disc       │ count_order │
│   varchar    │   varchar    │ decimal(38,2)  │   │       double       │       double        │    int64    │
├──────────────┼──────────────┼────────────────┼───┼────────────────────┼─────────────────────┼─────────────┤
│ A            │ F            │ 11326496070.00 │ … │  38236.50044804907 │ 0.05000206283958664 │   444178988 │
│ N            │ F            │   295553717.00 │ … │ 38240.985309890144 │  0.0500091127792277 │    11590317 │
│ N            │ O            │ 22305732548.00 │ … │  38237.47675056649 │ 0.04999911717914543 │   874730129 │
│ R            │ F            │ 11326615383.00 │ … │  38236.91884705748 │ 0.05000013577927881 │   444176759 │
├──────────────┴──────────────┴────────────────┴───┴────────────────────┴─────────────────────┴─────────────┤
│ 4 rows                                                                               10 columns (6 shown) │
└───────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

parquet:

```
┌──────────────┬──────────────┬────────────────┬───┬────────────────────┬─────────────────────┬─────────────┐
│ l_returnflag │ l_linestatus │    sum_qty     │ … │     avg_price      │      avg_disc       │ count_order │
│   varchar    │   varchar    │ decimal(38,2)  │   │       double       │       double        │    int64    │
├──────────────┼──────────────┼────────────────┼───┼────────────────────┼─────────────────────┼─────────────┤
│ A            │ F            │ 11326496070.00 │ … │  38236.50044804907 │ 0.05000206283958664 │   444178988 │
│ N            │ F            │   295553717.00 │ … │ 38240.985309890144 │  0.0500091127792277 │    11590317 │
│ N            │ O            │ 22305732548.00 │ … │  38237.47675056649 │ 0.04999911717914543 │   874730129 │
│ R            │ F            │ 11326615383.00 │ … │  38236.91884705748 │ 0.05000013577927881 │   444176759 │
├──────────────┴──────────────┴────────────────┴───┴────────────────────┴─────────────────────┴─────────────┤
│ 4 rows                                                                               10 columns (6 shown) │
└───────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

tpch q02

```
SELECT
    s_acctbal,
    s_name,
    n_name,
    p_partkey,
    p_mfgr,
    s_address,
    s_phone,
    s_comment
FROM
    part,
    supplier,
    partsupp,
    nation,
    region
WHERE
    p_partkey = ps_partkey
    AND s_suppkey = ps_suppkey
    AND p_size = 15
    AND p_type LIKE '%BRASS'
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'EUROPE'
    AND ps_supplycost = (
        SELECT
            min(ps_supplycost)
        FROM
            partsupp,
            supplier,
            nation,
            region
        WHERE
            p_partkey = ps_partkey
            AND s_suppkey = ps_suppkey
            AND s_nationkey = n_nationkey
            AND n_regionkey = r_regionkey
            AND r_name = 'EUROPE')
ORDER BY
    s_acctbal DESC,
    n_name,
    s_name,
    p_partkey
LIMIT 100;

```


pixels:
```
┌───────────────┬────────────────────┬────────────────┬───┬─────────────────┬──────────────────────┐
│   s_acctbal   │       s_name       │     n_name     │ … │     s_phone     │      s_comment       │
│ decimal(15,2) │      varchar       │    varchar     │   │     varchar     │       varchar        │
├───────────────┼────────────────────┼────────────────┼───┼─────────────────┼──────────────────────┤
│       9999.94 │ Supplier#002307508 │ GERMANY        │ … │ 17-503-112-5653 │ nag carefully. bli…  │
│       9999.79 │ Supplier#000712808 │ UNITED KINGDOM │ … │ 33-559-792-5549 │ uld have to use ca…  │
│       9999.77 │ Supplier#002254341 │ GERMANY        │ … │ 17-391-701-5367 │ uctions. blithely …  │
│       9999.75 │ Supplier#001017723 │ FRANCE         │ … │ 16-717-704-2066 │ ven instructions w…  │
│       9999.72 │ Supplier#001460104 │ UNITED KINGDOM │ … │ 33-408-531-3270 │ the carefully fina…  │
│       9999.70 │ Supplier#000836396 │ ROMANIA        │ … │ 29-579-143-3270 │ c accounts. blithe…  │
│       9999.70 │ Supplier#000239544 │ UNITED KINGDOM │ … │ 33-509-584-9496 │ ets are. blithely …  │
│       9999.67 │ Supplier#002003789 │ GERMANY        │ … │ 17-621-582-3046 │ y among the carefu…  │
│       9999.64 │ Supplier#002094356 │ ROMANIA        │ … │ 29-581-506-9539 │ ickly pending inst…  │
│       9999.36 │ Supplier#002937179 │ FRANCE         │ … │ 16-525-981-5086 │ e. slylyCustomer a…  │
│       9999.27 │ Supplier#002093565 │ GERMANY        │ … │ 17-644-158-9607 │ bove the blithely …  │
│       9999.21 │ Supplier#000241339 │ ROMANIA        │ … │ 29-796-589-8064 │ ounts are. unusual…  │
│       9998.98 │ Supplier#001266482 │ RUSSIA         │ … │ 32-583-820-1183 │ final, bold ideas …  │
│       9998.93 │ Supplier#002807981 │ RUSSIA         │ … │ 32-938-325-4297 │ p blithely after t…  │
│       9998.84 │ Supplier#000771025 │ UNITED KINGDOM │ … │ 33-208-199-6904 │  blithely express …  │
│       9998.78 │ Supplier#001171577 │ ROMANIA        │ … │ 29-280-166-8362 │ fily even accounts…  │
│       9998.77 │ Supplier#001583206 │ GERMANY        │ … │ 17-338-279-6817 │ egular theodolites…  │
│       9998.73 │ Supplier#002392929 │ FRANCE         │ … │ 16-846-156-5209 │ o beans are furiou…  │
│       9998.73 │ Supplier#002028638 │ RUSSIA         │ … │ 32-179-161-3169 │ ckages haggle furi…  │
│       9998.59 │ Supplier#000352619 │ GERMANY        │ … │ 17-705-323-4485 │ ctions along the r…  │
│          ·    │         ·          │    ·           │ · │        ·        │          ·           │
│          ·    │         ·          │    ·           │ · │        ·        │          ·           │
│          ·    │         ·          │    ·           │ · │        ·        │          ·           │
│       9992.70 │ Supplier#000245718 │ UNITED KINGDOM │ … │ 33-570-729-4176 │ carefully above th…  │
│       9992.66 │ Supplier#000541492 │ ROMANIA        │ … │ 29-172-548-6729 │ heodolites run fin…  │
│       9992.66 │ Supplier#002666275 │ UNITED KINGDOM │ … │ 33-232-137-3532 │ ording to the care…  │
│       9992.55 │ Supplier#002129389 │ GERMANY        │ … │ 17-758-434-2104 │ silent sheaves aga…  │
│       9992.54 │ Supplier#000099650 │ RUSSIA         │ … │ 32-971-481-2533 │ ged deposits cajol…  │
│       9992.41 │ Supplier#000853625 │ UNITED KINGDOM │ … │ 33-590-481-6043 │ ously across the b…  │
│       9992.40 │ Supplier#002540732 │ RUSSIA         │ … │ 32-855-640-9841 │ s cajole blithely …  │
│       9992.38 │ Supplier#002188434 │ FRANCE         │ … │ 16-811-300-2036 │ ular pinto beans. …  │
│       9992.27 │ Supplier#002149369 │ FRANCE         │ … │ 16-997-215-3908 │ h slyly after the …  │
│       9992.22 │ Supplier#002229635 │ ROMANIA        │ … │ 29-117-494-5343 │ unusual somas are …  │
│       9992.21 │ Supplier#001243459 │ ROMANIA        │ … │ 29-438-676-5070 │  ruthless deposits…  │
│       9992.18 │ Supplier#001499787 │ RUSSIA         │ … │ 32-981-276-2675 │ courts cajole slyl…  │
│       9992.14 │ Supplier#001551485 │ GERMANY        │ … │ 17-750-694-3182 │ lets. slyly specia…  │
│       9992.04 │ Supplier#001520263 │ RUSSIA         │ … │ 32-676-769-1115 │ ough the final, re…  │
│       9992.03 │ Supplier#000047868 │ FRANCE         │ … │ 16-814-219-3643 │ y. special account…  │
│       9992.00 │ Supplier#002889101 │ FRANCE         │ … │ 16-175-720-5603 │ ously ironic pinto…  │
│       9992.00 │ Supplier#002889101 │ FRANCE         │ … │ 16-175-720-5603 │ ously ironic pinto…  │
│       9991.98 │ Supplier#002500373 │ GERMANY        │ … │ 17-464-745-1571 │ . express, ironic …  │
│       9991.84 │ Supplier#000462341 │ UNITED KINGDOM │ … │ 33-607-871-1127 │  carefully bold th…  │
│       9991.57 │ Supplier#000991769 │ GERMANY        │ … │ 17-187-501-4064 │  regular asymptote…  │
├───────────────┴────────────────────┴────────────────┴───┴─────────────────┴──────────────────────┤
│ 100 rows (40 shown)                                                          8 columns (5 shown) │
└──────────────────────────────────────────────────────────────────────────────────────────────────┘
```

pixels encoding:
```
┌───────────────┬────────────────────┬────────────────┬───┬─────────────────┬──────────────────────┐
│   s_acctbal   │       s_name       │     n_name     │ … │     s_phone     │      s_comment       │
│ decimal(15,2) │      varchar       │    varchar     │   │     varchar     │       varchar        │
├───────────────┼────────────────────┼────────────────┼───┼─────────────────┼──────────────────────┤
│       9999.94 │ Supplier#002307508 │ GERMANY        │ … │ 17-503-112-5653 │ nag carefully. bli…  │
│       9999.79 │ Supplier#000712808 │ UNITED KINGDOM │ … │ 33-559-792-5549 │ uld have to use ca…  │
│       9999.77 │ Supplier#002254341 │ GERMANY        │ … │ 17-391-701-5367 │ uctions. blithely …  │
│       9999.75 │ Supplier#001017723 │ FRANCE         │ … │ 16-717-704-2066 │ ven instructions w…  │
│       9999.72 │ Supplier#001460104 │ UNITED KINGDOM │ … │ 33-408-531-3270 │ the carefully fina…  │
│       9999.70 │ Supplier#000836396 │ ROMANIA        │ … │ 29-579-143-3270 │ c accounts. blithe…  │
│       9999.70 │ Supplier#000239544 │ UNITED KINGDOM │ … │ 33-509-584-9496 │ ets are. blithely …  │
│       9999.67 │ Supplier#002003789 │ GERMANY        │ … │ 17-621-582-3046 │ y among the carefu…  │
│       9999.64 │ Supplier#002094356 │ ROMANIA        │ … │ 29-581-506-9539 │ ickly pending inst…  │
│       9999.36 │ Supplier#002937179 │ FRANCE         │ … │ 16-525-981-5086 │ e. slylyCustomer a…  │
│       9999.27 │ Supplier#002093565 │ GERMANY        │ … │ 17-644-158-9607 │ bove the blithely …  │
│       9999.21 │ Supplier#000241339 │ ROMANIA        │ … │ 29-796-589-8064 │ ounts are. unusual…  │
│       9998.98 │ Supplier#001266482 │ RUSSIA         │ … │ 32-583-820-1183 │ final, bold ideas …  │
│       9998.93 │ Supplier#002807981 │ RUSSIA         │ … │ 32-938-325-4297 │ p blithely after t…  │
│       9998.84 │ Supplier#000771025 │ UNITED KINGDOM │ … │ 33-208-199-6904 │  blithely express …  │
│       9998.78 │ Supplier#001171577 │ ROMANIA        │ … │ 29-280-166-8362 │ fily even accounts…  │
│       9998.77 │ Supplier#001583206 │ GERMANY        │ … │ 17-338-279-6817 │ egular theodolites…  │
│       9998.73 │ Supplier#002392929 │ FRANCE         │ … │ 16-846-156-5209 │ o beans are furiou…  │
│       9998.73 │ Supplier#002028638 │ RUSSIA         │ … │ 32-179-161-3169 │ ckages haggle furi…  │
│       9998.59 │ Supplier#000352619 │ GERMANY        │ … │ 17-705-323-4485 │ ctions along the r…  │
│          ·    │         ·          │    ·           │ · │        ·        │          ·           │
│          ·    │         ·          │    ·           │ · │        ·        │          ·           │
│          ·    │         ·          │    ·           │ · │        ·        │          ·           │
│       9992.70 │ Supplier#000245718 │ UNITED KINGDOM │ … │ 33-570-729-4176 │ carefully above th…  │
│       9992.66 │ Supplier#000541492 │ ROMANIA        │ … │ 29-172-548-6729 │ heodolites run fin…  │
│       9992.66 │ Supplier#002666275 │ UNITED KINGDOM │ … │ 33-232-137-3532 │ ording to the care…  │
│       9992.55 │ Supplier#002129389 │ GERMANY        │ … │ 17-758-434-2104 │ silent sheaves aga…  │
│       9992.54 │ Supplier#000099650 │ RUSSIA         │ … │ 32-971-481-2533 │ ged deposits cajol…  │
│       9992.41 │ Supplier#000853625 │ UNITED KINGDOM │ … │ 33-590-481-6043 │ ously across the b…  │
│       9992.40 │ Supplier#002540732 │ RUSSIA         │ … │ 32-855-640-9841 │ s cajole blithely …  │
│       9992.38 │ Supplier#002188434 │ FRANCE         │ … │ 16-811-300-2036 │ ular pinto beans. …  │
│       9992.27 │ Supplier#002149369 │ FRANCE         │ … │ 16-997-215-3908 │ h slyly after the …  │
│       9992.22 │ Supplier#002229635 │ ROMANIA        │ … │ 29-117-494-5343 │ unusual somas are …  │
│       9992.21 │ Supplier#001243459 │ ROMANIA        │ … │ 29-438-676-5070 │  ruthless deposits…  │
│       9992.18 │ Supplier#001499787 │ RUSSIA         │ … │ 32-981-276-2675 │ courts cajole slyl…  │
│       9992.14 │ Supplier#001551485 │ GERMANY        │ … │ 17-750-694-3182 │ lets. slyly specia…  │
│       9992.04 │ Supplier#001520263 │ RUSSIA         │ … │ 32-676-769-1115 │ ough the final, re…  │
│       9992.03 │ Supplier#000047868 │ FRANCE         │ … │ 16-814-219-3643 │ y. special account…  │
│       9992.00 │ Supplier#002889101 │ FRANCE         │ … │ 16-175-720-5603 │ ously ironic pinto…  │
│       9992.00 │ Supplier#002889101 │ FRANCE         │ … │ 16-175-720-5603 │ ously ironic pinto…  │
│       9991.98 │ Supplier#002500373 │ GERMANY        │ … │ 17-464-745-1571 │ . express, ironic …  │
│       9991.84 │ Supplier#000462341 │ UNITED KINGDOM │ … │ 33-607-871-1127 │  carefully bold th…  │
│       9991.57 │ Supplier#000991769 │ GERMANY        │ … │ 17-187-501-4064 │  regular asymptote…  │
├───────────────┴────────────────────┴────────────────┴───┴─────────────────┴──────────────────────┤
│ 100 rows (40 shown)                                                          8 columns (5 shown) │
└──────────────────────────────────────────────────────────────────────────────────────────────────┘
```

parquet:
```
┌───────────────┬────────────────────┬────────────────┬───┬─────────────────┬──────────────────────┐
│   s_acctbal   │       s_name       │     n_name     │ … │     s_phone     │      s_comment       │
│ decimal(15,2) │      varchar       │    varchar     │   │     varchar     │       varchar        │
├───────────────┼────────────────────┼────────────────┼───┼─────────────────┼──────────────────────┤
│       9999.94 │ Supplier#002307508 │ GERMANY        │ … │ 17-503-112-5653 │ nag carefully. bli…  │
│       9999.79 │ Supplier#000712808 │ UNITED KINGDOM │ … │ 33-559-792-5549 │ uld have to use ca…  │
│       9999.77 │ Supplier#002254341 │ GERMANY        │ … │ 17-391-701-5367 │ uctions. blithely …  │
│       9999.75 │ Supplier#001017723 │ FRANCE         │ … │ 16-717-704-2066 │ ven instructions w…  │
│       9999.72 │ Supplier#001460104 │ UNITED KINGDOM │ … │ 33-408-531-3270 │ the carefully fina…  │
│       9999.70 │ Supplier#000836396 │ ROMANIA        │ … │ 29-579-143-3270 │ c accounts. blithe…  │
│       9999.70 │ Supplier#000239544 │ UNITED KINGDOM │ … │ 33-509-584-9496 │ ets are. blithely …  │
│       9999.67 │ Supplier#002003789 │ GERMANY        │ … │ 17-621-582-3046 │ y among the carefu…  │
│       9999.64 │ Supplier#002094356 │ ROMANIA        │ … │ 29-581-506-9539 │ ickly pending inst…  │
│       9999.36 │ Supplier#002937179 │ FRANCE         │ … │ 16-525-981-5086 │ e. slylyCustomer a…  │
│       9999.27 │ Supplier#002093565 │ GERMANY        │ … │ 17-644-158-9607 │ bove the blithely …  │
│       9999.21 │ Supplier#000241339 │ ROMANIA        │ … │ 29-796-589-8064 │ ounts are. unusual…  │
│       9998.98 │ Supplier#001266482 │ RUSSIA         │ … │ 32-583-820-1183 │ final, bold ideas …  │
│       9998.93 │ Supplier#002807981 │ RUSSIA         │ … │ 32-938-325-4297 │ p blithely after t…  │
│       9998.84 │ Supplier#000771025 │ UNITED KINGDOM │ … │ 33-208-199-6904 │  blithely express …  │
│       9998.78 │ Supplier#001171577 │ ROMANIA        │ … │ 29-280-166-8362 │ fily even accounts…  │
│       9998.77 │ Supplier#001583206 │ GERMANY        │ … │ 17-338-279-6817 │ egular theodolites…  │
│       9998.73 │ Supplier#002392929 │ FRANCE         │ … │ 16-846-156-5209 │ o beans are furiou…  │
│       9998.73 │ Supplier#002028638 │ RUSSIA         │ … │ 32-179-161-3169 │ ckages haggle furi…  │
│       9998.59 │ Supplier#000352619 │ GERMANY        │ … │ 17-705-323-4485 │ ctions along the r…  │
│          ·    │         ·          │    ·           │ · │        ·        │          ·           │
│          ·    │         ·          │    ·           │ · │        ·        │          ·           │
│          ·    │         ·          │    ·           │ · │        ·        │          ·           │
│       9992.70 │ Supplier#000245718 │ UNITED KINGDOM │ … │ 33-570-729-4176 │ carefully above th…  │
│       9992.66 │ Supplier#000541492 │ ROMANIA        │ … │ 29-172-548-6729 │ heodolites run fin…  │
│       9992.66 │ Supplier#002666275 │ UNITED KINGDOM │ … │ 33-232-137-3532 │ ording to the care…  │
│       9992.55 │ Supplier#002129389 │ GERMANY        │ … │ 17-758-434-2104 │ silent sheaves aga…  │
│       9992.54 │ Supplier#000099650 │ RUSSIA         │ … │ 32-971-481-2533 │ ged deposits cajol…  │
│       9992.41 │ Supplier#000853625 │ UNITED KINGDOM │ … │ 33-590-481-6043 │ ously across the b…  │
│       9992.40 │ Supplier#002540732 │ RUSSIA         │ … │ 32-855-640-9841 │ s cajole blithely …  │
│       9992.38 │ Supplier#002188434 │ FRANCE         │ … │ 16-811-300-2036 │ ular pinto beans. …  │
│       9992.27 │ Supplier#002149369 │ FRANCE         │ … │ 16-997-215-3908 │ h slyly after the …  │
│       9992.22 │ Supplier#002229635 │ ROMANIA        │ … │ 29-117-494-5343 │ unusual somas are …  │
│       9992.21 │ Supplier#001243459 │ ROMANIA        │ … │ 29-438-676-5070 │  ruthless deposits…  │
│       9992.18 │ Supplier#001499787 │ RUSSIA         │ … │ 32-981-276-2675 │ courts cajole slyl…  │
│       9992.14 │ Supplier#001551485 │ GERMANY        │ … │ 17-750-694-3182 │ lets. slyly specia…  │
│       9992.04 │ Supplier#001520263 │ RUSSIA         │ … │ 32-676-769-1115 │ ough the final, re…  │
│       9992.03 │ Supplier#000047868 │ FRANCE         │ … │ 16-814-219-3643 │ y. special account…  │
│       9992.00 │ Supplier#002889101 │ FRANCE         │ … │ 16-175-720-5603 │ ously ironic pinto…  │
│       9992.00 │ Supplier#002889101 │ FRANCE         │ … │ 16-175-720-5603 │ ously ironic pinto…  │
│       9991.98 │ Supplier#002500373 │ GERMANY        │ … │ 17-464-745-1571 │ . express, ironic …  │
│       9991.84 │ Supplier#000462341 │ UNITED KINGDOM │ … │ 33-607-871-1127 │  carefully bold th…  │
│       9991.57 │ Supplier#000991769 │ GERMANY        │ … │ 17-187-501-4064 │  regular asymptote…  │
├───────────────┴────────────────────┴────────────────┴───┴─────────────────┴──────────────────────┤
│ 100 rows (40 shown)                                                          8 columns (5 shown) │
└──────────────────────────────────────────────────────────────────────────────────────────────────┘
```

tpch q03

```
SELECT
    l_orderkey,
    sum(l_extendedprice * (1 - l_discount)) AS revenue,
    o_orderdate,
    o_shippriority
FROM
    customer,
    orders,
    lineitem
WHERE
    c_mktsegment = 'BUILDING'
    AND c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND o_orderdate < CAST('1995-03-15' AS date)
    AND l_shipdate > CAST('1995-03-15' AS date)
GROUP BY
    l_orderkey,
    o_orderdate,
    o_shippriority
ORDER BY
    revenue DESC,
    o_orderdate
LIMIT 10;
```

pixels:
```
┌────────────┬───────────────┬─────────────┬────────────────┐
│ l_orderkey │    revenue    │ o_orderdate │ o_shippriority │
│   int64    │ decimal(38,4) │    date     │     int32      │
├────────────┼───────────────┼─────────────┼────────────────┤
│   16050176 │   489569.4989 │ 1995-03-06  │              0 │
│ 1161663111 │   477596.0480 │ 1995-03-06  │              0 │
│  257589767 │   476019.8902 │ 1995-02-20  │              0 │
│ 1557703747 │   474467.1367 │ 1995-03-12  │              0 │
│  725432482 │   469324.8793 │ 1995-03-08  │              0 │
│  173325283 │   468533.4626 │ 1995-02-18  │              0 │
│  162051301 │   468482.1688 │ 1995-03-11  │              0 │
│ 1435113092 │   467221.2564 │ 1995-02-28  │              0 │
│  177643398 │   466925.6490 │ 1995-03-14  │              0 │
│  914948229 │   466240.6896 │ 1995-03-10  │              0 │
├────────────┴───────────────┴─────────────┴────────────────┤
│ 10 rows                                         4 columns │
└───────────────────────────────────────────────────────────┘
```

pixels encoding
```
┌────────────┬───────────────┬─────────────┬────────────────┐
│ l_orderkey │    revenue    │ o_orderdate │ o_shippriority │
│   int64    │ decimal(38,4) │    date     │     int32      │
├────────────┼───────────────┼─────────────┼────────────────┤
│   16050176 │   489569.4989 │ 1995-03-06  │              0 │
│ 1161663111 │   477596.0480 │ 1995-03-06  │              0 │
│  257589767 │   476019.8902 │ 1995-02-20  │              0 │
│ 1557703747 │   474467.1367 │ 1995-03-12  │              0 │
│  725432482 │   469324.8793 │ 1995-03-08  │              0 │
│  173325283 │   468533.4626 │ 1995-02-18  │              0 │
│  162051301 │   468482.1688 │ 1995-03-11  │              0 │
│ 1435113092 │   467221.2564 │ 1995-02-28  │              0 │
│  177643398 │   466925.6490 │ 1995-03-14  │              0 │
│  914948229 │   466240.6896 │ 1995-03-10  │              0 │
├────────────┴───────────────┴─────────────┴────────────────┤
│ 10 rows                                         4 columns │
└───────────────────────────────────────────────────────────┘
```


parquet:
```
┌────────────┬───────────────┬─────────────┬────────────────┐
│ l_orderkey │    revenue    │ o_orderdate │ o_shippriority │
│   int64    │ decimal(38,4) │    date     │     int32      │
├────────────┼───────────────┼─────────────┼────────────────┤
│   16050176 │   489569.4989 │ 1995-03-06  │              0 │
│ 1161663111 │   477596.0480 │ 1995-03-06  │              0 │
│  257589767 │   476019.8902 │ 1995-02-20  │              0 │
│ 1557703747 │   474467.1367 │ 1995-03-12  │              0 │
│  725432482 │   469324.8793 │ 1995-03-08  │              0 │
│  173325283 │   468533.4626 │ 1995-02-18  │              0 │
│  162051301 │   468482.1688 │ 1995-03-11  │              0 │
│ 1435113092 │   467221.2564 │ 1995-02-28  │              0 │
│  177643398 │   466925.6490 │ 1995-03-14  │              0 │
│  914948229 │   466240.6896 │ 1995-03-10  │              0 │
├────────────┴───────────────┴─────────────┴────────────────┤
│ 10 rows                                         4 columns │
└───────────────────────────────────────────────────────────┘
```

tpch q04

```
SELECT
    o_orderpriority,
    count(*) AS order_count
FROM
    orders
WHERE
    o_orderdate >= CAST('1993-07-01' AS date)
    AND o_orderdate < CAST('1993-10-01' AS date)
    AND EXISTS (
        SELECT
            *
        FROM
            lineitem
        WHERE
            l_orderkey = o_orderkey
            AND l_commitdate < l_receiptdate)
GROUP BY
    o_orderpriority
ORDER BY
    o_orderpriority;

```

pixels
```
┌─────────────────┬─────────────┐
│ o_orderpriority │ order_count │
│     varchar     │    int64    │
├─────────────────┼─────────────┤
│ 1-URGENT        │     3156343 │
│ 2-HIGH          │     3157629 │
│ 3-MEDIUM        │     3157653 │
│ 4-NOT SPECIFIED │     3154374 │
│ 5-LOW           │     3158975 │
└─────────────────┴─────────────┘
```

pixels encoding
```
┌─────────────────┬─────────────┐
│ o_orderpriority │ order_count │
│     varchar     │    int64    │
├─────────────────┼─────────────┤
│ 1-URGENT        │     3156343 │
│ 2-HIGH          │     3157629 │
│ 3-MEDIUM        │     3157653 │
│ 4-NOT SPECIFIED │     3154374 │
│ 5-LOW           │     3158975 │
└─────────────────┴─────────────┘
```

parquet
```
┌─────────────────┬─────────────┐
│ o_orderpriority │ order_count │
│     varchar     │    int64    │
├─────────────────┼─────────────┤
│ 1-URGENT        │     3156343 │
│ 2-HIGH          │     3157629 │
│ 3-MEDIUM        │     3157653 │
│ 4-NOT SPECIFIED │     3154374 │
│ 5-LOW           │     3158975 │
└─────────────────┴─────────────┘
```

tpch q05

```
SELECT
    n_name,
    sum(l_extendedprice * (1 - l_discount)) AS revenue
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'ASIA'
    AND o_orderdate >= CAST('1994-01-01' AS date)
    AND o_orderdate < CAST('1995-01-01' AS date)
GROUP BY
    n_name
ORDER BY
    revenue DESC;

```

pixels
```
┌───────────┬──────────────────┐
│  n_name   │     revenue      │
│  varchar  │  decimal(38,4)   │
├───────────┼──────────────────┤
│ INDIA     │ 15902062821.7701 │
│ VIETNAM   │ 15894170669.5045 │
│ INDONESIA │ 15861134903.0691 │
│ CHINA     │ 15844593807.1730 │
│ JAPAN     │ 15793388465.3685 │
└───────────┴──────────────────┘
```

pixels encoding
```
┌───────────┬──────────────────┐
│  n_name   │     revenue      │
│  varchar  │  decimal(38,4)   │
├───────────┼──────────────────┤
│ INDIA     │ 15902062821.7701 │
│ VIETNAM   │ 15894170669.5045 │
│ INDONESIA │ 15861134903.0691 │
│ CHINA     │ 15844593807.1730 │
│ JAPAN     │ 15793388465.3685 │
└───────────┴──────────────────┘
```

parquet

```
┌───────────┬──────────────────┐
│  n_name   │     revenue      │
│  varchar  │  decimal(38,4)   │
├───────────┼──────────────────┤
│ INDIA     │ 15902062821.7701 │
│ VIETNAM   │ 15894170669.5045 │
│ INDONESIA │ 15861134903.0691 │
│ CHINA     │ 15844593807.1730 │
│ JAPAN     │ 15793388465.3685 │
└───────────┴──────────────────┘
```


