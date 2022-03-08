-- Run the following statements in Presto to create the Pixels database for TPC-H

CREATE SCHEMA IF NOT EXISTS tpch;

USE tpch;

CREATE TABLE IF NOT EXISTS customer (
  c_custkey bigint,
  c_name varchar(25),
  c_address varchar(40),
  c_nationkey bigint,
  c_phone char(15),
  c_acctbal decimal(15,2),
  c_mktsegment char(10),
  c_comment varchar(117)
) WITH (storage='s3');

CREATE TABLE IF NOT EXISTS lineitem (
  l_orderkey bigint,
  l_partkey bigint,
  l_suppkey bigint,
  l_linenumber integer,
  l_quantity decimal(15,2),
  l_extendedprice decimal(15,2),
  l_discount decimal(15,2),
  l_tax decimal(15,2),
  l_returnflag char(1),
  l_linestatus char(1),
  l_shipdate date,
  l_commitdate date,
  l_receiptdate date,
  l_shipinstruct char(25),
  l_shipmode char(10),
  l_comment varchar(44)
) WITH (storage='s3');

CREATE TABLE IF NOT EXISTS nation (
  n_nationkey bigint,
  n_name char(25),
  n_regionkey bigint,
  n_comment varchar(152)
) WITH (storage='s3');

CREATE TABLE IF NOT EXISTS orders (
  o_orderkey bigint,
  o_custkey bigint,
  o_orderstatus char(1),
  o_totalprice decimal(15,2),
  o_orderdate date,
  o_orderpriority char(15),
  o_clerk char(15),
  o_shippriority integer,
  o_comment varchar(79)
) WITH (storage='s3');

CREATE TABLE IF NOT EXISTS part (
  p_partkey bigint,
  p_name varchar(55),
  p_mfgr char(25),
  p_brand char(10),
  p_type varchar(25),
  p_size integer,
  p_container char(10),
  p_retailprice decimal(15,2),
  p_comment varchar(23)
) WITH (storage='s3');

CREATE TABLE IF NOT EXISTS partsupp (
  ps_partkey bigint,
  ps_suppkey bigint,
  ps_availqty integer,
  ps_supplycost decimal(15,2),
  ps_comment varchar(199)
) WITH (storage='s3');

CREATE TABLE IF NOT EXISTS region (
  r_regionkey bigint,
  r_name char(25),
  r_comment varchar(152)
) WITH (storage='s3');

CREATE TABLE IF NOT EXISTS supplier (
  s_suppkey bigint,
  s_name char(25),
  s_address varchar(40),
  s_nationkey bigint,
  s_phone char(15),
  s_acctbal decimal(15,2),
  s_comment varchar(101)
) WITH (storage='s3');
