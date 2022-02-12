-- Run the following statements in Presto to create database for TPC-H

CREATE SCHEMA IF NOT EXISTS tpch;

USE tpch;

CREATE TABLE IF NOT EXISTS customer (
  c_custkey bigint,
  c_name varchar(25),
  c_address varchar(40),
  c_nationkey bigint,
  c_phone varchar(15),
  c_acctbal double,
  c_mktsegment varchar(10),
  c_comment varchar(117)
);

CREATE TABLE IF NOT EXISTS lineitem (
  l_orderkey bigint,
  l_partkey bigint,
  l_suppkey bigint,
  l_linenumber integer,
  l_quantity double,
  l_extendedprice double,
  l_discount double,
  l_tax double,
  l_returnflag varchar(1),
  l_linestatus varchar(1),
  l_shipdate date,
  l_commitdate date,
  l_receiptdate date,
  l_shipinstruct varchar(25),
  l_shipmode varchar(10),
  l_comment varchar(44)
);

CREATE TABLE IF NOT EXISTS nation (
  n_nationkey bigint,
  n_name varchar(25),
  n_regionkey bigint,
  n_comment varchar(152)
);

CREATE TABLE IF NOT EXISTS orders (
  o_orderkey bigint,
  o_custkey bigint,
  o_orderstatus varchar(1),
  o_totalprice double,
  o_orderdate date,
  o_orderpriority varchar(15),
  o_clerk varchar(15),
  o_shippriority integer,
  o_comment varchar(79)
);

CREATE TABLE IF NOT EXISTS part (
  p_partkey bigint,
  p_name varchar(55),
  p_mfgr varchar(25),
  p_brand varchar(10),
  p_type varchar(25),
  p_size integer,
  p_container varchar(10),
  p_retailprice double,
  p_comment varchar(23)
);

CREATE TABLE IF NOT EXISTS partsupp (
  ps_partkey bigint,
  ps_suppkey bigint,
  ps_availqty integer,
  ps_supplycost double,
  ps_comment varchar(199)
);

CREATE TABLE IF NOT EXISTS region (
  r_regionkey bigint,
  r_name varchar(25),
  r_comment varchar(152)
);

CREATE TABLE IF NOT EXISTS supplier (
  s_suppkey bigint,
  s_name varchar(25),
  s_address varchar(40),
  s_nationkey bigint,
  s_phone varchar(15),
  s_acctbal double,
  s_comment varchar(101)
);
