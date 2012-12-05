/*-------------------------------------------------------------------------
 *
 *                pgxc_dns
 *
 * Copyright (c) 2012, StormDB, Inc.
 *
 * This software is released under the PostgreSQL Licence
 *
 * Author: Jim Mlodgenski <jim@stormdb.com>
 *
 * IDENTIFICATION
 *                pgxc_dns/pgxc_dns--1.0.sql
 *
 *-------------------------------------------------------------------------
 */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pgxc_dns" to load this file. \quit

-- Register functions.
CREATE FUNCTION pgxc_dns_zone(
    OUT name text,
    OUT ttl int4,
    OUT rdtype text,
    OUT rdata text
)
RETURNS SETOF record
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE FUNCTION pgxc_dns_host_weight(
    OUT host text,
    OUT weight int4
)
RETURNS SETOF record
AS 'MODULE_PATHNAME'
LANGUAGE C;

-- Register a view on the function for ease of use.
CREATE VIEW pgxc_dns_zone (NAME, TTL, RDTYPE, RDATA) AS
  SELECT * FROM pgxc_dns_zone();

GRANT SELECT ON pgxc_dns_zone TO public;

