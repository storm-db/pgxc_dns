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
 *                pgxc_dns/pgxc_dns--unpackaged--1.0.sql
 *
 *-------------------------------------------------------------------------
 */


-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pgxc_dns" to load this file. \quit

ALTER EXTENSION pgxc_dns ADD function pgxc_dns_zone();
ALTER EXTENSION pgxc_dns ADD function pgxc_dns_host_weight();
ALTER EXTENSION pgxc_dns ADD view pgxc_dns_zone;
