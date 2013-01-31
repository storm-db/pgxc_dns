##########################################################################
#
#                pgxc_dns
#
# Copyright (c) 2012-2013, StormDB, Inc.
#
# This software is released under the PostgreSQL Licence
#
# Author: Jim Mlodgenski <jim@stormdb.com>
#
# IDENTIFICATION
#                 pgxc_dns/Makefile
#                  
##########################################################################

MODULE_big = pgxc_dns
OBJS = pgxc_dns.o

EXTENSION = pgxc_dns
DATA = pgxc_dns--1.0.sql \
	pgxc_dns--unpackaged--1.0.sql

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pgxc_dns
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
