# Makefile
EXTENSION = my_extension
MODULE_big = my_extension
OBJS = my_extension.o
DATA = my_extension--1.0.sql

PG_CONFIG = /usr/bin/pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)