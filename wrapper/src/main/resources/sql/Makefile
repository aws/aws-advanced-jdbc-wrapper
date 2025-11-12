# Makefile for encrypted_data PostgreSQL extension
SDKROOT = /Library/Developer/CommandLineTools/SDKs/MacOSX.sdk
MODULE_big = encrypted_data
OBJS = encrypted_data.o

EXTENSION = encrypted_data
DATA = encrypted_data--1.0.sql

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
