PROJECT = kafine
PROJECT_DESCRIPTION = Kafka Client Library
PROJECT_ROOT_DIR := $(dir $(realpath $(lastword $(MAKEFILE_LIST))))
PROJECT_VERSION = $(shell $(PROJECT_ROOT_DIR)scripts/git-vsn)

# compile is the first target; if we used 'all', then erlang.mk (with this as a dependency) runs _that_ target.
compile:
	rebar3 compile

dialyzer:
	rebar3 dialyzer

eunit:
	rebar3 do eunit --cover, cover, covertool generate

.PHONY: integration

integration:
	rebar3 as integration do ct

eqwalize:: compile

ex_doc:
	rebar3 ex_doc

include eqwalizer.mk

all: compile eunit dialyzer eqwalize ex_doc

GNU_TAR ?= gtar
ARCHIVE := ../kafine-$(PROJECT_VERSION).tar

archive:
	sed -i .bak 's,git@github\.com:.*/.*kafcod,https://github.com/happening-oss/kafcod,' rebar.config
	sed -i .bak 's,git@github\.com:.*/.*kamock,https://github.com/happening-oss/kamock,' rebar.config
	sed -i .bak '/kamock/ s,master,main,' rebar.config
	$(GNU_TAR) -c -f $(ARCHIVE) --exclude-from .archive-exclude .
