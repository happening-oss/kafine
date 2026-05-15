PROJECT = kafine
PROJECT_DESCRIPTION = Kafka Client Library
PROJECT_ROOT_DIR := $(dir $(realpath $(lastword $(MAKEFILE_LIST))))
PROJECT_VERSION = $(shell $(PROJECT_ROOT_DIR)scripts/git-vsn)

# erlang.mk (with this as a dependency) runs the first target.
default: compile

get-deps:
	rebar3 get-deps

compile-deps:
	rebar3 compile --deps_only

compile:
	rebar3 compile

eunit:
	rebar3 do eunit

eunit-cover:
	rebar3 do eunit --cover, cover, covertool generate

elvis: lint

lint:
	rebar3 lint

fmt:
	rebar3 fmt -w '{src,include,test,integration}/**/*.{hrl,erl,app.src}'

check-fmt:
	rebar3 fmt -c '{src,include,test,integration}/**/*.{hrl,erl,app.src}'

elvis: lint

dialyzer:
	rebar3 dialyzer

eqwalize:: compile

ex_doc:
	rebar3 ex_doc

include eqwalizer.mk

all: get-deps compile-deps compile eunit-cover lint check-fmt dialyzer ex_doc

ci: all

.PHONY: integration

integration:
	rebar3 as integration do ct

GNU_TAR ?= gtar
ARCHIVE := ../kafine-$(PROJECT_VERSION).tar

archive:
	sed -i .bak 's,git@github\.com:.*/.*kafcod,https://github.com/happening-oss/kafcod,' rebar.config
	sed -i .bak 's,git@github\.com:.*/.*kamock,https://github.com/happening-oss/kamock,' rebar.config
	sed -i .bak '/kamock/ s,master,main,' rebar.config
	$(GNU_TAR) -c -f $(ARCHIVE) --exclude-from .archive-exclude .

checkouts:
	mkdir -p _checkouts
	ln -sf ../../kafcod _checkouts/kafcod
	ln -sf ../../kamock _checkouts/kamock

clean-checkouts:
	rm -rf _checkouts
