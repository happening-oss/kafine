PROJECT = kafine
PROJECT_DESCRIPTION = Kafka Client Library
PROJECT_VERSION = $(shell scripts/git-vsn)

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

ERLANG_MK_TMP = $(shell TERM=dumb QUIET=1 rebar3 path --base)
include eqwalizer.mk

all: compile dialyzer eqwalize eunit ex_doc

GNU_TAR ?= gtar
ARCHIVE := ../kafine-$(PROJECT_VERSION).tar

archive:
	sed -i .bak 's,git@github\.com:.*/.*kafcod,https://github.com/happening-oss/kafcod,' rebar.config
	sed -i .bak 's,git@github\.com:.*/.*kamock,https://github.com/happening-oss/kamock,' rebar.config
	sed -i .bak '/kamock/ s,master,main,' rebar.config
	$(GNU_TAR) -c -f $(ARCHIVE) --exclude-from .archive-exclude .
