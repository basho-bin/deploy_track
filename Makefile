.PHONY: test

REBAR = ./rebar3

DIALYZER_FLAGS =

all: compile

compile:
	$(REBAR) compile

clean:
	$(REBAR) clean

common_test: compile
	$(REBAR) ct

DIALYZER_APPS = kernel stdlib sasl erts ssl tools os_mon runtime_tools crypto inets \
	xmerl webtool snmp public_key mnesia eunit syntax_tools compiler

include tools.mk/tools.mk
