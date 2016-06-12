#!/bin/sh
cd `dirname $0`/..
MODE="$1"
EXTRA_ARGS="$2"
HOSTNAME=`hostname`
if [ "$MODE" = "dev" ]; then
    EXTRA_ARGS="$EXTRA_ARGS -setcookie deploy_track"
else
    EXTRA_ARGS="$EXTRA_ARGS -noshell -noinput -setcookie deploy_track"
exec erl -pa $PWD/_build/default/lib/*/ebin -name deploy_track@$HOSTNAME -config config/$HOSTNAME.config -s deploy_track $EXTRA_ARGS