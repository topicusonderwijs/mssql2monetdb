#!/bin/bash
set -e

if [ "$1" == "" ]; then
	echo "ERROR: at leat 1 config file must be specified as command argument to Docker"
	exit 1
fi

SWITCH_FLAG=""
for ARG in "$@"
do
	if [ "$ARG" == "--no-switch" ] || [ "$ARG" == "--switch-only" ]; then
		SWITCH_FLAG="$ARG"
	fi
done

for CONFIG in "$@"
do
	if [ "$CONFIG" != "--no-switch" ] && [ "$CONFIG" != "--switch-only" ]; then
	    if [ ! -f "$CONFIG" ]; then
			echo "WARN: skipping '$CONFIG', not available in container" >&2
		else
			# run tool with config
			echo "Running MSSQL2MonetDB tool with config file: $CONFIG"
			echo "========================="
			java -cp "/mssql2monetdb/*" -Dlog4j.configuration=file:"/mssql2monetdb/log4j.properties" nl.topicus.mssql2monetdb.CopyTool -c "$CONFIG" $SWITCH_FLAG
			echo "========================="
		fi
	fi
done

echo "Finished!"