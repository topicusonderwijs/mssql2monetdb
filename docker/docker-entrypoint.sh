#!/bin/bash
set -e

if [ "$1" == "" ]; then
	echo "ERROR: at leat 1 config file must be specified as command argument to Docker"
	exit 1
fi

for CONFIG in "$@"
do
    if [ ! -f "$CONFIG" ]; then
		echo "WARN: skipping '$CONFIG', not available in container" >&2
	else
		# run tool with config
		echo "Running MSSQL2MonetDB tool with config file: $CONFIG"
		echo "========================="
		java -jar mssql2monetdb-$TOOL_VERSION.jar -c "$CONFIG"
		echo "========================="
	fi
done

echo "Finished!"