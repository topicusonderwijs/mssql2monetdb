#!/bin/bash
set -e

CONFIG="$1"

if [ "$CONFIG" == "" ]; then
	echo "ERROR: config file must be specified as command argument to Docker"
	exit 1
fi

if [ ! -f "$CONFIG" ]; then
	echo "ERROR: specified config file '$CONFIG' is not available in container" >&2
	exit 1
fi

# run tool with config
java -jar mssql2monetdb-$TOOL_VERSION.jar -c "$CONFIG"