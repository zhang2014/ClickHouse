#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

set -e

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS rename_table"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS already_rename_table"
$CLICKHOUSE_CLIENT -q "CREATE TABLE rename_table (a UInt8, b Int16, c Float32, d String, e Array(UInt8), f Nullable(UUID), g Tuple(UInt8, UInt16)) ENGINE = MergeTree ORDER BY a"

function thread1()
{
    while true; do $CLICKHOUSE_CLIENT --query "RENAME TABLE rename_table TO already_rename_table"; done
}

function thread2()
{
    while true; do $CLICKHOUSE_CLIENT -n --query "RENAME TABLE already_rename_table TO rename_table"; done
}

# https://stackoverflow.com/questions/9954794/execute-a-shell-function-with-timeout
export -f thread1;
export -f thread2;

timeout 15 bash -c thread1 2> /dev/null &
timeout 15 bash -c thread1 2> /dev/null &
timeout 15 bash -c thread1 2> /dev/null &
timeout 15 bash -c thread1 2> /dev/null &
timeout 15 bash -c thread2 2> /dev/null &
timeout 15 bash -c thread2 2> /dev/null &
timeout 15 bash -c thread2 2> /dev/null &
timeout 15 bash -c thread2 2> /dev/null &
timeout 15 bash -c thread1 2> /dev/null &
timeout 15 bash -c thread1 2> /dev/null &
timeout 15 bash -c thread1 2> /dev/null &
timeout 15 bash -c thread1 2> /dev/null &
timeout 15 bash -c thread2 2> /dev/null &
timeout 15 bash -c thread2 2> /dev/null &
timeout 15 bash -c thread2 2> /dev/null &
timeout 15 bash -c thread2 2> /dev/null &

wait

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS rename_table"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS already_rename_table"
