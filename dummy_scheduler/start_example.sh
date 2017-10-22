#!/bin/bash

export PGDATABASE=postgres
export PGUSER=postgres
export PGPASSWORD=mysecretpassword
export PGHOST=localhost

python3 "$@"
