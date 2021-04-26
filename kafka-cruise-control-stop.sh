#!/bin/bash
# Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").
# See License in the project root for license information.

SIGNAL=${SIGNAL:-TERM}
PIDS=$(ps ax | grep 'java.*KafkaCruiseControlMain' | grep -v grep | awk '{print $1}')

if [ -z "$PIDS" ]; then
  echo "No cruise-control to stop"
  exit 1
else
  kill -s $SIGNAL $PIDS
fi
