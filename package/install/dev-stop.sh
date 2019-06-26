#!/bin/bash

SCRIPT_DIR=$(dirname ${BASH_SOURCE[0]})
PID_FILE="${SCRIPT_DIR}/drafter.pid"

if [ -f $PID_FILE ]; then
  kill $(cat $PID_FILE)
  rm $PID_FILE
fi
