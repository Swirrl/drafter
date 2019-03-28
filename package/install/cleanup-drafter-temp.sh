#!/bin/bash

echo 'cleaning up drafter temp'
echo "find /tmp -name "drafter-body*.tmp" -mmin +300 -delete"
find /tmp -name "drafter-body*.tmp" -mmin +300 -delete