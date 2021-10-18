#!/bin/bash
printenv | sed 's/^\(.*\)$/export \1/g' > /app/project_env.sh
cron -f