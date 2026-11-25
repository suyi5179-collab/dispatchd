#!/usr/bin/env bash
# Before the fixes: two shell bugs found while writing the runbook and the
# UI start script.

# @learn:runbook-v1
# runbook, first version: works in bash, breaks in zsh, where an unquoted
# variable is not split on spaces.
DC="docker-compose -p dispatchd"
$DC logs --since 10m worker
# zsh: no such file or directory: docker-compose -p dispatchd
# @learn:end runbook-v1

# @learn:ui-v1
# scripts/ui.sh, first version: `&` binds looser than `&&`, so
# "cd frontend && nohup node …" is ONE background job, and the
# `echo $!` runs in the parent, which never changed directory.
(cd frontend && nohup node node_modules/vite/bin/vite.js > ../.runtime/vite.log 2>&1 & echo $! > ../$pidf)
# ./scripts/ui.sh: line 13: ../.runtime/vite.pid: No such file or directory
# @learn:end ui-v1
