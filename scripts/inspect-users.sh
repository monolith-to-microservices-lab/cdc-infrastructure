#!/usr/bin/env bash
# Dump every CDC event currently retained on the users topic.
# Usage: ./scripts/inspect-users.sh [timeout_ms]
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_common.sh"

consume_topic "${TOPIC_PREFIX}.public.users" "${1:-10000}"
