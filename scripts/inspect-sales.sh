#!/usr/bin/env bash
# Dump every CDC event currently retained on the sales topic.
# Usage: ./scripts/inspect-sales.sh [timeout_ms]
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_common.sh"

consume_topic "${TOPIC_PREFIX}.public.sales" "${1:-10000}"
