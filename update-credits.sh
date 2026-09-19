#!/usr/bin/env bash

set -euo pipefail

# Keep the historical entry point aligned with make credits.
exec bash "$(dirname "${BASH_SOURCE[0]}")/buildscripts/gen-credits.sh" "$@"
