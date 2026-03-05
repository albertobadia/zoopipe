#!/bin/bash

set -e

uv run cargo test

uv run pytest
