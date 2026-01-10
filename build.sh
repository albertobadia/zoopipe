#!/bin/bash

rm -rf dist
rm -rf build
rm -rf zoopipe.egg-info

uv build
