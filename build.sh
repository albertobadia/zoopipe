#!/bin/bash

rm -rf dist
rm -rf build
rm -rf flowschema.egg-info

uv build
