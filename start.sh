#!/bin/bash

python3 -m venv environment
set -e
source environment/bin/activate
pip install -r requirements.txt