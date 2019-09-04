#!/usr/bin/env bash

# Print execution date time
echo `date`

# Change directory into where distributed hechms implementation is located.
echo "Changing into /home/uwcc-admin/distributed_hec/distributed_hechms"
cd /home/uwcc-admin/distributed_hec/distributed_hechms
echo "Inside `pwd`"

# Activate venv.
echo "Activating venv python3 virtual environment."
source venv/bin/activate

python3 controller.py >> distributed_hechms.log 2>&1
