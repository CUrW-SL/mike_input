#!/usr/bin/env bash

# Print execution date time
echo `date`

while getopts d:t:f:b:m: option
do
    case "${option}"
    in
        d) run_date=${OPTARG};;
        t) run_time=${OPTARG};;
        f) forward=${OPTARG};;
        b) backward=${OPTARG};;
        m) mode=${OPTARG};;
    esac
done

# Change directory into where mike is located.
echo "Changing into /home/uwcc-admin/mike21"
cd /home/uwcc-admin/mike21
echo "Inside `pwd`"

# Activate venv.
echo "Activating venv python3 virtual environment."
source venv/bin/activate

python3 mike_input/code/gen_mike_input.py -d ${run_date} -t ${run_time} \
           -f ${forward} -b ${backward} -m ${mode}>> gen_mike_input.log 2>&1

# Deactivating virtual environment
echo "Deactivating virtual environment"
deactivate
