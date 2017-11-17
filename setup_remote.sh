#!/usr/bin/env bash
time ./setup.sh
echo "Generating json"
rm -rf data
mkdir data
time python app/CSVToJSON.py -p 4224-project-files/data-files -o data/ 2>&1
time ./load_data.sh
time ./create_index.sh
time ./shard.sh