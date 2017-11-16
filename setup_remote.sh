#!/usr/bin/env bash
time ./setup.sh
echo "Generating csv"
rm -rf data
mkdir data
time python app/CSVToJSON.py -p 4224-project-files/data-files -o data/
time ./load_data.sh
time ./create_index.sh
time ./shard.sh