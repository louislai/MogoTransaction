#!/usr/bin/env bash
time ./setup.sh
echo "Generating test data"
time python test/TestDataGenerator.py
echo "Generating csv"
rm -rf data
mkdir data
time python app/CSVToJSON.py -p test_data -o data/
time ./load_data.sh
