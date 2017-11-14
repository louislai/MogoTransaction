#!/usr/bin/env bash

data_path=data

for f in `ls $data_path`; do
    collection=${f%%.*}
    echo "Loading collection $collection"
    time mongoimport -h localhost:21100 --db cs4224 --collection $collection --drop --file $data_path/$f --type json
done