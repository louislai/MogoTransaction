#!/usr/bin/env bash

# Check for data folder
if [ ! -d "4224-project-files" ]; then
    echo "Data folder does not exist. Downloading it from remote."
    wget http://www.comp.nus.edu.sg/~cs4224/4224-project-files.zip -O 4224-project-files.zip
    unzip 4224-project-files.zip
    rm 4224-project-files.zip
    echo "Data downloaded successfully!"
    echo "Replacing null values"
    cp replacenull.sh 4224-project-files/data-files/
    cd 4224-project-files/data-files
    ./replacenull.sh
    sort -t, -k1,1n -k2,2n -k3,3n order.csv -o order.csv
    sort -t, -k1,1n -k2,2n -k3,3n order-line.csv -o order-line.csv
    echo "Done replacing null values"
    cd ../../
else
    echo "Data folder already exists"
fi
