#!/usr/bin/env bash

killall -9 python
killall -9 mongod
killall -9 mongos

rm -rf /temp/mongodb
mkdir /temp/mongodb
tar -zxvf mongodb-linux-x86_64-3.4.7.tgz -C /temp/mongodb --strip-components=1
mkdir -p /temp/mongodb/data/cfgsvr

for i in `seq 0 4`; do
  mkdir -p "/temp/mongodb/data/shard${i}"
done

mkdir /temp/mongodb/log
