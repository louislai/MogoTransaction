#!/usr/bin/env bash
# Experiment scripts to be run in sunfire node

accs=`tail -1 config.txt`
acc_arr=($(tail -1 config.txt))

for acc in $accs; do
    ssh ${acc} "source .bash_profile; ./setup_mongodb.sh"
done

for accId in `seq 0 4`; do
  acc=${acc_arr[accId]}
  if [ "$accId" -ge 0 ] && [ 2 -ge "$accId" ]; then
    ssh $acc "source .bash_profile; mongod --storageEngine wiredTiger --enableMajorityReadConcern --fork --configsvr --directoryperdb --pidfilepath /temp/mongodb/pid --dbpath /temp/mongodb/data/cfgsvr --logpath /temp/mongodb/log/cfgsvr.log --replSet cfg --port 21000"
  fi 
done

ssh ${acc_arr[0]} "source .bash_profile; mongo --port 21000 <<EOF
rs.initiate(
        {
                _id: \"cfg\",
                configsvr: true,
                members: [
                        { _id : 0, host : \"${acc_arr[0]##*@}:21000\" },
                        { _id : 1, host : \"${acc_arr[1]##*@}:21000\" },
                        { _id : 2, host : \"${acc_arr[2]##*@}:21000\" }
                ]
        }
)
EOF"

for shardId in `seq 0 4`; do
  
  port=$((21001 + $shardId))
  # Run shard mongod
  for repId in `seq 0 2`; do
        echo "Running source .bash_profile; mongod --storageEngine wiredTiger --enableMajorityReadConcern --fork --shardsvr --directoryperdb --pidfilepath /temp/mongodb/pidshard${shardId} --dbpath /temp/mongodb/data/shard${shardId} --logpath /temp/mongodb/log/shard${shardId}.log --replSet shard${shardId} --port ${port}"
        ssh ${acc_arr[$(( ($shardId + $repId) % 5 ))]} "source .bash_profile; mongod  --storageEngine wiredTiger --enableMajorityReadConcern --fork --shardsvr --directoryperdb --pidfilepath /temp/mongodb/pidshard${shardId} --dbpath /temp/mongodb/data/shard${shardId} --logpath /temp/mongodb/log/shard${shardId}.log --replSet shard${shardId} --port ${port}"
  done

  # Create rs
  ssh ${acc_arr[shardId]} "source .bash_profile; mongo --port $port <<EOF
rs.initiate(
        {
                _id: \"shard${shardId}\",
                members: [
                        { _id : 0, host : \"${acc_arr[$shardId]##*@}:$port\" },
                        { _id : 1, host : \"${acc_arr[$(( ($shardId + 1) % 5 ))]##*@}:$port\" },
                        { _id : 2, host : \"${acc_arr[$(( ($shardId + 2) % 5 ))]##*@}:$port\" },
                ]
        }
)
EOF"

  ssh ${acc_arr[$shardId]} "source .bash_profile; mongos --fork --pidfilepath /temp/mongodb/pidmongos --logpath /temp/mongodb/log/mongos_${shardId}.log --configdb cfg/${acc_arr[0]##*@}:21000,${acc_arr[1]##*@}:21000,${acc_arr[2]##*@}:21000 --port 21100"
done

for accId in `seq 0 4`; do
 
  for shardId in `seq 0 4`; do
   port=$((21001 + $shardId))
   ssh ${acc_arr[$accId]} "source .bash_profile; mongo --port 21100 <<EOF
sh.addShard(\"shard${shardId}/${acc_arr[$shardId]##*@}:$port,${acc_arr[$(( ($shardId + 1) % 5 ))]##*@}:$port,${acc_arr[$(( ($shardId + 2) % 5 ))]##*@}:$port\")
EOF"
  done
done

# Enable sharding and create indexes
  ssh ${acc_arr[0]} "source .bash_profile; mongo --port 21100 <<EOF
sh.enableSharding(\"cs4224\")
EOF"
