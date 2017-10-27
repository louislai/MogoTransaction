#!/usr/bin/env bash
# Experiment scripts to be run in sunfire node

accs=$(tail -1 config.txt)
acc_arr=($(tail -1 config.txt))

for acc in $accs; do
    ssh ${acc} "source .bash_profile; ./setup_mongodb.sh"
done

i=0
for acc in $accs; do
  i=$(($i + 1))
  if [ "$i" -ge 1 ] && [ 3 -ge "$i" ]; then
    echo $acc
    ssh $acc "source .bash_profile; mongod --fork --configsvr --directoryperdb --pidfilepath /temp/mongodb/pid --dbpath /temp/mongodb/data/cfgsvr --logpath /temp/mongodb/log/cfgsvr.log --replSet cfg --port 21000"
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

i=0
for acc in $accs; do
  echo "yolo"
  echo ""
  
  port=$((21001 + $i))
  # Run shard mongod
  for p in `seq 0 2`; do
        echo "Running source .bash_profile; mongod --fork --shardsvr --directoryperdb --pidfilepath /temp/mongodb/pidshard${i} --dbpath /temp/mongodb/data/shard${i} --logpath /temp/mongodb/log/shard${i}.log --replSet shard${i} --port ${port}"
        ssh ${acc_arr[$(( ($i + $p) % 5 ))]} "source .bash_profile; mongod --fork --shardsvr --directoryperdb --pidfilepath /temp/mongodb/pidshard${i} --dbpath /temp/mongodb/data/shard${i} --logpath /temp/mongodb/log/shard${i}.log --replSet shard${i} --port ${port}"
  done

  # Create rs
  ssh ${acc_arr[i]} "source .bash_profile; mongo --port $port <<EOF
rs.initiate(
        {
                _id: \"shard${i}\",
                members: [
                        { _id : 0, host : \"${acc_arr[$i]##*@}:$port\" },
                        { _id : 1, host : \"${acc_arr[$(( ($i + 1) % 5 ))]##*@}:$port\" },
                        { _id : 2, host : \"${acc_arr[$(( ($i + 2) % 5 ))]##*@}:$port\" },
                ]
        }
)

  ssh ${acc_arr[$i]} "source .bash_profile; mongos --fork --pidfilepath /temp/mongodb/pidmongos --logpath /temp/mongodb/log/mongos_${i}.log --configdb cfg/${acc_arr[0]##*@}:21000,${acc_arr[1]##*@}:21000,${acc_arr[2]##*@}:21000 --port 21100"
EOF"
 
 ssh ${acc_arr[0]} "source .bash_profile; mongo --port 21100 <<EOF
sh.addShard(\"shard${i}/${acc_arr[$i]##*@}:$port,${acc_arr[$(( ($i + 1) % 5 ))]##*@}:$port,${acc_arr[$(( ($i + 2) % 5 ))]##*@}:$port\")
EOF"
  
  i=$(($i + 1))
done

i=0
for acc in $accs; do
 
  for p in `seq 0 4`; do
   ssh ${acc_arr[i]} "source .bash_profile; mongo --port 21100 <<EOF
sh.addShard(\"shard${p}/${acc_arr[$p]##*@}:$port,${acc_arr[$(( ($p + 1) % 5 ))]##*@}:$port,${acc_arr[$(( ($p + 2) % 5 ))]##*@}:$port\")
EOF"
  done
  
  ssh ${acc_arr[i]} "source .bash_profile; mongo --port 21100 <<EOF
sh.enableSharding(\"cs4224\")
EOF"
  
  i=$(($i + 1))
done
