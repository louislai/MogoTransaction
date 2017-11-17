#!/usr/bin/env bash
# Experiment scripts to be run in sunfire node

echo `date` >> duration.txt
# Sleep 5 mins to make balancing is done for shards
sleep 300

# run experiment with NC and LEVEL argument supplied, e.g `./benchmark.sh 20`
# LEVEL=0 means majority read and write, LEVEL=1 means local read and 1 write
NC=$1
LEVEL=$2
acc_arr=(`tail -1 config.txt`)

for i in `seq 1 ${NC}`; do
    serverIdx=$(( $i % 5 ))
    log="log${i}.txt"
    rm -f $log
    touch $log
    acc=${acc_arr[$(($serverIdx))]}
    ssh ${acc} \
     "cd MogoTransaction-master && python app/Client.py ${LEVEL} < 4224-project-files/xact-files/${i}.txt > /dev/null" \
     2>&1 | tee -a $log &
done

# Wait for all processes to finish andd output final db states
wait
rm -f db_state.txt
touch db_state.txt
ssh ${acc_arr[0]} \
 "cd MogoTransaction-master && python app/FinalOutputer.py ${LEVEL}" 2>&1 | tee -a db_state.txt

echo `date` >> duration.txt

# Get aggregate stats

NR=`ls | grep log | wc -l`
sum=0.0
max=-1
min=10000

for f in `ls | grep log`; do
  next=`tail -1 $f | awk '{ print $3 }'`
  sum=`echo $sum $next | awk '{print $1 + $2}'`
  min=`echo $min $next | awk '{if($2<$1){print $2}else{print $1}}'`
  max=`echo $max $next | awk '{if($2>$1){print $2}else{print $1}}'`
done

avg=`echo $sum $NR | awk '{print $1 / $2}'`
echo "Average throughput: $avg (xacts/s)" 2>&1 | tee -a aggregate.txt
echo "Min throughput: $min (xacts/s)" 2>&1 | tee -a aggregate.txt
echo "Max throughput: $max (xacts/s)" 2>&1 | tee -a aggregate.txt