#!/bin/bash
pos_max=1000000000 #10M * 100, mk_consensu makes pos_max/100 = 10M line
prefix="10M_"
for i in $(seq 0 10)
do
    
file="../consensus_data/$prefix$i"    
if [ -e $file ]; then
    echo "$file already exists"
else
    echo "start to creat file: "$file
    ruby mk_consensus.rb "1" $pos_max > $file &
fi

done

wait
