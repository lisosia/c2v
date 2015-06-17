#!/bin/bash -xu
trap 'kill $(jobs -p)' EXIT
jarfile="/home/denjo/DOCS/GENOME/jar/ExACMain.jar"
smplpre="10M_"
checkSexPath="./checkSex"
LAST=10

rm $checkSexPath
for i in $(seq 0 $LAST)
do
ruby ./mk_checkSex.rb ${smplpre}$i
done

for i in $(seq 0 $LAST)
do
java -classpath $jarfile genome.MainStore rundir000 "${smplpre}$i" ../consensus_data/${smplpre}$i 1 ../config $checkSexPath &
done

wait
