#!/bin/bash

scale=$1
if [ $# -lt 1 ]; then
    scale=10000
fi

# Random RO
./datagen -o $scale -r 1 -i 0
mv workload.txt workload1.txt

# Random RW
./datagen -o $scale -r 0.5 -i 0.5
mv workload.txt workload2.txt

# Random WO
./datagen -o $scale -r 0 -i 1
mv workload.txt workload3.txt

# Zipfian RO
./datagen -o $scale -r 1 -i 0 -z -s 0.7
mv workload.txt workload4.txt

# Zipfian RW
./datagen -o $scale -r 0.5 -i 0.5 -z -s 0.7
mv workload.txt workload5.txt

# Zipfian WO
./datagen -o $scale -r 0 -i 1 -z -s 0.7
mv workload.txt workload6.txt