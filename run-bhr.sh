#!/bin/bash
START=$1
END=$2
if [ -z "$START" -o -z "$END" ]; then
  # Arguments missing. Default to the previous week
  START=$(date -d 'last saturday - 6 days' +%Y%m%d)
  END=$(date -d 'last saturday' +%Y%m%d)
fi

if [ -z "$(which git)" ]; then
    yes | sudo apt-get install git
fi

python -c 'import simplejson' 2>/dev/null
if [ $? -ne 0 ]; then
    yes | sudo apt-get install python-dev
    yes | sudo pip install simplejson
fi

sudo chown -R ubuntu:ubuntu /mnt

FIRST_OUTPUT_DIR=/mnt/bhr-$START-$END
if [ ! -d "$FIRST_OUTPUT_DIR" ]; then
    mkdir -p "$FIRST_OUTPUT_DIR"
fi

echo "Running BHR analyzer for $START to $END"

BASE=$(pwd)
if [ -d "$BASE/hang-telemetry-tools" ]; then
    cd $BASE/hang-telemetry-tools
    git pull
    cd -
else
    git clone https://github.com/darchons/hang-telemetry-tools
fi

cd ~/telemetry-server
python $BASE/hang-telemetry-tools/fetchbhr.py $START $END
echo "Job exited with code: $?"

cd -
echo "Moving $FIRST_OUTPUT_DIR to final output dir"
if [ ! -d "output" ]; then
    mkdir -p output
fi
cp -r $FIRST_OUTPUT_DIR output/
echo "Done!"
