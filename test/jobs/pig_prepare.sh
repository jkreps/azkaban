#!/bin/bash

gateway=esv3-hcl96.corp
hadoop_home=/export/apps/hadoop/

input=$1

echo "input=$input, output=$output\n"

echo "remove input file $input"
ssh $gateway "bash -l $hadoop_home/bin/hadoop fs -rm $input "

echo "copy input file $input"
cat $input | ssh $gateway "bash -l $hadoop_home/bin/hadoop fs -put - $input"
if [[ "$?" != "0" ]]; then
	echo "error in copy input file to $input";
	exit 1;
fi

# ignore errors
exit 0;
