#!/bin/bash

sbt clean
sbt assembly

start=`date +%s`

input=$1

if [ ${input: -3} == ".gz" ]; then
	echo "uncompressing $input file"
	gunzip $input
	input=${input%.gz*}
fi

$SPARK_HOME/bin/spark-submit target/scala-2.11/WeblogChallenge.jar $input

end=`date +%s`

time=$((end-start))

echo "******************* Execution duration: ${time} seconds ***********************"
