#!/bin/sh
echo "Removing Files..."
rm -r classes/*
rm milestone2.jar
rm -r results
hadoop dfs -rmr std11/output
hadoop dfs -rmr std11/input
echo "*********************"
echo "Puting input..."
hadoop dfs -put std11/input std11/input
echo "*********************"
echo "Compiling..."
javac -cp lib/hadoop-mapred-0.21.0.jar:lib/hadoop-common-0.21.0.jar -source 1.6 -target 1.6 -d classes src/ch/epfl/advdb/milestone2/*.java src/ch/epfl/advdb/milestone2/io/*.java
jar cvf milestone2.jar -C classes .
echo "*********************"
echo "Starting Hadoop task..."
hadoop jar milestone2.jar ch.epfl.advdb.milestone2.Main std11/input/train std11/input/test std11/output 
echo "*********************"
echo "recovering resuts..."
hadoop dfs -get std11 results
echo "*********************"

