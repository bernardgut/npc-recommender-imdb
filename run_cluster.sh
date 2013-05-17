#!/bin/sh
echo "Removing Files..."
rm -r classes/*
rm milestone2.jar
echo "*********************"
echo "Compiling..."
javac -cp lib/hadoop-mapred-0.21.0.jar:lib/hadoop-common-0.21.0.jar -source 1.6 -target 1.6 -d classes src/ch/epfl/advdb/milestone2/*.java src/ch/epfl/advdb/milestone2/io/*.java src/ch/epfl/advdb/milestone2/counters/*.java
jar cvf milestone2.jar -C classes .
echo "*********************"
echo "copy Hadoop task..."
scp milestone2.jar std11@icdatasrv1.epfl.ch:milestone2.jar
echo "*********************"

