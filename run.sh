#!/bin/bash
mvn compile
cp src/main/resources/jms* target/classes
mvn compile exec:java -Dexec.classpathScope=compile -Dexec.mainClass=com.hortonworks.TruckEventProcessorTopology
