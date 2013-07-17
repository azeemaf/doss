#!/usr/bin/env bash
cd "`dirname \"$0\"`/.."
if  [ -d "target/classes" ]
then
    [ pom.xml -nt .javaclasspath ] && rm .javaclasspath && mvn dependency:build-classpath -q -Dmdep.outputFile=.javaclasspath
    [ ! -d /tmp/1 ] && mkdir /tmp/1  
    java -Ddoss.home=/tmp/1  -cp "$(cat .javaclasspath):target/classes" doss.Main $@
else
  echo "It looks like you have not compiled Doss yet."
  echo "Try running mvn clean compile"
fi


