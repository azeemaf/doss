#!/usr/bin/env bash
cd "`dirname \"$0\"`/.."
for f in src
do
    if [ -z "$NEWEST" ]
    then 
        NEWEST=$f
    elif [ "$f" -nt "$NEWEST" ] 
    then
        NEWEST=$f
    fi
done
if  [ -d "target/classes" ]
then
  if [ "target" -nt "$NEWEST"]  
  then
    [ pom.xml -nt .javaclasspath ] && rm .javaclasspath && mvn dependency:build-classpath -q -Dmdep.outputFile=.javaclasspath
    java -Ddoss.home=/tmp/1  -cp "$(cat .javaclasspath):target/classes" doss.Main $@
  else
    echo "Changes have been made to the source files since the last clean build"
    echo "Try running mvn clean compile"
  fi
else
  echo "It looks like you have not compiled Doss yet."
  echo "Try running mvn clean compile"
fi


