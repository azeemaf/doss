mvn package dependency:copy-dependencies
mkdir -p $1/lib
cp target/doss-*.jar target/dependency/* $1/lib
