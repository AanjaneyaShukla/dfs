rm -rf bin/*
javac -d bin -sourcepath gen-java -cp ./lib/libthrift-0.9.1.jar:./lib/slf4j-api-1.7.14.jar:./lib/slf4j-log4j12-1.6.1.jar gen-java/*.java
