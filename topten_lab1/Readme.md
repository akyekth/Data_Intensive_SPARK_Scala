### Compile the code and create JAR
```
cd src
hadoop com.sun.tools.javac.Main -d topten_classes sics/TopTen.java
jar -cvf topten.jar -C topten_classes/ .
```

### Run the code
```
$HADOOP_HOME/sbin/hadoop-daemon.sh start namenode
$HADOOP_HOME/sbin/hadoop-daemon.sh start datanode

hdfs dfs -mkdir -p input
hdfs dfs -put data/users.xml input/users

hadoop jar topten.jar sics.TopTen input output

```

### View the result
```
hdfs dfs -cat output/part-r-00000
```
