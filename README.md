# Hive UDF Examples


## Compile

```
mvn compile
```

## Test

```
mvn test
```

## Build
```
mvn clean package
```

## Run

```
%> sudo -u hdfs hive
hive> add jar /path/to/hive-udf-0.1.0.jar;
hive> create temporary function collect as 'hive.udf.CollectAggUDAF';
hive> select collect(col1, col2, ...) from people limit 10;
```
