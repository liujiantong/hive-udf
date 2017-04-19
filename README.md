# Hive UDF Examples for iKang


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
hive> add jar /path/to/ikang-hive-udf-0.1.0.jar;
hive> create temporary function collect as 'ikang.hive.udf.CollectAggUDAF';
hive> select collect(alias) from people limit 10;
```
