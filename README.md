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
%> hive
hive> ADD JAR /path/to/ikang-hive-udf-0.1.0.jar;
hive> create temporary function collect as 'ikang.hive.udf.CollectAggUDAF';
hive> select hello(firstname) from people limit 10;

```
