# scala-spark-in-action
scala-spark-in-action

#### 转化操作持久化
```
每当我们调用一个新的行动操作时，整个 RDD 都会从头开始计算。要避
免这种低效的行为，用户可以将中间结果持久化
```
#### scala 中 wordcount 简约版本
```scala
sc.textFile(" ").flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
```
#### combineByKey()
![combineByKey数据示意图](file/img/combineByKey()%20数据流示意图.png)