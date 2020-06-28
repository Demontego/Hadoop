
##### Hint: для более быстрой работы можно извлечь архив и залить *.txt в hdfs

### Команда для запуска Spark-задачи: 

/bin/spark-submit --class SparkPageRank16 --master yarn --deploy-mode cluster  --driver-memory 2G --num-executors 26 --executor-memory 2G  hw4_pagerank.jar /data/hw4/soc-LiveJournal1.txt.gz hw4_pr

##### Время выполнения ~6 мин с распаковкой (~10 без распаковки)

### Команда для запуска MapReduce-задачи: 

sh mapredpipe.sh

##### Время выполнения ~17 мин с распаковкой (~27 без распаковки)


