MRNB
====

Naive Bayes on MapReduce

Format for running the code

Training:

$HADOOP_HOME/bin/hadoop jar NaiveBayes.jar bayes.NaiveBayesTrainJob -D num_mappers="3" -D num_reducers="1" -D delimiter="," -D input="./iris.data" -D output="./outputiris" -D continousVariables="1,2,3,4" -D discreteVariables="" -D targetVariable="5" -D numColumns="5"

Testing:

$HADOOP_HOME/bin/hadoop jar NaiveBayes.jar bayes.NaiveBayesTestJob -D num_mappers="3" -D num_reducers="1" -D delimiter="," -D input="./iris.data" -D output="./output1" -D continousVariables="1,2,3,4" -D discreteVariables="" -D targetVariable="5" -D numColumns="5" -D modelPath="./outputiris" -D targetClasses="Iris-versicolor,Iris-setosa,Iris-virginica"

