# predictive-maintenance-spark

## Overview
This project simulates the prediction of an aircraft engine’s remaining useful life (RUL) using a survival analysis technique called accelerated failure time model. Determining the time available before a likely failure and being able to predict failures can help business’ better plan the use of their equipment, reduce operation costs, and avert issues before they become significant or catastrophic. The goal of predictive maintenance (PdM) is to allow for corrective actions and prevent unexpected equipment failure.

This project has two parts and leverages Spark/SparkML/Kafka and Spark structured streaming. The first part involved training an AFTSurvivalRegression model, the second processing that model simulated incoming time series events fed through a Kafka topic to exercise the prediction workflow.

<p align="center">
<img src ="doc/images/overview.png" />
</p> 

## Description of Data
The data used for this project is the NASA C-MAPSS Turbofan Engine Degradation Data Set https://ti.arc.nasa.gov/c/6/.  This data is model based simulated data from the Commercial Modular Aero-Propulsion System Simulation (C-MAPSS).

The data set is a multivariate time series. Each entry (row) in the data set reflects an operational cycle of a specific engine identified by engine id and cycle time. There are multiple entries per engine to represent different reporting times. Other columns represents different features 3 operational settings and 21 sensors:

<pre>
1)      engine id
2)      time, in cycles
3)      operational setting 1
4)      operational setting 2
5)      operational setting 3
6)      sensor measurement  1
7)      sensor measurement  2
...
26)     sensor measurement  21
</pre>

The CMAPSS data set is divided in 4 sets each for training, test, and RUL. Each subset represents a different operational condition and consists of a different number of engines.

All engines are assumed to be of the same model type and operating normally at the start of each series.  During its series, it develops a fault.

The cycle is a monotonically increasing integer and for the sake of the model it is assumed to be equally spaced and relative for each engine. The following figure shows 10 time series entry for engine 1.

![Fragment of the raw data](doc/images/data_fragment.png)


The data is further divided into a training and test set each that requires some subtly interpretation when being processed.

**Training Data Set**
* The last id, cycle entry is when the engine is declared unhealthy. For example if the first engine has 192 distinct time series events the cycle will go from 1 to 192, while the RUL will start with 192 and go down to 1. During data preparation I add an label column called ‘rul’. This is the ground truth. 

**Test Data Set** 
* Goal is to predict the amount of time remaining before the engine fails. This is referred to as the Remaining Useful Life (RUL).
* An engine’s status is terminated prior to actual engine failure. If the time series for an engine in the test data ends at 41, the model’s goal is to identify the RUL at that point.
* Using the provided RUL, a label column (rul) is added to hold the RUL at each time series.  This is generated in the following way: if the RUL is 112 at time series 41, then time series 1 will have an RUL of 153. The RUL is decremented with each succeeding entry.


The original CMAPSS data was in multiple txt files and were combined into dedicated train (train.csv), test (test_x.csv), test label (test_y.csv). In total the data is 20M compressed.

**Summary**
* NASA C-MAPSS Turbofan Engine Degradation Simulation Data Set https://ti.arc.nasa.gov/c/6/ 
* Size: Training and Test data 20M compressed
* Format: space delimited text files


**Download**

<pre>
$ curl -L https://ti.arc.nasa.gov/c/6/ -o data/CMAPSSDATA.zip
$ (cd data; unzip CMAPSSDATA.zip)
</pre>


## Description of Software
This project is was written using Python version 3.6.4 on the Anaconda platform. The development was done in Jupyter notebooks and Intellij. Notebooks were created for:
* [Data analysis](aft/project_data_analysis.ipynb)
* [Data Preparation, Training, Test](aft/project_aft_prep_train.ipynb)
* [Inference/Prediction](aft/project_aft_inference.ipynb)

Python scripts were created for:
* [Common data manipulation](engine_util.py)
* [Data visualization (plots)](engine_viz.py)
* [Engine Time Series Simulator](kafka/engine_cycle_producer.py) - Kafka event producer that randomly selects an engine and posts its setting and sensor data to the engine-stream topic at a specified rate.
* [Engine RUL Predictor](kafka/engine_cycle_consumer_struct.py) - Kafka/Spark Structured Streaming Monitor/Producer. This script uses the trained model to predict an engine's RUL based on its sensor data. If the RUL is within a threshold, an message is sent to the engine-alert topic.
* [Engine Alert Monitor](kafka/generic_consumer.py) - Alert Event Consumer that listens on the engine-alert topic and prints any received alerts.

Support shell scripts for starting demo and services include:
* start_services.sh
* local_kafka.sh
* start-kafka-shell.sh
* start_engine_alert_monitor.sh
* start_engine_monitor.sh
* start_engine_sim.sh 

 
Spark 2.4.0 was installed following the approach followed in the earlier part of class. To support kafka integration the additional jar spark-streaming-kafka-0-10_2.11-2.4.0.jar
 needs to be downloaded from :

https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka-0-10_2.11

This jar was placed in the projects jars directory. Note when the engine monitor is executed as a spark job via spark-submit, the spark-sql-kafka package must be specified. This command can be found in the  start_engine_monitor.sh script:

<pre>
$ spark-submit --master local[2] --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0 --jars spark-streaming-kafka-0-10_2.11-2.4.0.jar kafka/engine_cycle_consumer_struct.py localhost:9092 engine-stream -w 30 -s 30 -r 150
</pre>

Spark master is run on CentOS using: $SPARK_HOME/sbin/start-master.sh

To run Kafka and Zookeeper I leveraged Docker images from http://wurstmeister.github.io/kafka-docker/
* Install docker-ce. Follow instructions from https://docs.docker.com/install
* Install docker-compose. Docker-compose is used to configure, orchestration and start/stop the containers. Follow these instructions https://docs.docker.com/compose/install

This [docker-compose.yml](docker-compose.yml) file was used for downloading and starting the containers.

Note, the topics used are predefined in this file along with their partition and replication settings.

Starting Services
<pre>
$ mkdir -p data/cassandra/data
$ mkdir -p data/cassandra/scripts
$ sudo docker-compose up -d
</pre>

Cassandra can be ignored, I had intended to add streaming to persist, but did not have the time to finish.

python packages used can be installed with the provided requirements.txt file.
$ pip install -r requirements.txt

## Survival Regression
Survival Regression Models such as Spark’s AFTSurvivalRegression require an addition input column for each training sample to identified whether it is censored or uncensored. Censoring is the condition where the value of a measurement or observation is only partially known i.e. the event has not occurred before the completion of the trial. Observations are called censored when knowledge of their survival time is incomplete. 

A value of 1 means uncensored (the event has occurred). In the training set, all the engine time series in the training data are marked as uncensored under the assumptions that the failure event is known and is the last entry in the cycle life of that engine. However, I experimented with different settings and confirmed that setting all the uncensored gave the best result. 

## Results
Here are some sample results. A random set of 12 engines were chosen to see how the model did. The blue line shows the actual RUL which the yellow line is the prediction. 

![Sample results](doc/images/sample_results.png)

- The good:
  - While there is a lot of noise in the prediction, it trended well for the most part. 
  - The more cycles, the closer it converged. 

- The bad:
  - The prediction was frequently more optimistic than the true RUL. It needs to be more conservative.


## Conclusion
Accelerated Failure Time model is a powerful regression tool for a set of problem were time-to-event is needed. Spark structured stream coupled with an existing ML model is a scalable approach at handling large volumes of incoming sensor data for  performing prediction maintenance.

Experimenting with a [LSTM](https://github.com/sabderra/predictive-maintenance-lstm) based model is next.

## References
1. https://en.wikipedia.org/wiki/Predictive_maintenance
2. https://en.wikipedia.org/wiki/Accelerated_failure_time_model
3. https://spark.apache.org/docs/2.4.0/ml-classification-regression.html#survival-regression
4. https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.regression.AFTSurvivalRegression
5. https://gallery.azure.ai/Solution/Predictive-Maintenance-10
6. https://spark.apache.org/docs/2.4.0/structured-streaming-kafka-integration.html
7. https://ragulpr.github.io/2016/12/22/WTTE-RNN-Hackless-churn-modeling/
8. https://www.cscu.cornell.edu/news/statnews/stnews78.pdf 
