"""
Usage: engine_cycle_consumer.py <broker_list> <topic>

spark-submit --master local[2] --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0 --jars spark-streaming-kafka-0-10_2.11-2.4.0.jar kafka/engine_cycle_consumer_struct.py localhost:9092 engine-stream -w 30 -s 30 -r 150

"""
import sys
import os
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import RFormulaModel
from pyspark.ml.feature import MinMaxScalerModel
from pyspark.ml.regression import AFTSurvivalRegressionModel


ROOT_DIR = os.path.abspath('/shared/project/')
MODEL_DIR = ROOT_DIR + '/aft/models/'

sys.path.append(ROOT_DIR)
import engine_util

DEFAULT_OUTPUT = 'output'


class Predictor:

    def __init__(self, model_dir, config):

        self.config = config
        self.aft_model = AFTSurvivalRegressionModel.load(model_dir+'aft')
        self.formula_model = RFormulaModel.load(model_dir+'formula')
        self.scaler_model = MinMaxScalerModel.load(model_dir+'scaler')

        self.schema, self.feature_cols, self.label_cols = engine_util.create_engine_schema()
        self.cn = engine_util.CleanData(self.schema, self.feature_cols)

    def ds_predict(self, df, epoch_id):
        df2 = df.select(F.split('value', ',').alias('value'))
        df_result = df2.select(*[df2['value'][i] for i in range(26)])

        cycles_df = self.cn.fit(df_result)
        prepared_df = self.formula_model.transform(cycles_df)
        scaled_df = self.scaler_model.transform(prepared_df)
        pred_df = self.aft_model.transform(scaled_df)

        alert_df = pred_df.select('id', 'cycle', 'prediction') \
            .filter(F.col('prediction') <= self.config['rulThreshold'])

        if alert_df.count() > 0:
            alert_df.show()

        # Post alerts to kafka topic in the following format:
        #   id,cycle,rul_prediction
        # This data is packaged into a 'value' column that must be cast as a string or binary
        alert_df \
            .select(
                F.concat(
                    F.col('id'), F.lit(','), F.col('cycle'), F.lit(','), F.col('prediction')
                    ).alias('value')) \
            .selectExpr("CAST(value AS STRING)") \
            .write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.config['broker']) \
            .option("topic", self.config['alertTopic']) \
            .save()

        return alert_df


def main(broker, topic, config):
    """Main function that connects a Kafka topic to a Spark engine. Messages are consumed until this script is interrupted.

    Args:
        broker (str): Broke in host:port format.
        topic (str): Topic to listen on.
        config (dict): Configuration stored as name/value.
    """

    spark = SparkSession \
        .builder \
        .appName("engine-stream-consumer") \
        .master("local[2]") \
        .getOrCreate()

    predictor = Predictor(MODEL_DIR, config)

    ds = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", broker) \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .load()\
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    # Read the stream and perform prediction to see if the RUL threshold is reached.
    pred_ds = ds \
        .writeStream \
        .foreachBatch(predictor.ds_predict) \
        .start()

    trans = ds \
        .selectExpr("CAST(value AS STRING)") \
        .writeStream \
        .format("console") \
        .start()

    trans.awaitTermination()

    spark.stop()
    print("Done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("broker", help="host:port of the kafka broker.")
    parser.add_argument("topic", help="Topic to monitor.", default="engine-stream")
    parser.add_argument("alertTopic", help="Topic to send alerts to.", default="engine-alert")
    parser.add_argument("-b", "--batchDuration", help="Batch duration in seconds", default=30)
    parser.add_argument("-w", "--windowDuration", help="Window size", default=60)
    parser.add_argument("-s", "--slideDuration", help="sliding", default=60)
    parser.add_argument("-o", "--outputDirectory", help="Output directory", default=DEFAULT_OUTPUT)
    parser.add_argument("-r", "--rulThreshold", help="The predicted RUL to alert on", default=30)

    args = parser.parse_args()

    # Pass the various durations as config.
    conf = {"broker": args.broker,
            "topic": args.topic,
            "alertTopic": args.alertTopic,
            "batchDuration": int(args.batchDuration),
            "windowDuration": int(args.windowDuration),
            "slideDuration": int(args.slideDuration),
            "outputDirectory": args.outputDirectory,
            "rulThreshold": int(args.rulThreshold)
        }

    print("Broker={}, Monitoring Topic={}, Alert Topic={}"
          .format(args.broker, args.topic, args.alertTopic))

    print("Configuration: ", conf)

    main(args.broker, args.topic, conf)
