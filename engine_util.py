from pyspark.sql.types import *
from pyspark.ml import Estimator
from pyspark.sql import functions as F
import os

fn_map = {
    "train_FD001": 1000,
    "train_FD002": 2000,
    "train_FD003": 3000,
    "train_FD004": 4000
}
    
def get_data_set_name(path):
    filename = os.path.basename(path)
    fn = os.path.splitext(filename)[0]
    return fn_map.get(fn)

# Used to map data set file name to an integer to be combined
# with the id. This will give us unique ids across all
# data set files.
data_set_name = F.udf(lambda path: get_data_set_name(path))



class CleanData(Estimator):
    """ Processes a data frame to and to perform any needed
    cleaning. The schema and columns to clean are passed on object creation.
    
    Args:
        schema (StructType): Spark type object defining the target schema.
        feature_cols (list): List of column names to clean
    """
    
    def __init__(self, schema, feature_cols=None):
        self.schema = schema
        self.feature_cols = feature_cols
    
    
    def _fit(self, dataset):
        """Runs cleaning on the specified DataFrame
        
        Args:
            dataset: DataFrame to process. 
            
        Returns:
            Updated DataFrame
        """
        
        # Get summary of data set specifically to collect mean
        desc = dataset.describe().collect()
        
        # Change columns names to those passed in schema
        newDf = dataset
        for i, col in enumerate(newDf.columns):
            newDf = newDf.withColumnRenamed(col, self.schema[i].name)
    
        if self.feature_cols is not None:
            # For those columns specifed, update any non-values to the column
            # mean.
            for i, col in enumerate(newDf.columns):
                if ( col in self.feature_cols):
                    newDf = newDf.withColumn(col, F.regexp_replace(col, 'NA', str(desc[1][i+1])))
                
        # Update column type
        for i, name in enumerate(newDf.columns):
            newDf = newDf.withColumn(name, newDf[name].cast(self.schema[i].dataType))
                
        return newDf


def create_engine_schema():
    # Define data schema, although this is usually passed when the
    # DataFrame is created, I'll be leveraging it to update the DataFrame
    # using the Estimate derived class
    
    
    # Feature columns. 
    feature_cols = ['cycle']
    
    # Label columns
    label_cols = ['rul', 'censor']
    
    # Three operational setting columns
    setting_cols = ['setting' + str(i) for i in range(1,4)]
    feature_cols.extend(setting_cols)
    
    # Twenty one sensor columns
    sensor_cols = ['s' + str(i) for i in range(1,22)]
    feature_cols.extend(sensor_cols)
    
    # All will be read in from the training data except for RUL
    # and cycle_norm which will be generated from  the existing
    # data later on.
    schema = StructType()\
                .add("id", IntegerType())\
                .add("cycle", IntegerType())
    
    for c in setting_cols:
        schema.add(c, FloatType())
        
    for c in sensor_cols:
        schema.add(c, FloatType())

    return schema, feature_cols, label_cols



def calc_rmse(predictions):
    mse = lambda y,p: (y-p)**2

    rmse = predictions.select('id', 'cycle', 'rul', 'prediction')\
			.withColumn('se', mse(F.col('rul'),F.col('prediction')) )\
			.select( F.sqrt(F.mean('se')).alias('rmse'))\
			.collect()

    return rmse[0].__getitem__("rmse")

