import numpy as np
import matplotlib.pyplot as plt
import math
from pyspark.sql import functions as F

def display_engine_prediction(engine_ids, predictions):
    
    engine_ids.sort()
    
    # Total plots 
    n = len(engine_ids)
     
    ncols = max(n//3,1)
    nrows = math.ceil(n/ncols)
        
    fig = plt.figure(figsize=(20, 10))
    
    for i, engine_id in enumerate(engine_ids):

        # Collect the data per engine
        engine_data = predictions.select('cycle', 'rul', 'prediction').filter( F.col('id') == engine_id).orderBy( F.col('cycle').asc())

        cycle = np.array(engine_data.select('cycle').collect())
        rul = np.array(engine_data.select('rul').collect())
        prediction = np.array(engine_data.select('prediction').collect())
        
        ax = plt.subplot(nrows, ncols, i+1)

        l1, = ax.plot( cycle, rul, label='RUL')
        l2, = ax.plot( cycle, prediction, label='Prediction')
        ax.set_xlabel('cycle')
        ax.set_ylabel('RUL')
        ax.set_ylim(0)
        plt.title("Engine: {}".format(engine_id))

    fig.legend((l1,l2), ('Actual', 'Prediction'), 'upper right')
    plt.tight_layout()
    
    plt.show()
