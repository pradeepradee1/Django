import gc
import numpy as np
import pandas as pd
from tqdm import tqdm
from memory_profiler import profile
import pickle

import logging
logger = logging.getLogger(__name__)

def Reduce_Memory_Usage(DataFrame, int_cast=True, obj_to_category=True, subset=None):
    logger.info('Starting The Work In Reduce Memory Usage Functions')
    Rows_Count,Columns_Count=DataFrame.shape

    logger.info('DataFrame Rows {} Columns {}'.format(Rows_Count,Columns_Count))
    Start_Memory =DataFrame.memory_usage().sum() / 1024 ** 2
    gc.collect()

    logger.info('Memory Usage Before Optimization {:.2f} MB' .format(Start_Memory))
    # logger.info('Data Types \n {}'.format(df.dtypes))
    Columns_Name_List = subset if subset is not None else DataFrame.columns.tolist()

    for Column in tqdm(Columns_Name_List):
        Column_type = DataFrame[Column].dtypes

        if Column_type.name!='object' and Column_type.name != 'category'  and Column_type.name !='datetime':
            Column_Min=DataFrame[Column].min()
            Column_Max=DataFrame[Column].max()

            Integer_Type=str(Column_type)[:3]=='int'
            Float_Type=str(Column_type)[:5]=='float'

            if Integer_Type:

                if Column_Min >= np.iinfo(np.int8).min and Column_Max <= np.iinfo(np.int8).max:
                    DataFrame[Column]=DataFrame[Column].astype(np.int8)
                elif Column_Min >= np.iinfo(np.uint8).min and Column_Max <= np.iinfo(np.uint8).max:
                    DataFrame[Column]=DataFrame[Column].astype(np.uint8)
                elif Column_Min >= np.iinfo(np.int16).min and Column_Max <= np.iinfo(np.int16).max:
                    DataFrame[Column]=DataFrame[Column].astype(np.int16)
                elif Column_Min >= np.iinfo(np.uint16).min and Column_Max <= np.iinfo(np.uint16).max:
                    DataFrame[Column]=DataFrame[Column].astype(np.uint16)
                elif Column_Min >= np.iinfo(np.int32).min and Column_Max <= np.iinfo(np.int32).max:
                    DataFrame[Column]=DataFrame[Column].astype(np.int32)
                elif Column_Min >= np.iinfo(np.uint32).min and Column_Max <= np.iinfo(np.uint32).max:
                    DataFrame[Column]=DataFrame[Column].astype(np.uint32)
                elif Column_Min >= np.iinfo(np.int64).min and Column_Max <= np.iinfo(np.int64).max:
                    DataFrame[Column]=DataFrame[Column].astype(np.int64)
                elif Column_Min >= np.iinfo(np.uint64).min and Column_Max <= np.iinfo(np.uint64).max:
                    DataFrame[Column]=DataFrame[Column].astype(np.uint64)
            
            elif Float_Type:

                if Column_Min >= np.finfo(np.float16).min and Column_Max <= np.finfo(np.float16).max:
                    DataFrame[Column]=DataFrame[Column].astype(np.float16)
                elif Column_Min >= np.finfo(np.float32).min and Column_Max <= np.finfo(np.float32).max:
                    DataFrame[Column]=DataFrame[Column].astype(np.float32)
                else:
                    DataFrame[Column]=DataFrame[Column].astype(np.float64)
        
        elif  Column_type.name !='datetime' and obj_to_category:
            DataFrame[Column]=DataFrame[Column].astype('category')

    
    End_Memory=DataFrame.memory_usage().sum()/1024 ** 2
    
    logger.info('Memory Usage After Optimization  : {:.3f} MB' .format(End_Memory))
    logger.info('Decreased by {:.1f}%' .format(100*(Start_Memory - End_Memory)/Start_Memory))
    # logger.info('Data Types \n {}'.format(df.dtypes))
    Rows_Count,Columns_Count=DataFrame.shape

    logger.info('DataFrame Rows {} Columns {}'.format(Rows_Count,Columns_Count))
    logger.info('Completed The Work In Reduce Memory Usage Functions')
    gc.collect()

    return DataFrame
