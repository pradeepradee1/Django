from pyspark.sql import SparkSession
from rest_framework import status
from modeltrain.models import OutlierAudit
from modeltrain.utils import create_table,df_tosql,get_checksum,get_file_size
import pandas as pd
from pathlib import Path
from pyspark.sql import functions as f
import re
import logging
logger = logging.getLogger(__name__)

class Outlier():

    def __init__(self, df):
        self.df = df
    
    def _calculate_bounds(self,outliers_columns,Q1,Q2):
        bounds = {
            c: dict(
                zip(["q1", "q3"], self.df.approxQuantile(c, [Q1, Q2], 0))
            )
            for c, d in zip(self.df.columns, self.df.dtypes) if c in [outliers_columns]
        }
        for c in bounds:
            iqr = bounds[c]['q3'] - bounds[c]['q1']
            bounds[c]['min'] = bounds[c]['q1'] - (iqr * 1.5)
            bounds[c]['max'] = bounds[c]['q3'] + (iqr * 1.5)

        return bounds
    
    def _flag_outliers_df(self,outliers_columns,Q1,Q2):
        bounds = self._calculate_bounds(outliers_columns,Q1,Q2)
        outliers_col = [
            f.when(
                ~f.col(c).between(bounds[c]['min'], bounds[c]['max']),
                f.col(c)
            ).alias(c + '_outlier')
            for c in bounds]
        
        return self.df.select(*outliers_col)
    
    def removeoutliers(self,outliers_columns,Q1,Q2):
        print(outliers_columns)
        outlier_df = self._flag_outliers_df(outliers_columns,Q1,Q2)
        MinimumnRow=str(outlier_df.agg({"PRINCIPAL_AMOUNT_outlier":"min"}).collect()[0])
        value=re.findall(r"[0-9]+",MinimumnRow)
        print(self.df.count())
        self.df=self.df.where(self.df.outliers_columns<value[0])
        print(self.df.count())
        return self.df



def outliers(request):
    
    response = request
    
    try:
        logger.info("Outliers Module Started Successfuly")
        ProcessId = request.get('ProcessId')
        InputFile = request.get('InputFile')
        OutputFile = request.get('OutputFile')
        ProductName = request.get('ProductName')
        TableName = request.get('TableName')
        AlgorithmName = request.get('AlgorithmName')
        AlgorithmType = request.get('AlgorithmType')
        TemplateName = request.get('TemplateName')
        OutliersColumns = request.get('OutliersColumns')
        OutliersType = request.get('OutliersType')
        Q1 = request.get('Q1')
        Q2 = request.get('Q2')
        OutliersOptions = request.get('OutliersOptions')
        HeapMemorySize = request.get('HeapMemorySize')
        StackMemorySize = request.get('StackMemorySize')
        
        TableName = ProcessId+"_"+ ProductName +"_"+ TableName
        OutputFile = "/".join((OutputFile,ProcessId,TemplateName,ProductName,AlgorithmName,"Outliers"))
        logger.info("Request Got It Successfully For Outliers Module")

        # Output_file_Directory
        if "/" in OutputFile:
            Path(OutputFile).mkdir(parents=True, exist_ok=True)
        elif "\\" in OutputFile:
            Path(OutputFile).mkdir(parents=True, exist_ok=True)
        
        #Module process
        spark = SparkSession.builder \
                .master('local[*]') \
                .config("spark.driver.extraJavaOptions", StackMemorySize) \
                .config("spark.driver.memory", HeapMemorySize) \
                .appName('cub-app') \
                .getOrCreate()
        
        logger.info("Spark Session Created Successfully For Outliers Module")

        OutliersDF = spark.read.csv(InputFile,inferSchema =True,header=True)
        
        logger.info("DataFrame Read Successfully for Outliers Module")

        if OutliersOptions == "Enable" :
            logger.info("Outliers Module Enable")
            if AlgorithmType == "Regression" :
                obj=Outlier(OutliersDF)
                OutliersDF=obj.removeoutliers(OutliersColumns,Q1,Q2)
                logger.info("Outliers Module Completed")
        
        logger.info("Outliers DataFrame Starting To Write")
        # writing DF in CSV format
        OutliersDF.coalesce(6).write.format("csv").mode("overwrite").option("header", "true").save(OutputFile)
        logger.info("Outliers DataFrame Successfully Saved")
        
        response['Status'] = status.HTTP_200_OK
        response['OutlierStatus'] = ['Outliers Completed']
        response['OutputFile'] = OutputFile
        response['ErrorMessage'] = ''
        logger.info("Outliers DataFrame End Successfully")

    except Exception as e:
        response['Status'] = status.HTTP_500_INTERNAL_SERVER_ERROR
        response['OutlierStatus'] = 'Outliers Failed'
        logger.error(str(e))
        response['ErrorMessage'] = [str(e), 'Error occured while finding Outliers. Please check the debug logs']
    
    finally:
        audit = OutlierAudit(**response)
        audit.save()
        spark.stop()
    return response

