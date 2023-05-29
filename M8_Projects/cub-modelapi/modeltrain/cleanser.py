from pyspark.sql import SparkSession
from pyspark.sql.functions import isnull, when, count, col, mean as _mean
from rest_framework import status
from modeltrain.models import CleanserAudit,CleanserMPAudit
from modeltrain.utils import create_table,df_tosql,get_checksum,get_file_size
import pandas as pd
import numpy as np
import os
from pathlib import Path
from pyspark.ml.feature import Imputer
import pyspark.sql.functions as F
from pyspark.sql.functions import desc
from pyspark.sql.functions import col, countDistinct
from modeltrain.Reduce_Memory import Reduce_Memory_Usage
from sklearn.impute import SimpleImputer
import pickle

import logging
logger = logging.getLogger(__name__)


def cleanser(request):

    response = request
    
    try:
        logger.info("Cleaning Module Started Successfuly")
        ProcessId = request.get('ProcessId')
        InputFile = request.get('InputFile')
        OutputFile = request.get('OutputFile')
        ModelFile = request.get('ModelFile')
        TemplateName = request.get('TemplateName')
        ProductName = request.get('ProductName')
        AlgorithmName = request.get('AlgorithmName')
        CleansingDFTableName = request.get('CleansingDFTableName')
        CleansingFeaturesList = request.get('CleansingFeaturesList')
        ColumnNameForFeaturesList = request.get('ColumnNameForFeaturesList')
        Strategy = request.get('Strategy')
        LowerRange = request.get('LowerRange')
        UpperRange = request.get('UpperRange')
        HeapMemorySize = request.get('HeapMemorySize')
        StackMemorySize = request.get('StackMemorySize') 
        
        UpperRange = UpperRange / 100

        CleansingDFTableName = ProcessId+"_"+ ProductName +"_"+ CleansingDFTableName
        CleansingFeaturesList = ProcessId+"_"+ ProductName +"_"+ CleansingFeaturesList
        
        OutputFile = "/".join((OutputFile,ProcessId,TemplateName,ProductName,AlgorithmName,"Cleaning"))
        ModelFile = "/".join((ModelFile,ProcessId,TemplateName,ProductName,AlgorithmName,"ImputerModel"))
        logger.info("Request Got It Successfully For Cleaning Module")

        # Output_file_Directory
        if "/" in OutputFile:
            Path(OutputFile).mkdir(parents=True, exist_ok=True)
        elif "\\"  in OutputFile:
            Path(OutputFile).mkdir(parents=True, exist_ok=True)
        
        # Module process
        spark = SparkSession.builder \
                .master('local[*]') \
                .config("spark.driver.extraJavaOptions", StackMemorySize) \
                .config("spark.driver.memory", HeapMemorySize) \
                .appName('cub-app') \
                .getOrCreate()
        
        logger.info("Spark Session Created Successfully For Cleaning Module")
        
        Cleaningdf = spark.read.csv(InputFile,inferSchema =True,header=True)
        
        logger.info("DataFrame Read Successfully for Cleaning Module")
        
        # Removing the Most Unique Features and Homogenious Values Features
        CountDf=Cleaningdf.agg(*(countDistinct(col(c)).alias(c) for c in Cleaningdf.columns))
        CountPDDf=CountDf.toPandas()
        countdict=CountPDDf.to_dict()
        
        distinctcountcolums={}
        for key,val in countdict.items():
            distinctcountcolums[key]=list(val.values())[0]
        
        columnslist=[]
        for key1 in distinctcountcolums:
            if distinctcountcolums[key1] <= int(LowerRange):
                columnslist.append(key1)
                Cleaningdf=Cleaningdf.drop(key1)
            elif distinctcountcolums[key1] >= UpperRange*Cleaningdf.count():
                columnslist.append(key1)
                Cleaningdf=Cleaningdf.drop(key1)

        # Spliting the DF into Continous and Categorical
        ContinousColumnsNames=[]
        CategoricalColumnsNames=[]
        for i in Cleaningdf.dtypes:
            if i[1] == "string":
                CategoricalColumnsNames.append(i[0])
            else:
                ContinousColumnsNames.append(i[0])
        
        # Filling The Null Values Based on the Mode For Numerical Columns
        imputer = Imputer(inputCols=ContinousColumnsNames, outputCols=ContinousColumnsNames).setStrategy(Strategy)
        model = imputer.fit(Cleaningdf)
        Cleaningdf=model.transform(Cleaningdf)
        logger.info("Imputer Model Done Successfully for Numerical Columns")
        
        # Filling The Null Values Based on the Mode For CategoricalColumns
        for Columns in CategoricalColumnsNames :
            Groupbydf=Cleaningdf.groupBy(Columns).count().sort(desc("count"))
            PandasDF=Groupbydf.limit(2).toPandas().values        
            for j in range(0,2) :
                if PandasDF[j][0] != None :
                    Cleaningdf=Cleaningdf.fillna(str(PandasDF[j][0]), subset=[Columns])
                    break  
        
        logger.info("Null Values filled Successfully for Categorical Columns")
        # WritingTheDf
        Cleaningdf.coalesce(1).write.format("csv").mode("overwrite").option("header", "true").save(OutputFile)
        
        # Saving the model
        model.write().overwrite().save(ModelFile+"/Imputer_model")
        
        dfvalue = pd.DataFrame(columnslist, columns = [ColumnNameForFeaturesList])
        create_table(CleansingFeaturesList,dfvalue)
        df_tosql(CleansingFeaturesList,dfvalue)
        
        
        response['Status'] = status.HTTP_200_OK
        response['CleansingStatus'] = ['Cleansing Completed']
        response['OutputFile'] = OutputFile
        response['ModelFile'] = ModelFile
        response['ErrorMessage'] = ''

        logger.info("Cleaning Module Done Successfully")
    except Exception as e:
        response['Status'] = status.HTTP_500_INTERNAL_SERVER_ERROR
        response['CleansingStatus'] = 'Failed'
        logger.error(str(e))
        response['ErrorMessage'] = [str(e), 'Error occured while transposing. Please check the debug logs']
    
    finally:
        audit = CleanserAudit(**response)
        audit.save()
        spark=spark.close()
    return response




def Cleaning_MP(request):
    
    response = request
    try:
        logger.info("Cleaning Module Started Successfuly MP")
        ProcessId = request.get('ProcessId')
        File1 = request.get('File1')
        File2 = request.get('File2')
        OutputFile = request.get('OutputFile')
        ModelFile = request.get('ModelFile')
        TemplateName = request.get('TemplateName')
        AlgorithmName = request.get('AlgorithmName')
        LowerRangePercentage = request.get('LowerRangePercentage')
        UpperRangePercentage = request.get('UpperRangePercentage')
        Strategy = request.get('Strategy')
        OutputFeaturesList = request.get('OutputFeaturesList')
        DateFeatures = request.get('DateFeatures')
        logger.info("Request Got It Successfully For Cleaning Module")
        
        UpperRangePercentage = UpperRangePercentage / 100
        LowerRangePercentage = LowerRangePercentage / 100
        print("UpperRangePercentage")
        print(LowerRangePercentage)
        print(UpperRangePercentage)
        
        # Output_file_Directory
        logger.info("Output Directory Process")
        if "/" in OutputFile:
            OutputFile = "/".join((OutputFile,ProcessId,TemplateName,AlgorithmName,"Cleaning"))
            ModelFile = "/".join((ModelFile,ProcessId,TemplateName,AlgorithmName,"ImputerModel"))
            Path(OutputFile).mkdir(parents=True, exist_ok=True)

        elif "\\"  in OutputFile:
            OutputFile = "\\".join((OutputFile,ProcessId,TemplateName,AlgorithmName,"Cleaning"))
            ModelFile = "\\".join((ModelFile,ProcessId,TemplateName,AlgorithmName,"ImputerModel"))
            Path(OutputFile).mkdir(parents=True, exist_ok=True)
        
        logger.info("Output Directory Process Completed")
        
        # Module process
        logger.info("Cleaning Module Process")
        logger.info("Reading The Input Files")
        InputDataFrame = pd.read_csv(File1)
        Rows_Count,Columns_Count=InputDataFrame.shape
        logger.info("InputDataFrame Rows {} And Columns {}".format(Rows_Count,Columns_Count))
        logger.info("Calling The Reduce Memory Function")
        InputDataFrame=Reduce_Memory_Usage(InputDataFrame)
        
        logger.info("Reading The Output Files")
        OutputDataFrame = pd.read_csv(File2)
        Rows_Count,Columns_Count=OutputDataFrame.shape
        logger.info("OutputDataFrame Rows {} And Columns {}".format(Rows_Count,Columns_Count))
        logger.info("Calling The Reduce Memory Function")
        OutputDataFrame=Reduce_Memory_Usage(OutputDataFrame)        
        
        # Removing the Most Values in Hetrogenious Features and Atleast Values in Homogenious Features
        logger.info("Removing the Most Values in Hetrogenious Features and Atleast Values in Homogenious Features")
        DistinctColumnsCount=InputDataFrame.nunique().to_dict()
        DistinctColumnsCount = dict(sorted(DistinctColumnsCount.items(),key=lambda item : item[1]))
        logger.info("Distinct Count {}".format(DistinctColumnsCount))
        
        UniqueAndNonUniqueColumnList=[]
        for column in DistinctColumnsCount :
            if DistinctColumnsCount[column] <= int(LowerRangePercentage) :
                UniqueAndNonUniqueColumnList.append(column)
            elif DistinctColumnsCount[column] >= int(UpperRangePercentage*Rows_Count) : 
                UniqueAndNonUniqueColumnList.append(column)
        UniqueAndNonUniqueColumnList.extend(DateFeatures)
        logger.info("Count Of the Most Values in Hetrogenious Features and Atleast Values in Homogenious Features Are {}".format(len(UniqueAndNonUniqueColumnList)))
        
        logger.info("Droping The Features")
        print(UniqueAndNonUniqueColumnList)
        InputDataFrame.drop(UniqueAndNonUniqueColumnList,axis=1,inplace=True)
        Rows_Count,Columns_Count=InputDataFrame.shape
        logger.info("InputDataFrame Rows {} And Columns {}".format(Rows_Count,Columns_Count))
        
        logger.info("Creating Droped Features")
        dfvalue = pd.DataFrame(UniqueAndNonUniqueColumnList, columns = ['Columns'])
        create_table('CleansingFeaturesList',dfvalue)
        df_tosql('CleansingFeaturesList',dfvalue)        
        
        # Spliting The Input And Output
        logger.info("Spliting The Input And Output Features")
        OutputDataFrame = OutputDataFrame[OutputFeaturesList]
        logger.info("Output Features Process")
        Rows_Count,Columns_Count=OutputDataFrame.shape
        logger.info("OutputDataFrame Rows {} And Columns {}".format(Rows_Count,Columns_Count))
        logger.info("Output Features Completed")
        
        
        logger.info("Spliting The Categorical And Continous")
        CategoricalDataFrame=InputDataFrame.select_dtypes(include=['object','category'])
        Rows_Count,Columns_Count=CategoricalDataFrame.shape
        logger.info("CategoricalDataFrame Rows {} And Columns {}".format(Rows_Count,Columns_Count))

        ContinousDataFrame=InputDataFrame.select_dtypes(exclude=['object','category'])
        Rows_Count,Columns_Count=ContinousDataFrame.shape
        logger.info("ContinousDataFrame Rows {} And Columns {}".format(Rows_Count,Columns_Count))
        
        logger.info("Imputer Process in CategoricalDataFrame")
        ImputerModel = SimpleImputer(missing_values=np.nan,strategy=Strategy)
        ImputerModel.fit(CategoricalDataFrame)
        CategoricalDataFrameImputerdata = pd.DataFrame(ImputerModel.transform(CategoricalDataFrame))
        # CategoricalDataFrameImputerdata.columns = CategoricalDataFrame.columns
        logger.info("Imputer Process Completed in CategoricalDataFrame")

        logger.info("Saving The CategoricalDataFrame Model")
        with open(OutputFile+"/"+'CATEGORICALDATAFRAME.pkl','wb') as f:
            pickle.dump(ImputerModel,f)
        logger.info("Saving The CategoricalDataFrame Model Completed")

        
        logger.info("Imputer Process in ContinousDataFrame")
        ImputerModel = SimpleImputer(missing_values=np.nan,strategy=Strategy)
        ImputerModel.fit(ContinousDataFrame)
        ContinousDataFrameImputerdata = pd.DataFrame(ImputerModel.transform(ContinousDataFrame))
        # ContinousDataFrameImputerdata.columns = ContinousDataFrame.columns
        logger.info("Imputer Process Completed in CategoricalDataFrame")
        
        logger.info("Saving The ContinousDataFrame Model")
        with open(OutputFile+"/"+'CONTINOUSDATAFRAME.pkl','wb') as f:
            pickle.dump(ImputerModel,f)
        logger.info("Saving The ContinousDataFrame Model Completed")
        
        logger.info("Concating The ContinousDataFrame And CategoricalDataFrame")
        InputDataFrame = pd.concat([ContinousDataFrameImputerdata,CategoricalDataFrameImputerdata],axis=1)
        Rows_Count,Columns_Count=InputDataFrame.shape
        logger.info("InputDataFrame Rows {} And Columns {}".format(Rows_Count,Columns_Count))
        
        logger.info("Exporting The Input And Output DataFrame")
        InputDataFrame.to_csv(OutputFile+"/"+"Input.csv",index=False)
        OutputDataFrame.to_csv(OutputFile+"/"+"Output.csv",index=False)
        
        # create_table(CleansingFeaturesList,dfvalue)
        # df_tosql(CleansingFeaturesList,dfvalue)
        
        
        response['Status'] = status.HTTP_200_OK
        response['CleansingStatus'] = ['Cleansing Completed']
        response['DropedFeatures'] = UniqueAndNonUniqueColumnList
        response['OutputFile'] = OutputFile
        response['ModelFile'] = ModelFile
        response['ErrorMessage'] = ''

        logger.info("Cleaning Module Done Successfully MP")
    except Exception as e:
        response['Status'] = status.HTTP_500_INTERNAL_SERVER_ERROR
        response['CleansingStatus'] = 'Failed'
        logger.error(str(e))
        response['ErrorMessage'] = [str(e), 'Error occured while transposing. Please check the debug logs']
    
    finally:
        audit = CleanserMPAudit(**response)
        audit.save()
    return response
