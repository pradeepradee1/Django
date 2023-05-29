from email.header import Header
from operator import index
from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import OneHotEncoder, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.feature import StandardScaler
from pyspark.ml.classification import RandomForestClassificationModel,DecisionTreeClassificationModel
from pyspark.ml.regression import GBTRegressor,RandomForestRegressor,RandomForestRegressionModel,GBTRegressionModel
from pyspark.ml.evaluation import MulticlassClassificationEvaluator,RegressionEvaluator
from rest_framework import status
from modeltrain.models import TestAudit,TestMPAudit
from modeltrain.utils import create_table,df_tosql,get_checksum,get_file_size
import pandas as pd
import os
from pyspark.ml.feature import ChiSqSelector,UnivariateFeatureSelector
from pathlib import Path
import pickle
from pickle import load
from modeltrain.Reduce_Memory import Reduce_Memory_Usage
import logging
logger = logging.getLogger(__name__)
from sklearn.metrics import accuracy_score
from sklearn.metrics import confusion_matrix


def test(request):
    response = request
    try:        
        
        process_id = request.get('process_id')
        input_file = request.get('input_file')
        output_file = request.get('output_file')
        model_path = request.get('model_path')
        encoding_path = request.get('encoding_path')
        scalar_path = request.get('scalar_path')
        template_name=request.get('template_name')
        product_name=request.get('product_name')
        feature_seletion_type = request.get('feature_seletion_type')
        feature_selection_list = request.get('feature_selection_list')        
        callback_url = request.get('callback_url')
        algorithm_name = request.get('algorithm_name')
        algorithm_type = request.get('algorithm_type')
        label_col = request.get('label_col')
        metrics_name=request.get('metrics_name')
        handleinvalid = request.get('handleinvalid')

        # Output_file_Directory
        output_file = "/".join((output_file,process_id,template_name,product_name,algorithm_name,"Test"))
        # model_path = "/".join((model_path,process_id,template_name,product_name,algorithm_name,"Model"))
        encoding_path = "/".join((encoding_path,process_id,template_name,product_name,algorithm_name,"Encoding"))
        scalar_path = "/".join((scalar_path,process_id,template_name,product_name,algorithm_name,"Scaling"))

        if "/" in encoding_path:
            Path(encoding_path).mkdir(parents=True, exist_ok=True)
        elif "\\" in encoding_path:
            Path(encoding_path).mkdir(parents=True, exist_ok=True)  

        if "/" in scalar_path:
            Path(scalar_path).mkdir(parents=True, exist_ok=True)
        elif "\\" in scalar_path:
            Path(scalar_path).mkdir(parents=True, exist_ok=True)        

        
        spark = SparkSession.builder \
                .master('local[*]') \
                .config("spark.driver.memory", "15g") \
                .appName('cub-app') \
                .getOrCreate()
        
        df = spark.read.csv(input_file,inferSchema =True,header=True)
        
        CONT_COLS_NAMES=[]
        STRING_COLS_NAMES=[]
        for i in df.dtypes:
            if i[0] != label_col:
                if i[1] == "string":
                    STRING_COLS_NAMES.append(i[0])
                else:
                    CONT_COLS_NAMES.append(i[0])

        si = []
        ohe = []
        out_cols = []
        for i in STRING_COLS_NAMES:
            stage = StringIndexer(inputCol= i, outputCol= "".join([i,"_index"]),handleInvalid=handleinvalid)
            si.append(stage)
            ohe.append(stage.getOutputCol())
            out_cols.append("".join([i,"_encoded"]))
        
        in_out_cols = [*out_cols,*CONT_COLS_NAMES]
        oheout = OneHotEncoder(inputCols=ohe, outputCols= out_cols)
        vecass = VectorAssembler(inputCols=in_out_cols, outputCol='features')
        regression_pipeline = Pipeline(stages=[*si,oheout,vecass])
        ppmodel = regression_pipeline.fit(df)
        data = ppmodel.transform(df)
        
        encoding_path=encoding_path+"/Encoding"
        ppmodel.write().overwrite().save(os.path.join(encoding_path))
        
        
        standardscaler=StandardScaler().setInputCol("features").setOutputCol("Scaled_features")
        scalar_fit=standardscaler.fit(data)
        data=scalar_fit.transform(data)
        
        scalar_path=scalar_path+"/Scalar"
        scalar_fit.write().overwrite().save(os.path.join(scalar_path))
        
        train, test = data.randomSplit([0.8, 0.2], seed=12345)


        if algorithm_name == "RFC":
            model =RandomForestClassificationModel.load(os.path.join(model_path))
        elif algorithm_name == "DTC":
            model =DecisionTreeClassificationModel.load(os.path.join(model_path))
        elif algorithm_name == "GBTR":
            model = GBTRegressionModel.load(os.path.join(model_path))
        elif algorithm_name == "RFR":
            model = RandomForestRegressionModel.load(os.path.join(model_path))

        predictions = model.transform(test)
        
        
        if algorithm_type == "Classification" :
            evaluator = MulticlassClassificationEvaluator(
                labelCol=label_col,predictionCol='prediction',metricName=metrics_name
                )
            prediction_score = evaluator.evaluate(predictions)
        
        elif algorithm_type == "Regression" :
            evaluator = RegressionEvaluator(
                labelCol=label_col, predictionCol="prediction", metricName=metrics_name
                )
            prediction_score = evaluator.evaluate(predictions)
        
        
        output_file=output_file+"/test_out.csv"
        resultfortest=predictions.select(label_col,"prediction").toPandas()
        resultfortest.to_csv(output_file,index=False)
        response['status'] = status.HTTP_200_OK
        response['test_status'] = ['Testing Completed']
        response['output_file'] = output_file
        response['prediction_score'] = prediction_score
        response['error_message'] = ''
        
    except Exception as e:
        response['status'] = status.HTTP_500_INTERNAL_SERVER_ERROR
        response['test_status'] = 'Failed'
        response['error_message'] = [str(e), 'Error occured while Testing. Please check the debug logs']

    finally:
        audit = TestAudit(**response)
        audit.save()
    return response




def Test_MP(request):
    response = request
    try:
        logger.info("Testing Module Started Successfuly MP")
        ProcessId = request.get('ProcessId')
        ModelType = request.get('ModelType')
        TestingFile = request.get('TestingFile')
        ActualFile = request.get('ActualFile')
        OutputFile = request.get('OutputFile')
        ModelFile = request.get('ModelFile')
        TemplateName = request.get('TemplateName')
        AlgorithmName = request.get('AlgorithmName')        
        AlgorithmType = request.get('AlgorithmType')     
        logger.info("Request Got It Successfully For Testing Module")
        
        # Output_file_Directory
        logger.info("Output Directory Process")
        if "/" in OutputFile:
            OutputFile = "/".join((OutputFile,ProcessId,TemplateName,AlgorithmName,"Testing"))
            Path(OutputFile).mkdir(parents=True, exist_ok=True)

        elif "\\"  in OutputFile:
            OutputFile = "\\".join((OutputFile,ProcessId,TemplateName,AlgorithmName,"Testing"))
            Path(OutputFile).mkdir(parents=True, exist_ok=True)
        logger.info("Output Directory Process Completed")   
        
        # Module process
        logger.info("Testing Module Process")
        logger.info("Reading The TestingDataFrame Files")
        TestingDataFrame = pd.read_csv(TestingFile)
        Rows_Count,Columns_Count=TestingDataFrame.shape
        logger.info("TestingDataFrame Rows {} And Columns {}".format(Rows_Count,Columns_Count))
        logger.info("Calling The Reduce Memory Function")
        # TestingDataFrame=Reduce_Memory_Usage(TestingDataFrame)

        logger.info("Reading The Actual Files")
        ActualDataFrame = pd.read_csv(ActualFile)
        Rows_Count,Columns_Count=ActualDataFrame.shape
        logger.info("ActualDataFrame Rows {} And Columns {}".format(Rows_Count,Columns_Count))
        logger.info("Calling The Reduce Memory Function")
        # ActualDataFrame=Reduce_Memory_Usage(ActualDataFrame)
        ActualDataFrameColumns=list(ActualDataFrame.columns)
        print(ActualDataFrameColumns)
        for ActualColumns in range(len(ActualDataFrameColumns)):
            ActualDataFrame.rename(columns={ActualDataFrameColumns[ActualColumns]:ActualColumns},inplace=True)
        ActualDataFrameColumns=list(ActualDataFrame.columns)
        print(ActualDataFrameColumns)
        logger.info("Loading The Model Process")
        Model = load(open(ModelFile, 'rb'))
        TestedDataFrame = pd.DataFrame(Model.predict(TestingDataFrame))
        logger.info("Loading The Model Completed")
        
        
        logger.info("Accuracy Details")
        AccuracyDetails={}
        for i in range(len(ActualDataFrame.columns)):
            Accuracy=str(accuracy_score(ActualDataFrame[i],TestedDataFrame[i]))
            AccuracyDetails[ActualDataFrameColumns[i]]=[Accuracy]
            logger.info("Accuracy {}".format(Accuracy))
        
        AccuracyDataFrame=pd.DataFrame(AccuracyDetails)
        logger.info("Creating DataFrame For Accuracy Details")
        logger.info("Exporting DataFrame For Accuracy Details")
        AccuracyDataFrame.to_csv(OutputFile+"/"+"AccuracyDataFrame.csv",index=False)
        logger.info("Completed Exporting DataFrame For Accuracy Details")
        
        logger.info("Confussion Matrix Details")
        logger.info("Exporting The Confussion Matrix Details")
        ConfussionDetails={}
        for i in range(len(ActualDataFrame.columns)):
            Confussion_Matrix=confusion_matrix(ActualDataFrame[i],TestedDataFrame[i])
            ConfussionDetails[ActualDataFrameColumns[i]]=pd.DataFrame(Confussion_Matrix)
            logging.info("Confussion Matrix {}".format(Confussion_Matrix))
        logger.info("Completed Exporting The Confussion Matrix Details")
        for i in ConfussionDetails:
            print(i)
            ConfussionDetails[i].to_csv(OutputFile+"/"+str(i)+".csv",index=False,header=False)

        TestedDataFrame.to_csv(OutputFile+"/"+"TestedDataFrame.csv",index=False)
        
        response['status'] = status.HTTP_200_OK
        response['test_status'] = ['Testing Completed']
        response['OutputFile'] = OutputFile
        response['error_message'] = ''
        
    except Exception as e:
        response['status'] = status.HTTP_500_INTERNAL_SERVER_ERROR
        response['test_status'] = 'Failed'
        response['error_message'] = [str(e), 'Error occured while Testing. Please check the debug logs']

    finally:
        audit = TestMPAudit(**response)
        audit.save()
    return response
