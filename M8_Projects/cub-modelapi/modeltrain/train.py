from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import OneHotEncoder, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.feature import StandardScaler
from pyspark.ml.classification import RandomForestClassifier,DecisionTreeClassifier
from pyspark.ml.regression import GBTRegressor,RandomForestRegressor
from pyspark.ml.evaluation import MulticlassClassificationEvaluator,RegressionEvaluator
from modeltrain.models import TrainAudit,TrainMPAudit
from rest_framework import status
from modeltrain.utils import create_table,df_tosql,get_checksum,get_file_size
import os
import pandas as pd
from pathlib import Path
import pickle
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.neighbors import KNeighborsClassifier
import xgboost as xgb
import logging
from modeltrain.Reduce_Memory import Reduce_Memory_Usage
import logging
logger = logging.getLogger(__name__)


def train(request):

    response = request
    
    try:
        logger.info("Training Module Started Successfuly")
        process_id = request.get('process_id')
        input_file = request.get('input_file')
        output_file = request.get('output_file')
        train_file = request.get('train_file')
        test_file = request.get('test_file')
        model_path = request.get('model_path')
        encoding_path = request.get('encoding_path')
        feature_selection_path = request.get('feature_selection_path')
        scalar_path = request.get('scalar_path')
        template_name = request.get('template_name')
        product_name = request.get('product_name')
        feature_seletion_type = request.get('feature_seletion_type')
        feature_selection_list = request.get('feature_selection_list')
        train_table_name = request.get('train_table_name')
        test_table_name = request.get('test_table_name')
        callback_url = request.get('callback_url')
        algorithm_name = request.get('algorithm_name')
        algorithm_type = request.get('algorithm_type')  
        max_depth = int(request.get('max_depth'))
        max_bins = int(request.get('max_bins'))
        num_trees = int(request.get('num_trees'))
        label_col = request.get('label_col')
        metrics_name=request.get('metrics_name')
        feature_col = request.get('feature_col')
        handleinvalid = request.get('handleinvalid')
        HeapMemorySize = request.get('HeapMemorySize')
        StackMemorySize = request.get('StackMemorySize')        
        
        train_table_name = process_id+"_"+ product_name +"_"+ train_table_name
        test_table_name = process_id+"_"+ product_name +"_"+ test_table_name

        output_file = "/".join((output_file,process_id,template_name,product_name,algorithm_name,"Train"))
        
        train_file = "/".join((train_file,process_id,template_name,product_name,algorithm_name,"Train"))
       
        test_file = "/".join((test_file,process_id,template_name,product_name,algorithm_name,"Test"))

        model_path = "/".join((model_path,process_id,template_name,product_name,algorithm_name,"Model"))

        encoding_path = "/".join((encoding_path,process_id,template_name,product_name,algorithm_name,"Encoding"))

        scalar_path = "/".join((scalar_path,process_id,template_name,product_name,algorithm_name,"Scaling"))
        # Output_file_Directory
        if "/" in train_file:
            Path(train_file).mkdir(parents=True, exist_ok=True)
        elif "\\" in train_file:
            Path(train_file).mkdir(parents=True, exist_ok=True)
        
        if "/" in test_file:
            Path(test_file).mkdir(parents=True, exist_ok=True)
        elif "\\" in test_file:
            Path(test_file).mkdir(parents=True, exist_ok=True)
        
        if "/" in model_path:
            Path(model_path).mkdir(parents=True, exist_ok=True)
        elif "\\" in model_path:
            Path(model_path).mkdir(parents=True, exist_ok=True)

        if "/" in encoding_path:
            Path(encoding_path).mkdir(parents=True, exist_ok=True)
        elif "\\" in encoding_path:
            Path(encoding_path).mkdir(parents=True, exist_ok=True)  

        if "/" in scalar_path:
            Path(scalar_path).mkdir(parents=True, exist_ok=True)
        elif "\\" in scalar_path:
            Path(scalar_path).mkdir(parents=True, exist_ok=True)        

        logger.info("Request Got It Successfully For Train Module")
        train_file=train_file+"/train.csv"     
        test_file=test_file+"/test.csv"     

        #Module process
        spark = SparkSession.builder \
                .master('local[*]') \
                .config("spark.driver.extraJavaOptions", StackMemorySize) \
                .config("spark.driver.memory", HeapMemorySize) \
                .appName('cub-app') \
                .getOrCreate()
        
        logger.info("Spark Session Created Successfully For Train Module")
        
        df = spark.read.csv(input_file,inferSchema =True,header=True)

        logger.info("DataFrame Read Successfully for Train Module")
        
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
        
        logger.info("Encoding Done Successfully")

        standardscaler=StandardScaler().setInputCol("features").setOutputCol("Scaled_features")
        scalar_fit=standardscaler.fit(data)
        data=scalar_fit.transform(data)
        
        scalar_path=scalar_path+"/Scalar"
        scalar_fit.write().overwrite().save(os.path.join(scalar_path))
        
        logger.info("Scalar Done Successfully")

        train, test = data.randomSplit([0.8, 0.2], seed=12345)

        logger.info("Train and Test DataFrame Successfully")
        
        if algorithm_name == "RFC":
            model_al = RandomForestClassifier(labelCol=label_col,
                                    featuresCol=feature_col,
                                    maxDepth=max_depth,maxBins=max_bins,numTrees=num_trees)
        elif algorithm_name == "DTC":
            
            model_al = DecisionTreeClassifier(labelCol=label_col,
                                    featuresCol=feature_col)
        elif algorithm_name == "GBTR":
            model_al = GBTRegressor(labelCol=label_col,
                                    featuresCol=feature_col)
        
        elif algorithm_name == "RFR":
            model_al = RandomForestRegressor(labelCol=label_col,
                                    featuresCol=feature_col,
                                    maxDepth=max_depth,maxBins=max_bins,numTrees=num_trees)
        
        model = model_al.fit(train)
        
        model_path=model_path+"/"+str(algorithm_name)+"_"+str(label_col)+"_"+"Model"
        model.save(os.path.join(model_path))        
        
        logger.info("Model Train Successfully")
        
        predictions = model.transform(train)       
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
        
        output_file=output_file+"/train_out.csv"
        resultfortrain=predictions.select(label_col,"prediction").toPandas()
        resultfortrain.to_csv(output_file,index=False)        
        
        #Writing_CSV
        pd_train=train.toPandas()
        pd_test=test.toPandas()
        pd_train.to_csv(train_file,index=False)
        pd_test.to_csv(test_file,index=False)
        
        # df1 = pd.read_csv(train_file)
        # df2 = pd.read_csv(test_file)
        # create_table(train_table_name,pd_train)
        # df_tosql(train_table_name,pd_train)
        # create_table(test_table_name,pd_test)
        # df_tosql(test_table_name,pd_test)
        
        train_file_size = get_file_size(train_file)
        test_file_size = get_file_size(test_file)
        train_checksum = get_checksum(train_file)
        test_checksum = get_checksum(test_file)

        response['status'] = status.HTTP_200_OK
        response['train_status'] = ['Training Completed']
        response['train_file'] = train_file
        response['output_file'] = output_file
        response['test_file'] = test_file
        response['encoding_path'] = encoding_path
        response['model_path'] = model_path
        response['scalar_path'] = scalar_path
        response['train_file_size'] = train_file_size
        response['train_checksum'] = train_checksum
        response['test_file_size'] = test_file_size
        response['test_checksum'] = test_checksum
        response['prediction_score'] = prediction_score
        response['error_message'] = ''
        logger.info("Train Module End Successfully")

    except Exception as e:
        response['status'] = status.HTTP_500_INTERNAL_SERVER_ERROR
        response['train_status'] = 'Failed'
        response['train_file_size'] = ''
        response['train_checksum'] = ''
        response['test_file_size'] = ''
        response['test_checksum'] = ''
        response['prediction_score'] = ''
        logger.error(str(e))

        response['error_message'] = [str(e), 'Error occured while Training. Please check the debug logs']

    finally:
        audit = TrainAudit(**response)
        audit.save()
    return response








def Train_MP(request):
    response = request
    try:
        logger.info("Training Module Started Successfuly MP")
        ProcessId = request.get('ProcessId')
        ModelType = request.get('ModelType')
        File1 = request.get('File1')
        File2 = request.get('File2')
        OutputFile = request.get('OutputFile')
        ModelFile = request.get('ModelFile')
        TemplateName = request.get('TemplateName')
        AlgorithmName = request.get('AlgorithmName')
        TestSize = request.get('TestSize')
        RandomStatePattern = request.get('RandomStatePattern')
        logger.info("Request Got It Successfully For Training Module")
        
        # Output_file_Directory
        logger.info("Output Directory Process")
        if "/" in OutputFile:
            OutputFile = "/".join((OutputFile,ProcessId,TemplateName,AlgorithmName,"Training"))
            ModelFile = "/".join((ModelFile,ProcessId,TemplateName,AlgorithmName,"TrainingModel"))
            Path(OutputFile).mkdir(parents=True, exist_ok=True)

        elif "\\"  in OutputFile:
            OutputFile = "\\".join((OutputFile,ProcessId,TemplateName,AlgorithmName,"Training"))
            ModelFile = "\\".join((ModelFile,ProcessId,TemplateName,AlgorithmName,"TrainingModel"))
            Path(OutputFile).mkdir(parents=True, exist_ok=True)
        logger.info("Output Directory Process Completed")    
        
        # Module process
        logger.info("Training Module Process")
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
        
        logger.info("Spliting The Training And Testing")
        TestSize = int(TestSize) / 100
        RandomStatePattern = int(RandomStatePattern)
        
        InputDataFrame.to_csv(OutputFile+"/"+"Train.csv",index=False)
        X_train ,X_test , y_train ,y_test = train_test_split(InputDataFrame,OutputDataFrame,test_size = TestSize,random_state=RandomStatePattern)
        
        logger.info("Train And Testing Shape Details")
        Rows,Columns=X_train.shape
        logger.info("XTrain Rows {} And Columns {}".format(Rows,Columns))
        Rows,Columns=X_test.shape
        logger.info("Xtest Rows {} And Columns {}".format(Rows,Columns))
        Rows,Columns=y_train.shape
        logger.info("ytrain Rows {} And Columns {}".format(Rows,Columns))
        Rows,Columns=y_test.shape
        logger.info("ytest Rows {} And Columns {}".format(Rows,Columns))        

        logger.info("Model Training Started")
        if AlgorithmName.upper() or AlgorithmName.lower() == "RFC" :
            Model = RandomForestClassifier(random_state=42)
        elif AlgorithmName.upper() or AlgorithmName.lower() == "KNN" :
            Model = KNeighborsClassifier()
        elif AlgorithmName.upper() or AlgorithmName.lower() == "XGB" :
            Model = xgb.XGBClassifier()
        Model.fit(X_train,y_train)
        logger.info("Model Training Completed")
        
        logger.info("Saving The Model")
        with open(OutputFile+"/"+'MODEL.pkl','wb') as f:
            pickle.dump(Model,f)        
        logger.info("Completed Saving The Model")
        
        logger.info("Exporting The Test DataFrame")
        X_test.to_csv(OutputFile+"/"+"xtest.csv",index=False)
        y_test.to_csv(OutputFile+"/"+"ytest.csv",index=False)
        
        
        response['status'] = status.HTTP_200_OK
        response['train_status'] = ['Training Completed']
        response['error_message'] = ''
        logger.info("Train Module End Successfully")

    except Exception as e:
        response['status'] = status.HTTP_500_INTERNAL_SERVER_ERROR
        response['train_status'] = 'Failed'
        logger.error(str(e))
        response['error_message'] = [str(e), 'Error occured while Training. Please check the debug logs']

    finally:
        audit = TrainMPAudit(**response)
        audit.save()
    return response
