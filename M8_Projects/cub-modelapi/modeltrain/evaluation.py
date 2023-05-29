from modeltrain.scalar import scalar
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.ml.feature import ImputerModel
from pyspark.ml.feature import StandardScalerModel
from pyspark.ml.classification import RandomForestClassificationModel,DecisionTreeClassificationModel
from pyspark.ml.regression import RandomForestRegressionModel,GBTRegressionModel
from rest_framework import status
from modeltrain.models import EvaluationAudit,EvaluationMPAudit
from modeltrain.utils import create_table,df_tosql,get_checksum,get_file_size
import pandas as pd
import numpy as np
import os
from pathlib import Path
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType
from pyspark.sql.functions import monotonically_increasing_id
import pyspark.sql.functions as F
from modeltrain.utils import get_values_from_tables
from pyspark.sql.types import IntegerType
import logging
logger = logging.getLogger(__name__)
import pickle
from pickle import load
from modeltrain.Reduce_Memory import Reduce_Memory_Usage


def evaluation(request):
    response = request
    try:
        logger.info("Evaluation Module Started Successfuly")
        process_id = request.get('process_id')
        input_file = request.get('input_file')
        Imputer_path = request.get('Imputer_path')
        encoding_path = request.get('encoding_path')
        scalar_path = request.get('scalar_path')   
        model_path = request.get('model_path')
        output_file = request.get('output_file')
        algorithm_name = request.get('algorithm_name')
        algorithm_type = request.get('algorithm_type')
        label_col = request.get('label_col')
        output_features = request.get('output_features')
        table_name = request.get('table_name')
        template_name = request.get('template_name')
        Cleansing_Features_list=request.get('Cleansing_Features_list')
        product_name=request.get('product_name')
        customer_case=request.get('customer_case')
        Customer_id=request.get('Customer_id')
        purpose=request.get('purpose')
        
        output_file= "/".join((output_file,process_id,template_name,product_name,algorithm_name,"Evaluation"))
        table_name =str(process_id)+"_"+str(template_name)+"_"+str(product_name)+"_"+str(label_col)+"_"+str(table_name)
        #Output_file_Directory
        if "/" in output_file :
            Path(output_file).mkdir(parents=True, exist_ok=True)
        elif "\\" in output_file :
            Path(output_file).mkdir(parents=True, exist_ok=True)        
        
        logger.info("Request Got It Successfully For Evaluation Module")
        
        #Module process
        spark = SparkSession.builder \
                .master('local[*]') \
                .config("spark.driver.extraJavaOptions", "-Xss4M") \
                .config("spark.driver.memory", "14g") \
                .appName('cub-app') \
                .getOrCreate()

        logger.info("Spark Session Created Successfully For Evaluation Module")
        
        df = spark.read.csv(input_file,inferSchema =True,header=True)

        logger.info("DataFrame Read Successfully for Evaluation Module")
        
        if customer_case == "Brand_New" :
            for i in df.columns:
                if i.startswith("M"):
                    df = df.withColumn(i, df[i].cast(IntegerType()))       
        
        customerdetails=df.select(Customer_id)
        customerdetails.show()
        
        CONT_COLS_NAMES=[]
        STRING_COLS_NAMES=[]
        for i in df.dtypes:
            if i[1] == "string":
                STRING_COLS_NAMES.append(i[0])
            else:
                CONT_COLS_NAMES.append(i[0])
        
        model=ImputerModel.load(Imputer_path)
        df=model.transform(df)

        for col_name in STRING_COLS_NAMES:
            common = df.dropna().groupBy(col_name).agg(F.count("*")).orderBy('count(1)', ascending=False).first()[col_name]
            df = df.withColumn(col_name, F.when(F.isnull(col_name), common).otherwise(df[col_name]))
        
        logger.info("Imputer Model and Null Values files Successfully")
        #Outliers
        columnslist=get_values_from_tables(Cleansing_Features_list)
        columnslist.append(label_col)
        
        for i in columnslist:
            df=df.drop(i)
        
        
        #Module_process_encoding
        ppmodel=PipelineModel.load(os.path.join(encoding_path))
        data = ppmodel.transform(df)
        logger.info("Encoding Transform Completed")

        #Module_process_scalar
        scalarmodel=StandardScalerModel.load(os.path.join(scalar_path))
        data = scalarmodel.transform(data)
        logger.info("Scalar Transform Completed")

        #Module_process_Model
        if algorithm_name == "RFC":
            model =RandomForestClassificationModel.load(os.path.join(model_path))
        elif algorithm_name == "DTC":
            model = DecisionTreeClassificationModel.load(os.path.join(model_path))
        elif algorithm_name == "GBTR":
            model = GBTRegressionModel.load(os.path.join(model_path))
        elif algorithm_name == "RFR":
            model = RandomForestRegressionModel.load(os.path.join(model_path))

        predictions = model.transform(data)
        logger.info("Model Transform Completed")

        if algorithm_type == "Classification":
            find_max = udf(lambda x: float(np.max(np.abs(x))*100), FloatType())
            result=predictions.select('prediction',find_max("probability").alias("Score %"))
            result.show()
            result = result.withColumnRenamed("prediction", "LOAN_CLASS")\
                    .withColumnRenamed("Score %", "LOAN_RATING")
            result.show()
        else:
            result=predictions.select('prediction')
            result = result.withColumnRenamed("prediction", "PRINCIPAL_AMOUNT")
            result.show()
        
        customerdetailsdf=customerdetails.withColumn("row_id",monotonically_increasing_id())
        resultdf=result.withColumn("row_id",monotonically_increasing_id())
        final_result_df = customerdetailsdf.join(resultdf, ("row_id")).drop("row_id")
        

        # Writing CSV 
        output_file = output_file+"/"+process_id+"_"+str(label_col)+"_output.csv"
        pd_predictions=final_result_df.toPandas()
        pd_predictions.to_csv(output_file,index=False)
        df1 = pd.read_csv(output_file)

        create_table(table_name,df1)
        df_tosql(table_name,df1)
        
        output_file_size = get_file_size(output_file)
        output_file_checksum = get_checksum(output_file)
        
        response['status'] = status.HTTP_200_OK
        response['evaluation_status'] = ['Evaluation Completed']
        response['output_file'] = output_file
        response['prediction_score'] = ''
        response['output_file_size'] = output_file_size
        response['output_file_checksum'] = output_file_checksum
        response['error_message'] = ''
        logger.info("Evaluation Module Completed")
        
    except Exception as e:
        response['status'] = status.HTTP_500_INTERNAL_SERVER_ERROR
        response['evaluation_status'] = 'Failed'
        response['prediction_score'] = ''
        response['output_file_size'] = ''
        response['output_file_checksum'] = ''
        logger.error(str(e))
        response['error_message'] = [str(e), 'Error occured while Testing. Please check the debug logs']

    finally:
        audit = EvaluationAudit(**response)
        audit.save()
    return response






def EvaluationMP(request):
    response = request
    try:
        logger.info("Evaluation Module Started Successfuly MP")
        ProcessId = request.get('ProcessId')
        ModelType = request.get('ModelType')
        InputFile = request.get('InputFile')
        CategoricalImputerFile = request.get('CategoricalImputerFile')
        ContinousImputerFile = request.get('ContinousImputerFile')
        EncodingFile = request.get('EncodingFile')
        ScalingFile = request.get('ScalingFile')
        ModelFile = request.get('ModelFile')        
        OutputFile = request.get('OutputFile')
        DropedFeatures = request.get('DropedFeatures')
        CustomerIdFeatures = request.get('CustomerIdFeatures')
        TemplateName = request.get('TemplateName')
        AlgorithmName = request.get('AlgorithmName')        
        AlgorithmType = request.get('AlgorithmType')             
        logger.info("Request Got It Successfully For Evaluation Module MP ")

        # Output_file_Directory
        logger.info("Output Directory Process")
        if "/" in OutputFile:
            OutputFile = "/".join((OutputFile,ProcessId,TemplateName,AlgorithmName,"Evaluation"))
            Path(OutputFile).mkdir(parents=True, exist_ok=True)

        elif "\\"  in OutputFile:
            OutputFile = "\\".join((OutputFile,ProcessId,TemplateName,AlgorithmName,"Evaluation"))
            Path(OutputFile).mkdir(parents=True, exist_ok=True)
        logger.info("Output Directory Process Completed")           

        # Module process
        logger.info("Evaluation Module Process")
        logger.info("Reading The InputDataFrame Files")
        InputDataFrame = pd.read_csv(InputFile)
        Rows_Count,Columns_Count=InputDataFrame.shape
        logger.info("InputDataFrame Rows {} And Columns {}".format(Rows_Count,Columns_Count))
        logger.info("Calling The Reduce Memory Function")
        # InputDataFrame=Reduce_Memory_Usage(InputDataFrame)
        
        # Removing the Most Values in Hetrogenious Features and Atleast Values in Homogenious Features
        logger.info("Removing the Most Values in Hetrogenious Features and Atleast Values in Homogenious Features")
        logger.info("Droping The Features")
        columnslist=get_values_from_tables('CleansingFeaturesList')
        print(columnslist)
        CustomerID=InputDataFrame[CustomerIdFeatures]
        InputDataFrame.drop(columnslist,axis=1,inplace=True)
        Rows_Count,Columns_Count=InputDataFrame.shape
        logger.info("InputDataFrame Rows {} And Columns {}".format(Rows_Count,Columns_Count))
        
        logger.info("Spliting The Categorical And Continous")
        CategoricalDataFrame=InputDataFrame.select_dtypes(include=['object'])
        Rows_Count,Columns_Count=CategoricalDataFrame.shape
        logger.info("CategoricalDataFrame Rows {} And Columns {}".format(Rows_Count,Columns_Count))


        ContinousDataFrame=InputDataFrame.select_dtypes(include=['int','float'])
        Rows_Count,Columns_Count=ContinousDataFrame.shape
        logger.info("ContinousDataFrame Rows {} And Columns {}".format(Rows_Count,Columns_Count))



        logger.info("Loading The Imputer Process in CategoricalDataFrame")
        CategoricalImputerModel = pickle.load(open(CategoricalImputerFile,'rb'))
        CategoricalDataFrameImputerdata = pd.DataFrame(CategoricalImputerModel.transform(CategoricalDataFrame))
        # CategoricalDataFrameImputerdata.columns = CategoricalDataFrame.columns
        logger.info("Imputer Process Completed in CategoricalDataFrame")     


        logger.info("Loading The Imputer Process in ContinousDataFrame")
        ContinousImputerModel = pickle.load(open(ContinousImputerFile,'rb'))
        ContinousDataFrameImputerdata = pd.DataFrame(ContinousImputerModel.transform(ContinousDataFrame))
        # ContinousDataFrameImputerdata.columns = ContinousDataFrame.columns
        logger.info("Imputer Process Completed in ContinousDataFrame")

        
        logger.info("Loading The Encoding Process")
        EncodingFile = pickle.load(open(EncodingFile,'rb'))
        CategoricalEncodedDataFrame=pd.DataFrame(EncodingFile.transform(CategoricalDataFrameImputerdata))
        # CategoricalEncodedDataFrame.columns=EncodingFile.get_feature_names_out()
        Rows_Count,Columns_Count=CategoricalEncodedDataFrame.shape
        logger.info("CategoricalEncodedDataFrame Rows {} And Columns {}".format(Rows_Count,Columns_Count))
        logger.info("Encoding Process Completed")


        logger.info("Concating The ContinousDataFrame And CategoricalDataFrame")
        InputDataFrame = pd.concat([ContinousDataFrameImputerdata,CategoricalEncodedDataFrame],axis=1)
        Rows_Count,Columns_Count=InputDataFrame.shape
        logger.info("InputDataFrame Rows {} And Columns {}".format(Rows_Count,Columns_Count))        
 

        logger.info("Loading Scaling Module Process")
        ScalingFile = pickle.load(open(ScalingFile,'rb'))
        ScalerDataFrame = pd.DataFrame(ScalingFile.transform(InputDataFrame))
        # ScalerDataFrame.columns=ScalingFile.get_feature_names_out()
        Rows_Count,Columns_Count=ScalerDataFrame.shape
        logger.info("ScalingDataFrame Rows {} And Columns {}".format(Rows_Count,Columns_Count))
        logger.info("Scaling Process Completed")        
        

        logger.info("Loading The Model")
        # ScalerDataFrame.to_csv(OutputFile+"/"+"Eval.csv",index=False)
        Model = pickle.load(open(ModelFile,'rb'))
        Prediction = pd.DataFrame(Model.predict(ScalerDataFrame))
        
        logger.info("Adding CustomerID")
        Prediction = pd.concat([CustomerID,Prediction],axis=1)
        
        logger.info("Exporting The Prediction Result")
        Prediction.to_csv(OutputFile+'/'+'Prediction.csv',index=False)
        

        response['status'] = status.HTTP_200_OK
        response['evaluation_status'] = ['Evaluation Completed']
        response['OutputFile'] = OutputFile
        response['error_message'] = ''
        logger.info("Evaluation Module Completed")
        
    except Exception as e:
        response['status'] = status.HTTP_500_INTERNAL_SERVER_ERROR
        response['evaluation_status'] = 'Failed'
        logger.error(str(e))
        response['error_message'] = [str(e), 'Error occured while Testing. Please check the debug logs']

    finally:
        audit = EvaluationMPAudit(**response)
        audit.save()
    return response
