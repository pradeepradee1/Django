from os import execv
from modeltrain.train import train
from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import OneHotEncoder, VectorAssembler
from pyspark.ml import Pipeline
from rest_framework import status
from modeltrain.models import FeatureselecionAudit
from modeltrain.utils import create_table,df_tosql,get_checksum,get_file_size
import pandas as pd
import logging
from pathlib import Path
from pyspark.ml.feature import ChiSqSelector,UnivariateFeatureSelector

def featureselection(request):

    response = request
    
    try:
        input_file = request.get('input_file')
        output_file = request.get('output_file')
        label_col = request.get('label_col')
        process_id = request.get('process_id')
        product_name = request.get('product_name')
        template_name = request.get('template_name')
        feature_seletion_type = request.get('feature_seletion_type')
        feature_selection_list = request.get('feature_selection_list')

        input_file=input_file.replace("template_name",str(template_name))
        input_file=input_file.replace("process_id",str(process_id))
        input_file=input_file.replace("product_name",str(product_name))

        output_file=output_file.replace("template_name",str(template_name))
        output_file=output_file.replace("process_id",str(process_id))
        output_file=output_file.replace("product_name",str(product_name))

        # Output_file_Directory
        if "/" in output_file:
            Path(output_file).mkdir(parents=True, exist_ok=True)
        elif "\\" in output_file:
            Path(output_file).mkdir(parents=True, exist_ok=True)
        input_file=input_file+"/outliers.csv"
        output_file=output_file+"/feature_selection.csv"          
        
        #Module process
        spark = SparkSession.builder.getOrCreate()
        df = spark.read.csv(input_file,inferSchema =True,header=True)
        print(len(df.columns))

        CONT_COLS_NAMES=[]
        STRING_COLS_NAMES=[]
        for i in df.dtypes:
            if label_col not in i:
                if i[1] == "string":
                    STRING_COLS_NAMES.append(i[0])
                else:
                    CONT_COLS_NAMES.append(i[0])
        
        si = []
        ohe = []
        out_cols = []
        for i in STRING_COLS_NAMES:
            stage = StringIndexer(inputCol= i, outputCol= "".join([i,"_index"]))
            si.append(stage)
            ohe.append(stage.getOutputCol())
            out_cols.append("".join([i,"_encoded"]))
        
        in_out_cols = [*out_cols,*CONT_COLS_NAMES]
        oheout = OneHotEncoder(inputCols=ohe, outputCols= out_cols)
        vecass = VectorAssembler(inputCols=in_out_cols, outputCol='features')
        regression_pipeline = Pipeline(stages=[*si,oheout,vecass])
        ppmodel = regression_pipeline.fit(df)
        data = ppmodel.transform(df)
        print(data.select('features').show(1))
  
        if feature_seletion_type == "Chisquare":
 
            selector = ChiSqSelector(numTopFeatures=feature_selection_list, featuresCol="features",
                        outputCol="selectedFeatures", labelCol=label_col)
        elif feature_seletion_type == "Univariant":

            selector = UnivariateFeatureSelector(featuresCol="features", outputCol="selectedFeatures",
                        labelCol=label_col, selectionMode="numTopFeatures")
            selector.setFeatureType("continuous").setLabelType("categorical").setSelectionThreshold(feature_selection_list)

        

        data = selector.fit(data).transform(data)
        data.select('features','selectedFeatures').show()
        
        #WritingCSV
        data1=data.toPandas()
        data1.to_csv(output_file,index=False)
        df1 = pd.read_csv(output_file)
        # create_table(table_name,df1)
        # df_tosql(table_name,df1)
        file_size = get_file_size(output_file)
        checksum = get_checksum(output_file)

        response['status'] = status.HTTP_200_OK
        response['feature_status'] = ['Feature Selection Completed']
        response['file_size'] = file_size
        response['checksum'] = checksum
        response['error_message'] = ''

    except Exception as e:
        response['status'] = status.HTTP_500_INTERNAL_SERVER_ERROR
        response['feature_status'] = 'Failed'
        response['file_size'] = ''
        response['checksum'] = ''
        response['error_message'] = [str(e), 'Error occured while featureselection. Please check the debug logs']

    finally:
        audit = FeatureselecionAudit(**response)
        audit.save()        
    
    return response
