from modeltrain.models import TransposeMPAudit
from rest_framework import status
import pandas as pd
import numpy as np
from pathlib import Path
from pickle import dump
from sklearn.preprocessing import LabelEncoder
import gc
from tqdm import tqdm
from memory_profiler import profile
import pickle
from modeltrain.Reduce_Memory import Reduce_Memory_Usage


import logging
logger = logging.getLogger(__name__)

def Transpose_MP(request):
    response = request
    try:
        logger.info("Transpose Module Starting Successfuly MP")
        ProcessId = request.get('ProcessId')
        InputDataFrame = request.get('InputFile')
        OutputDataFrame = request.get('OutputFile')
        OutputDirectory = request.get('OutputDirectory')
        InputUniqueValuesColumns = request.get('InputUniqueValuesColumns')
        InputTransposeValues = request.get('InputTransposeValues')
        InputTransposeColumns = request.get('InputTransposeColumns')
        LabelEncodingColumns = request.get('LabelEncodingColumns')
        LabelEncodingColumnsFillValue = request.get('LabelEncodingColumnsFillValue')
        OutputTransposeIndex = request.get('OutputTransposeIndex')
        OutputTransposeColumn = request.get('OutputTransposeColumn')
        OutputTransposeValues = request.get('OutputTransposeValues')
        ModuleName = request.get('ModuleName')
        Template = request.get('Template')
        Product = request.get('Product')  
        CallbackURL = request.get('CallbackURL')  
        logger.info("Transpose Module Got The Request Successfully")

        # Output_file_Directory
        
        if "/" in OutputDirectory:
            OutputDirectory = "/".join((OutputDirectory,ProcessId,Template,ModuleName))
            Path(OutputDirectory).mkdir(parents=True, exist_ok=True)
        elif "\\"  in OutputDirectory:
            OutputDirectory = "\\".join((OutputDirectory,ProcessId,Template,ModuleName))
            Path(OutputDirectory).mkdir(parents=True, exist_ok=True)
        
        #Module_Process
        logger.info("Reading The InputDF Files")
        InputDataFrame = pd.read_csv(InputDataFrame)
        InputDataFrame[LabelEncodingColumns[0]].fillna(value=LabelEncodingColumnsFillValue,inplace=True)
        InputDataFrame[LabelEncodingColumns[1]].fillna(value=LabelEncodingColumnsFillValue,inplace=True)
        Rows_Count,Columns_Count=InputDataFrame.shape
        logger.info("InputDataFrame Rows {} And Columns {}".format(Rows_Count,Columns_Count))
        logger.info("Calling The Reduce Memory Function")
        # InputDataFrame=Reduce_Memory_Usage(InputDataFrame)
        
        logger.info("Reading The OutputDF Files")
        OutputDataFrame = pd.read_csv(OutputDataFrame)
        OutputDataFrame = OutputDataFrame[[OutputTransposeIndex,OutputTransposeColumn,OutputTransposeValues]]
        Rows_Count,Columns_Count=OutputDataFrame.shape
        logger.info("OutputDataFrame Rows {} And Columns {}".format(Rows_Count,Columns_Count))
        logger.info("Calling The Reduce Memory Function")
        # OutputDataFrame=Reduce_Memory_Usage(OutputDataFrame)
        
        logger.info("Spliting The Unique Values And Non-Unique Values Columns In The InputDataFrame")
        Unique_Values_DF = InputDataFrame[InputUniqueValuesColumns]
        Unique_Values_DF = Unique_Values_DF.drop_duplicates(subset=InputUniqueValuesColumns[0])
        Rows,Columns = Unique_Values_DF.shape
        logger.info("Unique_Value Rows {} And Columns {}".format(Rows,Columns))

        logger.info("Label_Encoding_Process In The InputDataFrame")
        Label_Encoder=LabelEncoder()
        Label_Encoder=Label_Encoder.fit(InputDataFrame[LabelEncodingColumns[0]])
        InputDataFrame[LabelEncodingColumns[0]]=Label_Encoder.transform(InputDataFrame[LabelEncodingColumns[0]])
        logger.info("Saving_The_Label_Encoding_Process")
        with open(OutputDirectory+"/"+LabelEncodingColumns[0]+'_ENCODING.pkl','wb') as f:
            pickle.dump(Label_Encoder,f)
        
        logger.info("Label_Encoding_Process")
        Label_Encoder=LabelEncoder()
        Label_Encoder=Label_Encoder.fit(InputDataFrame[LabelEncodingColumns[1]])
        InputDataFrame[LabelEncodingColumns[1]]=Label_Encoder.transform(InputDataFrame[LabelEncodingColumns[1]])
        logger.info("Saving_The_Label_Encoding_Process")
        with open(OutputDirectory+"/"+LabelEncodingColumns[1]+'_ENCODING.pkl','wb') as f:
            pickle.dump(Label_Encoder,f)
        logger.info("Completed_The_Label_Encoding_Process")
        
        logger.info("Pivoting The DataFrame Process In The InputDataFrame")
        Rows_Count,Columns_Count=InputDataFrame.shape
        logger.info("InputDataFrame Rows {} And Columns {}".format(Rows_Count,Columns_Count))
        print(InputDataFrame.dtypes)
        FinalPivotDF=pd.DataFrame()
        for i in range(len(InputTransposeValues)):
            PivotDF=pd.DataFrame()
            if i==0:
                IPData=InputDataFrame.pivot_table(index=[InputUniqueValuesColumns[0]],values=[str(InputTransposeValues[i])],columns=InputTransposeColumns,fill_value=0)
                FinalPivotDF = IPData
                FinalPivotDF.columns=[str(s2)+"_"+ str(s1) for(s1,s2) in FinalPivotDF.columns.tolist()]
                FinalPivotDF.reset_index(inplace=True)
            else:
                IPData1=InputDataFrame.pivot_table(index=[InputUniqueValuesColumns[0]],values=[str(InputTransposeValues[i])],columns=InputTransposeColumns,fill_value=0)
                IPData1.columns=[str(s2)+"_"+ str(s1) for(s1,s2) in IPData1.columns.tolist()]
                IPData1.reset_index(inplace=True)
                PivotDF = pd.merge(FinalPivotDF,IPData, on=[InputUniqueValuesColumns[0]], how="outer")
                FinalPivotDF = PivotDF
        
        Rows_Count,Columns_Count=FinalPivotDF.shape
        logger.info("Pivoting_DF Rows {} And Columns {}".format(Rows_Count,Columns_Count))
        logger.info("Pivoting The DataFrame Completed In The InputDataFrame")
        
        logger.info("Merging The Unique And Pivoting DataFrame Process In The InputDataFrame")
        InputDataFrame = pd.merge(Unique_Values_DF,FinalPivotDF,on=[InputUniqueValuesColumns[0]],how="inner")
        Rows_Count,Columns_Count=InputDataFrame.shape
        logger.info("InputDataFrame Rows {} And Columns {}".format(Rows_Count,Columns_Count))
        logger.info("Merging The Unique And Pivoting DataFrame Completed")

        logger.info("Output DataFrame Process")
        #Replacing The Fortnight Values In Output DF
        OutputRatingLabel={}
        OutputFortNightValues=set(OutputDataFrame[OutputTransposeColumn].values)
        FortNightCount=0
        for FN_Value in OutputFortNightValues:
            FortNightCount+=1
            OutputRatingLabel[FN_Value]="OFN"+str(FortNightCount)
        OutputDataFrame[OutputTransposeColumn].replace(OutputRatingLabel,inplace=True)
        
        logger.info("Pivoting Process In The Output DataFrame")
        OutputDataFrame=OutputDataFrame.pivot_table(index=[OutputTransposeIndex],values=OutputTransposeValues,columns=OutputTransposeColumn)
        Rows_Count,Columns_Count=OutputDataFrame.shape
        logger.info("OutputDataFrame Rows {} And Columns {}".format(Rows_Count,Columns_Count))
        logger.info("Pivoting Completed in The Output DataFrame")
        

        OutputDataFrame.fillna(0,inplace=True)
        OutputDataFrame = OutputDataFrame.astype(np.int8)
        OutputDataFrame.columns=OutputDataFrame.columns.tolist()
        OutputDataFrame.reset_index(inplace=True)

        logger.info("Output DataFrame Process Completed")
        
        logger.info("Merging The Input And Output DataFrame")
        FianlInputDataFrame = pd.merge(InputDataFrame,OutputDataFrame,on=InputUniqueValuesColumns[0],how="inner")
        Rows_Count,Columns_Count=FianlInputDataFrame.shape
        logger.info("FianlInputDataFrame Rows {} And Columns {}".format(Rows_Count,Columns_Count))

        logger.info("Writing The  MergedDataFrame in Directory")
        with open(OutputDirectory+"/"+ModuleName+".csv",'w') as File:
            FianlInputDataFrame.to_csv(File,index=False)
        
        OutputDataFrame=FianlInputDataFrame[["OFN1","OFN2","OFN3","OFN4","OFN5","OFN6"]]
        FianlInputDataFrame.drop(["OFN1","OFN2","OFN3","OFN4","OFN5","OFN6"],axis=1,inplace=True)
        InputDataFrame=FianlInputDataFrame
        logger.info("Input DataFrame")
        # FianlInputDataFrame = pd.merge(InputDataFrame,OutputDataFrame,on=InputUniqueValuesColumns[0],how="inner")
        Rows_Count,Columns_Count=InputDataFrame.shape
        logger.info("Input Rows {} And Columns {}".format(Rows_Count,Columns_Count))
        
        logger.info("Output DataFrame")
        # FianlInputDataFrame = pd.merge(InputDataFrame,OutputDataFrame,on=InputUniqueValuesColumns[0],how="inner")
        
        Rows_Count,Columns_Count=OutputDataFrame.shape
        logger.info("Output Rows {} And Columns {}".format(Rows_Count,Columns_Count))                 
        

        # logger.info("Writing The DataFrame For Evaluation in Directory")
        # with open(OutputDirectory+"/"+ModuleName+"Eval"+".csv",'w') as File:
        #     InputDataFrame.to_csv(File,index=False)
        


        logger.info("Writing The DataFrame in Directory")
        with open(OutputDirectory+"/"+"IPFile.csv",'w') as File:
            InputDataFrame.to_csv(File,index=False)
        
        logger.info("Writing The DataFrame in Directory")
        with open(OutputDirectory+"/"+"OPFile.csv",'w') as File:
            OutputDataFrame.to_csv(File,index=False)        
        logger.info("Completed the Transpose Module")
        
    except Exception as e:
        response['Status'] = status.HTTP_500_INTERNAL_SERVER_ERROR
        response['TransposeStatus'] = ['Transpose Failed']
        logger.error(str(e))
        response['ErrorMessage'] = [str(e), 'Error occured while transposing. Please check the debug logs']
        
    finally:
        audit = TransposeMPAudit(**response)
        audit.save()
    return response

