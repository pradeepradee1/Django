from os import replace
from modeltrain.models import SamplingAudit
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

def Sampling(request):
    response = request
    try:
        logger.info("Sampling Module Starting Successfuly")
        ProcessId = request.get('ProcessId')
        InputDataFrame = request.get('InputFile')
        OutputDirectory = request.get('OutputDirectory')
        OutputFeatureNames = request.get('OutputFeatureNames')
        OutputType = request.get('OutputType')
        ModuleName = request.get('ModuleName')
        ModuleEnable = request.get('ModuleEnable')
        SamplingType = request.get('SamplingType')
        Template = request.get('Template')  
        Product = request.get('Product')  
        CallbackURL = request.get('CallbackURL')  
        logger.info("Sampling Module Got The Request Successfully")
        
        # Output_file_Directory
        if "/" in OutputDirectory:
            OutputDirectory = "/".join((OutputDirectory,ProcessId,Template,ModuleName,SamplingType))
            Path(OutputDirectory).mkdir(parents=True, exist_ok=True)
        elif "\\"  in OutputDirectory:
            OutputDirectory = "\\".join((OutputDirectory,ProcessId,Template,ModuleName,SamplingType))
            Path(OutputDirectory).mkdir(parents=True, exist_ok=True)            
        
        #Module_Process
        if ModuleEnable == 'True' :
            logger.info("Reading The InputDataFrame Files")
            InputDataFrame = pd.read_csv(InputDataFrame)
            Rows_Count,Columns_Count=InputDataFrame.shape
            logger.info("InputDataFrame Rows {} And Columns {}".format(Rows_Count,Columns_Count))
            logger.info("Calling The Reduce Memory Function")
            InputDataFrame=Reduce_Memory_Usage(InputDataFrame)
            logger.info("Sampling Technique Started For MultiOutput")

            if SamplingType.lower() == "downsampling":
                logger.info("Sampling Technique Started For MultiOutput {}".format(SamplingType))
                SortingValue = False
            elif SamplingType.lower() == "upsampling":
                logger.info("Sampling Technique Started For MultiOutput {}".format(SamplingType))
                SortingValue = True 

            FinalDataFrame=pd.DataFrame()
            FinalCountDataFrame=0
            for OPFeatureName in OutputFeatureNames:
                logger.info("Output Feature Name {}".format(OPFeatureName))
                Sampling_Values_Count=dict(InputDataFrame[OPFeatureName].value_counts())
                Sampling_Values_Count_New={}
                for Sampling_key in Sampling_Values_Count:
                    Sampling_Values_Count_New[int(Sampling_key)]=Sampling_Values_Count[Sampling_key]               
                logger.info("Count Of Output Feature {}".format(Sampling_Values_Count_New))

                Sampling_Sort_Values_Count=dict(sorted(Sampling_Values_Count_New.items(),key=lambda x:x[1],reverse=SortingValue ))
                logger.info("Sorting The Count Of Output Feature {}".format(Sampling_Sort_Values_Count))
                
                for MinIteration in Sampling_Sort_Values_Count:
                    MinmumClassName = MinIteration
                    MinCount = Sampling_Sort_Values_Count[MinIteration]
                    break
                logger.info("MinmumClassName {} And MinCount {}".format(MinmumClassName,MinCount))

                InputDataFrame_In_Dict={}
                count=0
                for SamplingSortValuesCount_Key in Sampling_Sort_Values_Count:
                    InputDataFrame_In_Dict["DF_Class_"+str(count)]=InputDataFrame[InputDataFrame[OPFeatureName]==SamplingSortValuesCount_Key]
                    count+=1

                InputDataFrameIteration=pd.DataFrame()
                count=0
                for dataframeiteration in InputDataFrame_In_Dict:
                    if count < 1 :
                        InputDataFrameIteration=InputDataFrame_In_Dict[dataframeiteration].sample(MinCount,replace=True)
                    else:
                        TempDataFrame=InputDataFrame_In_Dict[dataframeiteration].sample(MinCount,replace=True)
                        InputDataFrameIteration=pd.concat([InputDataFrameIteration,TempDataFrame])
                    count+=1

                logger.info("Sampling Completed For Single Output Features")
                Rows_Count,Columns_Count=InputDataFrameIteration.shape
                logger.info("InputDataFrameIteration Rows {} And Columns {}".format(Rows_Count,Columns_Count))
                if FinalCountDataFrame == 0:
                    FinalDataFrame = InputDataFrameIteration.copy()
                else:
                    FinalDataFrame = pd.concat([FinalDataFrame,InputDataFrameIteration])
                FinalCountDataFrame+=1
                logger.info("Sampling Completed For Single Output Features")
                Rows_Count,Columns_Count=FinalDataFrame.shape
                logger.info("FinalDataFrame Rows {} And Columns {}".format(Rows_Count,Columns_Count))

            logger.info("Writing The File Process")
            with open(OutputDirectory+"/"+ModuleName+".csv",'w') as File:
                FinalDataFrame.to_csv(File,index=False)
            logger.info("Completed the Sampling Module")

    except Exception as e:
        response['Status'] = status.HTTP_500_INTERNAL_SERVER_ERROR
        response['SamplingStatus'] = ['Sampling Failed']
        logger.error(str(e))
        response['ErrorMessage'] = [str(e), 'Error occured while transposing. Please check the debug logs']
        
    finally:
        audit = SamplingAudit(**response)
        audit.save()
    return response

