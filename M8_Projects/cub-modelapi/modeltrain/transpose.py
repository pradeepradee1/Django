from modeltrain.models import TransposeAudit
from rest_framework import status
import pandas as pd
import numpy as np
from modeltrain.utils import create_table,df_tosql,get_checksum,get_file_size
from pathlib import Path
from functools import reduce

import logging
logger = logging.getLogger(__name__)


def transpose(request):
    response = request
    try:
        logger.info("Transpose Module Started Successfuly")
        ProcessId = request.get('ProcessId')
        RunningAccountTranscationFile = request.get('RunningAccountTranscationFile')
        JLAccountTranscationFile = request.get('JLAccountTranscationFile')
        RunningAccountTranscationColumns = request.get('RunningAccountTranscationColumns')
        JLAccountTranscationColumns = request.get('JLAccountTranscationColumns')
        OtherAcccountMonths = request.get('OtherAcccountMonths')
        JLAccountMonths = request.get('JLAccountMonths')
        ResetIndex = request.get('ResetIndex')
        Iteration = request.get('Iteration')
        Iteration1 = request.get('Iteration1')
        Template = request.get('Template')
        JLTableName = request.get('JLTableName')
        OtherAccountTableName = request.get('OtherAccountTableName')
        MappingTable = request.get('MappingTable')
        MonthTable = request.get('MonthTable')
        OutputFile = request.get('OutputFile')
        Product = request.get('Product')
        CallbackURL = request.get('CallbackURL')
        
        JLTableName = str(ProcessId)+"_"+str(Template)+"_"+JLTableName
        OtherAccountTableName = str(ProcessId)+"_"+str(Template)+"_"+OtherAccountTableName
        MappingTable = str(ProcessId)+"_"+str(Template)+"_"+MappingTable
        MonthTable = str(ProcessId)+"_"+str(Template)+"_"+MonthTable
        logger.info("Request Got It Successfully For Transpose Module")
        
        # Output_file_Directory
        OutputFile = "/".join((OutputFile,ProcessId,Template,Product,"Transpose"))
        
        if "/" in OutputFile:
            Path(OutputFile).mkdir(parents=True, exist_ok=True)
        elif "\\"  in OutputFile:
            Path(OutputFile).mkdir(parents=True, exist_ok=True)    
        
        #Module_Process
        RunningAccounutTranscation = pd.read_csv(RunningAccountTranscationFile)
        JLAccountTranscation = pd.read_csv(JLAccountTranscationFile)
        logger.info("Reading The OtherAccount and JLAccount Input files")
        
        OtherAccTxnMonthsValues=set(RunningAccounutTranscation[RunningAccountTranscationColumns[2]].values)
        OtherAccTxnMonthsValues=sorted(OtherAccTxnMonthsValues,reverse=True)

        JLAccTxnMonthsValues=set(JLAccountTranscation[JLAccountTranscationColumns[1]].values)
        JLAccTxnMonthsValues=sorted(JLAccTxnMonthsValues,reverse=True)
        
        OtherAccTxnMonthsColumns={}
        for i in range(0,len(OtherAccTxnMonthsValues)):
            if i < OtherAcccountMonths:
                OtherAccTxnMonthsColumns[OtherAccTxnMonthsValues[i]]='M'+str(i+1)
        
        JLAccTxnMonthsColumns={}
        for i in range(0,len(JLAccTxnMonthsValues)):
            if i < JLAccountMonths:
                JLAccTxnMonthsColumns[JLAccTxnMonthsValues[i]]='M'+str(i+1)        
        
        
        RunningAccounutTranscation[RunningAccountTranscationColumns[2]]=RunningAccounutTranscation[RunningAccountTranscationColumns[2]].replace(OtherAccTxnMonthsColumns)
        RunningAccounutTranscation=RunningAccounutTranscation[RunningAccounutTranscation[RunningAccountTranscationColumns[2]].str.startswith("M")]    

        JLAccountTranscation[JLAccountTranscationColumns[1]]=JLAccountTranscation[JLAccountTranscationColumns[1]].replace(JLAccTxnMonthsColumns)
        JLAccountTranscation=JLAccountTranscation[JLAccountTranscation[JLAccountTranscationColumns[1]].str.startswith("M")]


        RunningAccounutTranscation.set_index(ResetIndex, inplace = True,append = True, drop = True)
        final=[Iteration1]        
        
        logger.info("Pivoting The DataFrames")
        Dataframe = []
        for i in final:
        
            Trans= pd.pivot_table(RunningAccounutTranscation, values=i[0],index=i[1],columns=i[2],fill_value=0)
            
            if len(i[2])==1:
                Trans.columns=[str(s2) for (s1,s2) in Trans.columns.tolist()]
                Trans.reset_index(inplace=True)
                Dataframe.append(Trans)

            if len(i[2])==2:
                Trans.columns=[str(s2)+"-"+s3+"-"+s1 for (s1,s2,s3) in Trans.columns.tolist()]
                Trans.reset_index(inplace=True)
                Dataframe.append(Trans)        
        
        
        Tran= pd.pivot_table(JLAccountTranscation, values=Iteration[0],index=Iteration[1],columns=Iteration[2],fill_value=0)
        if len(Iteration[2])==1:
            Tran.columns=[str(s2)+"-"+str(s1) for (s1,s2) in Tran.columns.tolist()]
            Tran.reset_index(inplace=True)
        
        if len(Iteration[2])==2:
            Tran.columns=[str(s2)+"-"+s3+"-"+s1 for (s1,s2,s3) in Tran.columns.tolist()]
            Tran.reset_index(inplace=True)
        

        DfMerged = reduce(lambda  left,right: pd.merge(left,right,on=[RunningAccountTranscationColumns[0]],
                                                    how='outer'), Dataframe)
        
        if len(OtherAccTxnMonthsColumns) < OtherAcccountMonths :
            for i in range(len(OtherAccTxnMonthsColumns)+1,OtherAcccountMonths+1):
                DfMerged['M'+str(i)]=0                
        
        
        if len(JLAccTxnMonthsColumns) < JLAccountMonths :
            for i in range(len(JLAccTxnMonthsColumns)+1,22):
                Tran["M"+str(i)+"-"+JLAccountTranscationColumns[4]]=0
                Tran["M"+str(i)+"-"+JLAccountTranscationColumns[3]]=0
                Tran["M"+str(i)+"-"+JLAccountTranscationColumns[2]]=0
        
        Tran[RunningAccountTranscationColumns[0]] = Tran[RunningAccountTranscationColumns[0]].astype(int)
        
        if len(Tran) < len(DfMerged):
            FinalData=pd.merge(Tran,DfMerged,on=RunningAccountTranscationColumns[0],how="right")
        else:
            FinalData=pd.merge(DfMerged,Tran,on=RunningAccountTranscationColumns[0],how="right")
        
        FinalData.to_csv(OutputFile+"/JlTxn.csv",index=False)
        DfMerged.to_csv(OutputFile+"/RunAccTxn.csv",index=False)
        logger.info("Export The OP files of JLTxn and OtherAcct Successfully")
        # create_table(JLTableName,FinalData)
        # df_tosql(JLTableName,FinalData)
        
        # create_table(OtherAccountTableName,DfMerged)
        # df_tosql(OtherAccountTableName,DfMerged)
        
        create_table(MappingTable,OtherAccTxnMonthsColumns)
        
        value=list(OtherAccTxnMonthsColumns.values())
        dfvalue = pd.DataFrame(value, columns = ["Month_label"])
        
        create_table(MonthTable,dfvalue)
        df_tosql(MonthTable,dfvalue)
        
        response['Status'] = status.HTTP_200_OK
        response['TransposeStatus'] = ['Transpose Completed']
        response['ErrorMessage'] = ''
        logger.info("Transpose End Successfully")
        
    except Exception as e:
        response['Status'] = status.HTTP_500_INTERNAL_SERVER_ERROR
        response['TransposeStatus'] = ['Transpose Failed']
        logger.error(str(e))
        response['ErrorMessage'] = [str(e), 'Error occured while transposing. Please check the debug logs']
        
    finally:
        audit = TransposeAudit(**response)
        audit.save()
    return response
