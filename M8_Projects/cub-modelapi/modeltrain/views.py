from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework import status
from model.tasks import traintask,transposetask,cleansertask,encodertask,scalartask,testtask
from modeltrain.transpose import transpose
from modeltrain.transpose_MP import Transpose_MP
from modeltrain.sampling import Sampling
from modeltrain.cleanser import cleanser,Cleaning_MP
from modeltrain.encoder import encoder,Encoding_MP
from modeltrain.scalar import scalar,Scaling_MP
from modeltrain.train import train,Train_MP
from modeltrain.test import test,Test_MP
from modeltrain.featureselection import featureselection
from modeltrain.outliers import outliers
from modeltrain.evaluation import evaluation,EvaluationMP


@api_view(['POST'])
def transposeapi(request):

    response = request.data

    try:                
        response['Status'] = status.HTTP_200_OK
        response['TransposeStatus'] = 'Transpose in progress'
        response['ErrorMessage'] = ['']
        # transposetask.delay(request.data)
        transpose(request.data)

    except Exception as e:
        response['Status'] = status.HTTP_500_INTERNAL_SERVER_ERROR
        response['TransposeStatus'] = 'Failed'
        response['ErrorMessage'] = [str(e), 'Incorrect or Invalid Json Contract']
        
    return Response(response)


@api_view(['POST'])
def transposecpapi(request):
    
    response = request.data

    try:                
        response['Status'] = status.HTTP_200_OK
        response['TransposeStatus'] = 'Transpose in progress'
        response['ErrorMessage'] = ['']
        # transposetask.delay(request.data)
        Transpose_MP(request.data)

    except Exception as e:
        
        response['Status'] = status.HTTP_500_INTERNAL_SERVER_ERROR
        response['TransposeStatus'] = 'Failed'
        response['ErrorMessage'] = [str(e), 'Incorrect or Invalid Json Contract']
        
    return Response(response)


@api_view(['POST'])
def samplingapi(request):
    
    response = request.data

    try:                
        response['Status'] = status.HTTP_200_OK
        response['SamplingStatus'] = 'Sampling in progress'
        response['ErrorMessage'] = ['']
        # transposetask.delay(request.data)
        Sampling(request.data)

    except Exception as e:
        
        response['Status'] = status.HTTP_500_INTERNAL_SERVER_ERROR
        response['SamplingStatus'] = 'Failed'
        response['ErrorMessage'] = [str(e), 'Incorrect or Invalid Json Contract']
        
    return Response(response)



@api_view(['POST'])
def cleanserapi(request):
    
    response = request.data
    
    try:                
        response['Status'] = status.HTTP_200_OK
        response['CleansingStatus'] = 'Cleansing in progress'
        response['ErrorMessage'] = ['']
        # cleansertask.delay(request.data)
        if request.data['ModelType'].lower() == "multioutput" :
            Cleaning_MP(request.data)
        elif request.data['ModelType'].lower() == "singleoutput" :
            cleanser(request.data)

    except Exception as e:
        response['Status'] = status.HTTP_500_INTERNAL_SERVER_ERROR
        response['CleansingStatus'] = 'Failed'
        response['ErrorMessage'] = [str(e), 'Incorrect or Invalid Json Contract']
        
    return Response(response)


@api_view(['POST'])
def outliersapi(request):
    
    response = request.data
    
    try:                
        response['Status'] = status.HTTP_200_OK
        response['OutlierStatus'] = 'outlier in progress'
        response['ErrorMessage'] = ['']
        # cleansertask.delay(request.data)
        outliers(request.data)

    except Exception as e:
        response['Status'] = status.HTTP_500_INTERNAL_SERVER_ERROR
        response['OutlierStatus'] = 'Failed'
        response['ErrorMessage'] = [str(e), 'Incorrect or Invalid Json Contract']
        
    return Response(response)



@api_view(['POST'])
def encoderapi(request):

    response = request.data

    try:    
        response['status'] = status.HTTP_200_OK
        response['encoding_status'] = 'Encoding in progress'
        response['error_message'] = ['']
        #encodertask.delay(request.data)
        if request.data['ModelType'].lower() == "multioutput" :
            Encoding_MP(request.data)
        elif request.data['ModelType'].lower() == "singleoutput" :
            encoder(request.data)
        

    except Exception as e:
        response['status'] = status.HTTP_500_INTERNAL_SERVER_ERROR
        response['encoding_status'] = 'Failed'
        response['error_message'] = [str(e), 'Incorrect or Invalid Json Contract']
        
    return Response(response)


@api_view(['POST'])
def featureselectionapi(request):

    response = request.data

    try:    
        response['status'] = status.HTTP_200_OK
        response['feature_status'] = 'Feature Selection in progress'
        response['error_message'] = ['']
        #encodertask.delay(request.data)
        featureselection(request.data)

    except Exception as e:
        response['status'] = status.HTTP_500_INTERNAL_SERVER_ERROR
        response['feature_status'] = 'Failed'
        response['error_message'] = [str(e), 'Incorrect or Invalid Json Contract']
        
    return Response(response)

@api_view(['POST'])
def scalarapi(request):

    response = request.data

    try:    
        response['status'] = status.HTTP_200_OK
        response['scalar_status'] = 'Scaling in progress'
        response['error_message'] = ['']
        #scalartask.delay(request.data)
        if request.data['ModelType'].lower() == "multioutput" :
            Scaling_MP(request.data)
        elif request.data['ModelType'].lower() == "singleoutput" :
            scalar(request.data)

    except Exception as e:
        response['status'] = status.HTTP_500_INTERNAL_SERVER_ERROR
        response['scalar_status'] = 'Failed'
        response['error_message'] = [str(e), 'Incorrect or Invalid Json Contract']

    return Response(response)

@api_view(['POST'])
def trainapi(request):

    response = request.data

    try:    
        response['status'] = status.HTTP_200_OK
        response['train_status'] = 'Training in progress'
        response['error_message'] = ['']
        #traintask.delay(request.data)
        if request.data['ModelType'].lower() == "multioutput" :
            Train_MP(request.data)
        elif request.data['ModelType'].lower() == "singleoutput" :        
            train(request.data)

    except Exception as e:
        response['status'] = status.HTTP_500_INTERNAL_SERVER_ERROR
        response['train_status'] = 'Failed'
        response['error_message'] = [str(e), 'Incorrect or Invalid Json Contract']

    return Response(response)

@api_view(['POST'])
def testapi(request):

    response = request.data
    
    try:  
        response['status'] = status.HTTP_200_OK
        response['test_status'] = 'Testing in progress'
        response['error_message'] = ['']
        #testtask.delay(request.data)

        if request.data['ModelType'].lower() == "multioutput" :
            Test_MP(request.data)
        elif request.data['ModelType'].lower() == "singleoutput" :        
            test(request.data)
        

    except Exception as e:
        response['status'] = status.HTTP_500_INTERNAL_SERVER_ERROR
        response['test_status'] = 'Failed'
        response['error_message'] = [str(e), 'Incorrect or Invalid Json Contract']

    return Response(response)


@api_view(['POST'])
def evaluationapi(request):

    response = request.data
    
    try:  
        response['status'] = status.HTTP_200_OK
        response['evaluation_status'] = 'Evaluation in progress'
        response['error_message'] = ['']
        #testtask.delay(request.data)
        if request.data['ModelType'].lower() == "multioutput" :
            EvaluationMP(request.data)
        elif request.data['ModelType'].lower() == "singleoutput" :        
            evaluation(request.data) 

    except Exception as e:
        response['status'] = status.HTTP_500_INTERNAL_SERVER_ERROR
        response['Evaluation_status'] = 'Failed'
        response['error_message'] = [str(e), 'Incorrect or Invalid Json Contract']

    return Response(response)
