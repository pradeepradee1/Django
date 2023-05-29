from celery.decorators import task
from celery.utils.log import get_task_logger
from modeltrain.transpose import transpose
# from modeltrain.new_transpose import new_transpose
from modeltrain.cleanser import cleanser
from modeltrain.outliers import outliers
from modeltrain.encoder import encoder
from modeltrain.scalar import scalar
from modeltrain.train import train
from modeltrain.test import test

logger = get_task_logger(__name__)

# app = Celery()


@task(name='transposetask',serializer='json')
def transposetask(request):

    try:
        response = transpose(request)

    except Exception as e:
        raise e
    
    return response







@task(name='cleansertask',serializer='json')
def cleansertask(request):

    try:
        print("ch")
        response = cleanser(request)
    
    except Exception as e:
        raise e
    
    return response

@task(name='outlierstask',serializer='json')
def outlierstask(request):

    try:
        print("outliers")
        response = outliers(request)
    
    except Exception as e:
        raise e
    
    return response

@task(name='encodertask',serializer='json')
def encodertask(request):

    try:
        print("")
        response = encoder(request)

    except Exception as e:
        raise e

    return response

@task(name='scalartask',serializer='json')
def scalartask(request):

    try:
        response = scalar(request)

    except Exception as e:
        raise e
        
    return response

@task(name='traintask',serializer='json')
def traintask(request):

    try:
        response = train(request)

    except Exception as e:
        raise e

    return response

@task(name='testtask',serializer='json')
def testtask(request):

    try:
        response = test(request)

    except Exception as e:
        raise e

    return response

@task(name="add")
def add(x, y):

    print(x+y)
    return x + y