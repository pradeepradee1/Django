from modeltrain.views import transposeapi,transposecpapi,samplingapi,cleanserapi,encoderapi,scalarapi,trainapi,testapi,featureselectionapi,outliersapi,evaluationapi

from django.urls import path

urlpatterns = [
    path(r'model/transpose',transposeapi),
    path(r'model/mptranspose',transposecpapi),
    path(r'model/sampling',samplingapi),
    path(r'model/cleanser',cleanserapi),
    path(r'model/outliers',outliersapi),
    path(r'model/encoder',encoderapi),
    path(r'model/scalar',scalarapi),
    path(r'model/train',trainapi),
    path(r'model/test',testapi),
    path(r'model/featureselection',featureselectionapi),
    path(r'model/evaluation',evaluationapi)
]

