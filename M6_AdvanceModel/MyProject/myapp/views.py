from django.shortcuts import render
from django.http import HttpResponse
from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework import status
from myapp.models import Employee
from django.db.models import Avg,Sum,Max,Min,Count,Q

@api_view(['POST'])
def display_all(request):
    
    response = Employee.objects.all()
    print(response.query)

    print(response.id,response.eno,response.ename,response.esal)
    return Response("response")


@api_view(['POST'])
def display_get(request):
    
    response = Employee.objects.get(id__exact=2)
    print(response)

    response = Employee.objects.get(id__iexact=2)
    print(response)    

    response = Employee.objects.get(id__contains=2)
    print(response)
    
    response = Employee.objects.get(id__icontains=2)
    print(response)

    print(response.id,response.eno,response.ename,response.esal)

    return Response("response")    


@api_view(['POST'])
def display_filter(request):

    response = Employee.objects.filter(id__in=[2,5])
    print(response)

    # print(response.id,response.eno,response.ename,response.esal)
    
    response = Employee.objects.filter(id__gt=2)
    print(response)

    response = Employee.objects.filter(id__gte=2)
    print(response)

    response = Employee.objects.filter(id__lt=2)
    print(response)

    response = Employee.objects.filter(id__lte=2)
    print(response)


    response = Employee.objects.filter(ename__startswith="prad")
    print(response)

    response = Employee.objects.filter(ename__istartswith="prad")
    print(response)

    response = Employee.objects.filter(ename__endswith="eep")
    print(response)

    response = Employee.objects.filter(ename__iendswith="eep")
    print(response)    

    response = Employee.objects.filter(ename__startswith='p') | Employee.objects.filter(esal__lt=15000)
    print(response)

    response = Employee.objects.filter(ename__startswith='p') & Employee.objects.filter(esal__gt=3000)
    print(response)

    return Response("response")


@api_view(['POST'])
def display_exclude(request):
    
    #Not Queries
    response= Employee.objects.filter(~Q(ename__startswith='p'))
    print(response)

    response= Employee.objects.exclude(ename__startswith='p')
    print(response)

    return Response("response")


@api_view(['POST'])
def display_union(request):
    
    q1=Employee.objects.filter(id__lt=2)
    q2=Employee.objects.filter(ename__endswith='p')
    response=q1.union(q2)
    print(response)

    return Response("response")

@api_view(['POST'])
def display_select(request):
    response=Employee.objects.all().only('ename','esal','eaddr')
    print(response)
    response=Employee.objects.all().values('ename','esal','eaddr')
    print(response)
    response=Employee.objects.all().values_list('ename','esal','eaddr')
    print(response)
    return Response("response")


@api_view(['POST'])
def display_agg(request):
    avg1=Employee.objects.all().aggregate(Avg('esal'))
    max=Employee.objects.all().aggregate(Max('esal'))
    min=Employee.objects.all().aggregate(Min('esal'))
    sum=Employee.objects.all().aggregate(Sum('esal'))
    count=Employee.objects.all().aggregate(Count('esal'))
    my_dict={'avg':avg1,'max':max,'min':min,'sum':sum,'count':count}
    print(my_dict)
    return Response("response")


@api_view(['POST'])
def display_create(request):
    print(Employee.objects.all().count())
    #Adding Single Records
    e=Employee(eno=4,ename='Dheeraj',esal=1234.0,eaddr='Delhi')
    e.save()
    print(Employee.objects.all().count())

    #Adding Multiple Records
    Employee.objects.bulk_create([Employee(eno=5,ename='DDD',esal=1000,eaddr='Hyd'),
    Employee(eno=6,ename='HHH',esal=1000,eaddr='Hyd'),
    Employee(eno=7,ename='MMM',esal=1000,eaddr='Hyd')])
    print(Employee.objects.all().count())
    return Response("response")    


@api_view(['POST'])
def display_delete(request):
    print(Employee.objects.all().count())
    
    #deleting Single Records
    e=Employee.objects.get(eno=4)   
    e.delete()

    #deleting Multiple Records
    qs=Employee.objects.filter(eno__gt=3)
    qs.delete()
    print(Employee.objects.all().count())

    #Delete all the records
    #Employee.objects.all().delete()

    return Response("response")


@api_view(['POST'])
def display_update(request):
    print(Employee.objects.all().count())
    
    #update Single Records
    e=Employee.objects.get(eno=2)
    e.ename='Radee'
    e.save()

    return Response("response")    


