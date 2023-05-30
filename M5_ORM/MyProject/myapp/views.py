from django.shortcuts import render
from django.http import HttpResponse
from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework import status
from myapp.models import Employee
from django.db.models import Avg,Sum,Max,Min,Count

@api_view(['POST'])
def display(request):
    
    response = Employee.objects.all()
    print(response.query)
    
    response = Employee.objects.filter(id__gt=2)
    print(response)

    response = Employee.objects.filter(id__gte=2)
    print(response)

    response = Employee.objects.filter(id__lt=2)
    print(response)

    response = Employee.objects.filter(id__lte=2)
    print(response)

    response = Employee.objects.filter(id__in=[2,5])
    print(response)

    response = Employee.objects.filter(ename__startswith="prad")
    print(response)

    response = Employee.objects.filter(ename__istartswith="prad")
    print(response)

    response = Employee.objects.filter(ename__endswith="eep")
    print(response)

    response = Employee.objects.filter(ename__iendswith="eep")
    print(response)    

    response = Employee.objects.get(id__exact=2)
    print(response)

    response = Employee.objects.get(id__iexact=2)
    print(response)    

    response = Employee.objects.get(id__contains=2)
    print(response)

    response = Employee.objects.get(id__icontains=2)
    print(response)

    response = Employee.objects.filter(ename__startswith='p') | Employee.objects.filter(esal__lt=15000)
    print(response)

    response = Employee.objects.filter(ename__startswith='p') & Employee.objects.filter(esal__lt=3000)
    print(response)

    response = Employee.objects.exclude(ename__startswith='p')
    print(response) 

    q1=Employee.objects.filter(id__lt=2)

    q2=Employee.objects.filter(ename__endswith='p')
    
    response=q1.union(q2)
    print(response)
    
    # print(response.id,response.eno,response.ename,response.esal)

    # How to select only some columns in the queryset ?
    response=Employee.objects.all().values_list('ename','esal','eaddr')
    print(response)

    response=Employee.objects.all().values('ename','esal','eaddr')
    print(response)

    response=Employee.objects.all().only('ename','esal','eaddr')
    print(response)    

    avg1=Employee.objects.all().aggregate(Avg('esal'))
    max=Employee.objects.all().aggregate(Max('esal'))
    min=Employee.objects.all().aggregate(Min('esal'))
    sum=Employee.objects.all().aggregate(Sum('esal'))
    count=Employee.objects.all().aggregate(Count('esal'))
    my_dict={'avg':avg1,'max':max,'min':min,'sum':sum,'count':count}
    print(my_dict)

    #Case-9: How to Create, Update, Delete Records

    print(Employee.objects.all().count())
    e=Employee(eno=1,ename='Dheeraj',esal=1234.0,eaddr='Delhi')
    e.save()
    print(Employee.objects.all().count())

    #How to Add Multiple Records at a Time:
    Employee.objects.bulk_create([Employee(eno=1,ename='DDD',esal=1000,eaddr='Hyd'),
    Employee(eno=2,ename='HHH',esal=1000,eaddr='Hyd'),
    Employee(eno=3,ename='MMM',esal=1000,eaddr='Hyd')])

    print(Employee.objects.all().count())

    #How to Delete a Single Record:
    e=Employee.objects.get(eno=1)
    print(e.eno)
    print(e.ename)

    return Response("response")

