from django.db import models

# Create your models here.

from django.db import models

class Employee(models.Model):
    eno=models.IntegerField()
    ename=models.CharField(max_length=30)
    esal=models.FloatField()
    eaddr=models.CharField(max_length=30)

#1)Abstraction Model
class ContactInfo(models.Model):
    name=models.CharField(max_length=64)
    email=models.EmailField()
    address=models.CharField(max_length=256)
    class Meta:
        abstract=True
    
class Student(ContactInfo):
    rollno=models.IntegerField()
    marks=models.IntegerField()

class Teacher(ContactInfo):
    subject=models.CharField(max_length=64)
    salary=models.FloatField()

#2)SingleLevel Inheritance
class BasicModel(models.Model):
    f1=models.CharField(max_length=64)
    f2=models.CharField(max_length=64)
    f3=models.CharField(max_length=64)

class StandardModel(BasicModel):
    f4=models.CharField(max_length=64)
    f5=models.CharField(max_length=64)

#3) Multi Level Inhertiance

class Person(models.Model):
    name=models.CharField(max_length=64)
    age=models.IntegerField()

class Employee(Person):
    eno=models.IntegerField()
    esal=models.FloatField()

class Manager(Employee):
    exp=models.IntegerField()
    team_size=models.IntegerField()

#4) Multiple Inhertiance


# class Parent1(models.Model):
#     f1=models.CharField(max_length=64)
#     f2=models.CharField(max_length=64)

# class Parent2(models.Model):
#     f3=models.CharField(max_length=64)
#     f4=models.CharField(max_length=64)

# class Child(Parent1,Parent2):
#     f5=models.CharField(max_length=64)
#     f6=models.CharField(max_length=64)
