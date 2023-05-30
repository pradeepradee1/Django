from django.urls import path
from django.contrib import admin
from myapp import views


urlpatterns = [
    # url(r'^admin/', admin.site.urls),
    path(r'greet/', views.display),
]

