from django.urls import path
from django.contrib import admin
from myapp import views


urlpatterns = [
    path(r'display_all/', views.display_all),
    path(r'display_get/', views.display_get),
    path(r'display_filter/', views.display_filter),
    path(r'display_exclude/', views.display_exclude),
    path(r'display_union/', views.display_union),
    path(r'display_select/', views.display_select),
    path(r'display_agg/', views.display_agg),
    path(r'display_create/', views.display_create),
    path(r'display_delete/', views.display_delete),
    path(r'display_update/', views.display_update),
]


