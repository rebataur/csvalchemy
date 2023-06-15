"""rapidiam URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/4.1/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path, include
from . import views

app_name = 'rapidiamapp'

urlpatterns = [
    path('', views.index, name="index"),
    path('dataingestion/<str:action>/<int:id>', views.dataingestion, name='dataingestion'),
    path('datapreparation/<str:action>/<int:id>', views.datapreparation, name='datapreparation'),
    path('edit_fieldtype/<int:id>', views.edit_fieldtype, name='edit_fieldtype'),

    path('datascience/<str:action>/<int:id>', views.datascience, name='datascience'),
    path('dataviz/<str:action>/<int:id>', views.dataviz, name='dataviz'),
    path('dataalerts/<str:action>/<int:id>', views.dataalerts, name='dataalerts'),


    path('fieldfunction/<str:action>/<int:id>', views.fieldfunction, name='fieldfunction'),

    
]
