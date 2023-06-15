from django.contrib import admin

# Register your models here.
from .models import Entity, Field, FunctionMeta, ArgumentMeta, DerivedFieldArgument, FieldFilter,DataScience,DerivedDataScienceArgument,EntityChildren

model = [Entity, Field, FunctionMeta, ArgumentMeta,
         DerivedFieldArgument, FieldFilter,DataScience,DerivedDataScienceArgument,EntityChildren]
for m in model:
    admin.site.register(m)
