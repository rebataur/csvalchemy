from django.db import models
from datetime import date,datetime
from django.utils.timezone import now

# Create your models here.
DATA_TYPES = (
    ('TEXT', 'Text'),
    ('INTEGER', 'Integer'),
    ('NUMERIC', 'Numeric'),
    ('DATE', 'Date'),
    ('BINARY', 'Binary'),
    ('PNG_IMAGE', 'PNG Image'),
    ('COLUMN', 'Column Name'),

)

FIELD_TYPES = (
    ('COLUMN', 'Column'),
    ('DERIVED', 'Derived'),
    ('CALCULATED', 'Calculated'),

)

FUNCTION_TYPES = (
    ('CALCULATION', 'Calculation'),
    ('VISUALIZE', 'Visualize'),
    ('DATASCIENCE', 'DataScience'),
    ('GENERATED','Generated'),
)

OP_TYPES = (
    ('eq', 'Exact'),
    ('not', 'Not'),
    ('gt','Greater Than'),
    ('gte','Greater Than Equal To'),
    ('lt','Less Than'),
    ('lte','Less Than Equal'),

)
EXECUTION_MODE = (
    ('TRAINING','Training'),
    ('EXECUTION','Execution'),
)

SQL_OP_TYPES = {'exact': '=', 'not': '!=','gt':'>','gte':'>=','lt':'<','lte':'<='}



class Entity(models.Model):
    name = models.CharField(max_length=30, unique=True)
    # child = models.ForeignKey('self',on_delete=models.DO_NOTHING,null=True)

    def __str__(self):
        return self.name
    

class FunctionMeta(models.Model):
    name = models.CharField(max_length=30)
    type = models.CharField(
        max_length=20, choices=FUNCTION_TYPES, default='CALCULATION')
    return_type = models.CharField(
        max_length=20, choices=DATA_TYPES, default='TEXT')
    
    function_code = models.TextField()
    test_code = models.TextField()
    return_sql = models.TextField(max_length=1024)   

    def __str__(self):
        return self.name
  
        

class ArgumentMeta(models.Model):
    function = models.ForeignKey(
        FunctionMeta, on_delete=models.CASCADE, null=True, related_name='functions')
    name = models.CharField(max_length=30)
    type = models.CharField(max_length=20, choices=DATA_TYPES, default='TEXT')

    def __str__(self):
        return self.name


class Field(models.Model):
    entity = models.ForeignKey(Entity, on_delete=models.CASCADE, null=True)
    actual_name = models.CharField(max_length=64)
    name = models.CharField(max_length=64,unique=True)
    # entity_field_name = models.CharField(max_length=64,unique=True)
    main = models.BooleanField(default=False)
    datatype = models.CharField(max_length=64, choices=DATA_TYPES, default='TEXT')
    type = models.CharField(max_length=64, choices=FIELD_TYPES, default='COLUMN')
    # child_entity_id = models.IntegerField(null=True)
    # child_field_id = models.IntegerField(null=True)
    derived_level = models.IntegerField(default=0)
    function = models.ForeignKey(
        FunctionMeta, on_delete=models.CASCADE, null=True)
    # order = models.IntegerField()

    def __str__(self):
        return self.name


class EntityChildren(models.Model):
    parent_entity = models.ForeignKey(Entity,on_delete=models.CASCADE,null=True,related_name='parent_entity')
    child_entity = models.ForeignKey(Entity,on_delete=models.CASCADE,null=True,related_name='child_entity')
    parent_field = models.ForeignKey(Field,on_delete=models.CASCADE,null=True,related_name='parent_field')
    child_field = models.ForeignKey(Field,on_delete=models.CASCADE,null=True,related_name='child_field')


class FieldFilter(models.Model):
    entity = models.ForeignKey(Entity, on_delete=models.CASCADE, null=False)
    filter_col = models.CharField(max_length=30, null=False)
    filter_op = models.CharField(
        max_length=30, null=False, choices=OP_TYPES, default='EXACT')
    filter_val = models.CharField(max_length=120, null=False)

    def __str__(self):
        return self.entity.name + " " + self.filter_op


class DerivedFieldArgument(models.Model):
    field = models.ForeignKey(Field, on_delete=models.CASCADE, null=True)
    argument_name = models.CharField(max_length=30, null=True)
    argument_value = models.TextField(null=True)
    argument_type = models.CharField(max_length=30, null=True)

    def __str__(self):
        return str(self.id) + " " + self.field.name + " -> " + self.argument_name + " " +  self.argument_type


class DataScience(models.Model):
    entity = models.ForeignKey(Entity, on_delete=models.CASCADE, null=True)
    name = models.CharField(max_length=64,unique=True)             
    function = models.ForeignKey(
        FunctionMeta, on_delete=models.CASCADE, null=True)
    execution_mode = models.CharField(max_length=64, choices=EXECUTION_MODE, default='TRAINING')

    def __str__(self):
        return self.name

class DerivedDataScienceArgument(models.Model):
    field = models.ForeignKey(DataScience, on_delete=models.CASCADE, null=True)
    argument_name = models.CharField(max_length=30, null=True)
    argument_value = models.TextField(null=True)
    argument_type = models.CharField(max_length=30, null=True)

    def __str__(self):
        return str(self.id) + " " + self.field.name + " -> " + self.argument_name + " " +  self.argument_type
    
class DataAlertFieldFilter(models.Model):
    entity = models.ForeignKey(Entity, on_delete=models.CASCADE, null=False)
    filter_col = models.CharField(max_length=30, null=False)
    filter_op = models.CharField(
        max_length=30, null=False, choices=OP_TYPES, default='EXACT')
    filter_val = models.CharField(max_length=120, null=False)
    
    def __str__(self):
        return self.entity.name + " " + self.filter_op
    
class ScheduleJob(models.Model):
    entity = models.ForeignKey(Entity, on_delete=models.CASCADE, null=False)
    job_sql = models.TextField()
    callback_url = models.CharField(max_length=1024, null=False)
    sched_min = models.IntegerField(default=0)
    last_run = models.DateTimeField()
