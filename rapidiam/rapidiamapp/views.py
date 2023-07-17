from django.shortcuts import render, HttpResponseRedirect, reverse, HttpResponse
from django.contrib import messages
from .forms import UploadFileForm, UploadFileDataForm, DerivedColumnForm,UploadFunctionForm
import pandas as pd
import pygwalker as pyg
from io import StringIO
import logging
from .models import Entity, Field, DATA_TYPES, SQL_OP_TYPES, FUNCTION_TYPES,FunctionMeta, ArgumentMeta, DerivedFieldArgument, FieldFilter,DataScience,DerivedDataScienceArgument,EntityChildren,DataAlertFieldFilter,ScheduleJob
from django.db import connection
from django.conf import settings
import psycopg2
from sqlalchemy import create_engine
from django.contrib import messages
import traceback
from django.core import serializers
import json


logger = logging.getLogger(__name__)
# Create your views here.

PG_NAME = settings.DATABASES['default']['NAME']
PG_USER = settings.DATABASES['default']['USER']
PG_PWD = settings.DATABASES['default']['PASSWORD']
PG_HOST = settings.DATABASES['default']['HOST']
PG_PORT  = settings.DATABASES['default']['PORT']


conn = psycopg2.connect(dbname=PG_NAME, user=PG_USER, password=PG_PWD,host=PG_HOST,port=PG_PORT)
engine = create_engine(f"postgresql+psycopg2://{PG_USER}:{PG_PWD}@{PG_HOST}/{PG_NAME}")

# sql = "with cte_0 as ( select scripname, market, outlook, growwportfolio_file_name, pe, code, low, close, last, prevclose, no_trades, no_of_shrs, sc_name, net_turnov, sc_group, sc_type, tdcloindi, bhavcopy_file_name, sc_code, open, high from growwportfolio left join bhavcopy on growwportfolio.code = bhavcopy.sc_code ), cte_1 as ( select *, convert_str_to_date(bhavcopy_file_name) as trade_date from cte_0), cte_2 as ( select *, avg(close) over(partition by code order by trade_date asc rows between 200 preceding and current row) as sma200, rsi_sma(close) over(partition by code order by trade_date asc rows between 14 preceding and current row) as rsi_sma_14 from cte_1) , cte_3 as ( select *, RANK() OVER (PARTITION BY code ORDER BY trade_date desc) AS rnk from cte_2 ) select distinct * from cte_3 where outlook = 'LONGTERM' and close < sma200 and rsi_sma_14 < 30 and pe < 22 order by trade_date desc"
# sql = "with cte_0 as ( select low,close,last,prevclose,no_trades,no_of_shrs,sc_name,net_turnov,sc_group,sc_type,tdcloindi,bhavcopy_file_name,sc_code,open,high from bhavcopy) select * from cte_0"


def index(request):
    # cur = conn.cursor()
    # cur.execute(sql)
    # rs = cur.fetchall()
    # print(rs[0])

   

    entities = Entity.objects.all()
    return render(request, 'rapidiamapp/index.html', context={'entities': entities})


def dataingestion(request, action, id):
    data = {}
    entity = None
    fields = None
    entities = Entity.objects.all()
    if id:
        entity = Entity.objects.get(id=id)
        fields = Field.objects.filter(entity__id=id)
    if "GET" == request.method:        
        functions_calculated_meta = FunctionMeta.objects.filter(type='GENERATED')
        form = UploadFileForm()
        dataupload_form = UploadFileDataForm()
        entities_excluded = Entity.objects.exclude(id=id).all()
        return render(request, "rapidiamapp/dataingestion.html", context={'entities': entities,'form': form, 'entity': entity, 'entities': entities, 'fields': fields, 'dtypes': DATA_TYPES, 'dataupload_form': dataupload_form,'functions_calculated_meta':functions_calculated_meta,'entities_excluded':entities_excluded})
    # if not GET, then proceed
    if action == 'create':
        try:
            name = request.POST['name'].lower()
            csv_file = request.FILES["csv_file"]

            file_data = csv_file.read().decode("utf-8")
           
            cols = StringIO(file_data).readlines()[0].split(',')
      
            if id == 0:
                Entity.objects.filter(name=name).delete()
            entity = Entity.objects.create(name=name)
            entity.save()
            for col in cols:
                name = col.replace('.', '').replace(
                    '-', '_').replace("'", "").replace('"', '').replace(' ', '_').lower()
                Field.objects.create(
                    actual_name=col, name=f'{entity}_{name}', entity=entity).save()
            # create an additinal file_name columnf
            Field.objects.create(
                actual_name=f'{entity.name}_file_name',
                name=f'{entity.name}_file_name', entity=entity).save()
            return HttpResponseRedirect(f'/dataingestion/display/{entity.id}')
        # return HttpResponseRedirect(reverse("rapidiamapp:create_entity"))

        except Exception as e:
            logging.getLogger("error_logger").error(
                "Unable to upload file. "+repr(e))
            messages.error(request, "Unable to upload file. "+repr(e))
    
    if action == 'edit':
        if 'submit_action_delete' in request.POST:
            # if used in derived field then don't delete
            entity = Entity.objects.get(id=id)
            child_entity = EntityChildren.objects.filter(parent_entity=entity)
         
            if len(child_entity) > 0:
                messages.error(request, f"This Entity is referenced in following Entity [{child_entity}] with field [{child_entity[0].child_entity.name}]")
            else:
                entity.delete()
                return HttpResponseRedirect(f'/')

    if action == 'add_calculated_field':
        derived_field_name = request.POST['derived_field_name']
        function_id = request.POST['function_id']
        

        function = FunctionMeta.objects.get(id=function_id)
        arguments_meta = ArgumentMeta.objects.filter(
            function__id=function_id)
        provided_argument = []
        for arg in arguments_meta:
            val = request.POST[arg.name]
            provided_argument.append(
                {"name": arg.name, "value": val, "type": arg.type})

        # argument_type = models.CharField(max_length=30, null=True)
        # create the derived field
        # field_level = get_level_of_fields(entity.id)
        derived_field = Field.objects.create(
            actual_name=derived_field_name, name=derived_field_name, entity=entity, type='CALCULATED', datatype=function.return_type, derived_level=0, function=function)
        derived_field.save()
        for parg in provided_argument:
            DerivedFieldArgument.objects.create(
                field=derived_field, argument_name=parg['name'], argument_value=parg['value'], argument_type=parg['type']).save()
        html = f'<span class="badge text-bg-primary">{function.name}</span>'
        return HttpResponse(html)

    if action == 'create_table':
        entity = Entity.objects.get(id=id)
        fields = Field.objects.filter(entity__id=id)
        sql = f"create table if not exists {entity.name}("

        for field in fields:
            if field.type == 'CALCULATED':
                function_meta = field.function
                derived_field_arguments = DerivedFieldArgument.objects.filter(
                    field=field.id)
                derived_values = {
                    d.argument_name: d.argument_value for d in derived_field_arguments}
                derived_values['name'] = field.name
                derived_function_sql = function_meta.return_sql.format(
                    **derived_values)

                sql += f"{field.name} {field.datatype} GENERATED ALWAYS AS ({derived_function_sql}) STORED,"
            elif field.type == 'DERIVED':
                pass
            else:
                sql += f"{field.name} {field.datatype},"

        # add created_at and refreshed_at
        sql += "created_at TIMESTAMP default now(),refreshed_at TIMESTAMP default now())"
        # sql = f"{sql[:-1]})"
        execute_raw_query(f"drop table if exists {entity.name}")
        execute_raw_query(sql)
    if action == 'uploaddata':
        try:
            csv_files = request.FILES.getlist('csv_file')
            print(csv_files)

            entity = Entity.objects.get(id=id)
            fields = Field.objects.filter(entity__id=id)

            for csv_file in csv_files:
                print(csv_file)
                file_data = csv_file.read().decode("utf-8")
                file = StringIO(file_data).readlines()
                cols = file[0].split(',')
                print("============================================-")
                columns = []
                for col in cols:
                    name = replace_clean_upload(col).lower()
                    name = f'{entity}_{name}'                    
                    columns.append(name)

                columns.append(f'{entity.name}_file_name')

                sql_cols = f"({(',').join(columns)})"
                for line in file[1:]:
                    # next(line)
                    vals = line.split(',')
                    field_sql = ''

                    for i, c in enumerate(vals):
                        if c.isdecimal() or c.isdigit() or c.isnumeric() or c.replace('.', '', 1).isdigit():
                            field_sql += f"{c},"
                        else:
                            c = c.replace("'", "")
                            field_sql += f"'{c}',"

                    field_sql = f"{field_sql}'{csv_file.name}'"
                    sql = f"insert into {entity.name} {sql_cols} values({field_sql})"
                    execute_raw_query(sql)
                    # break

            
        # return HttpResponseRedirect(reverse("rapidiamapp:create_entity"))

        except Exception as e:
            logging.getLogger("error_logger").error(
                "Unable to upload file. "+repr(e))
            messages.error(request, "Unable to upload file. "+repr(e))
        return HttpResponseRedirect(f'/dataingestion/display/{id}')
    if action == 'add_child':
        
        if request.POST['child_entity_id'] and not request.POST['child_field_id']:
            child_entity_name = request.POST['child_entity_id']
            entity = Entity.objects.get(id=child_entity_name)
            fields = Field.objects.filter(entity__id=entity.id)
            html = '<option></option>'
            for field in fields:
                html += f"<option value='{field.id}'>{field.name}</option>"
            return HttpResponse(html)
        else:
            parent_field_id = request.POST['parent_field_id']
            child_entity_id = request.POST['child_entity_id']
            child_field_id = request.POST['child_field_id']

            child_entity = Entity.objects.get(id=child_entity_id)
            parent_field = Field.objects.get(id=parent_field_id)
            child_field = Field.objects.get(id=child_field_id)
            EntityChildren.objects.create(parent_entity=entity,
                                          child_entity=child_entity,
                                          parent_field=parent_field,
                                          child_field=child_field)
            
            # parent_field = Field.objects.get(id=parent_field_id)
            # parent_field.child_entity_id = child_entity_id
            # parent_field.child_field_id = child_field_id
            # parent_field.save()

            # field = Field.objects.get(id=parent)
        return HttpResponse("done")
    return HttpResponseRedirect(reverse("rapidiamapp:dataingestion", kwargs={'action': 'display', 'id': id}))

def toggle_visibility(request,id):
    if "POST" == request.method:
        print(id, request.POST)     
        field = Field.objects.get(id=id)
        field.visible = False if field.visible else True
        field.save()
        return HttpResponse("ok")
    
def edit_fieldtype(request, id):
    entities = Entity.objects.all()
    if "POST" == request.method:
        print(id, request.POST)
        field_name = None
        dtype = None
        for k, v in request.POST.items():
            field_name = k
            dtype = v

        field = Field.objects.get(id=id)
        field.datatype = dtype
        field.save()
        return HttpResponse("<option>selected</option>")


def datapreparation(request, action, id):
    entities = Entity.objects.all()
    print("***********************************")
    print(action, id)
    print("***********************************")
    data = {}
    entity = Entity.objects.get(id=id)
    if "GET" == request.method:
      
        # tree view of all fields level wise
        fields = Field.objects.filter(
            entity=entity, derived_level__gte=1).order_by('derived_level')
        print(fields)

        derived_tree = {}
        level = 1      
        field_arr = []
        for field in fields:
            print(field.derived_level,level)
            if field.derived_level == level:
                field_arr.append(field)
            else:                
              
                field_arr = [field]
                level += 1
            derived_tree[f'{level}'] = field_arr
        print("DERIVED TREE")
        print(derived_tree)
        # return HttpResponse("hello")

        data_sql = generate_cte_sql(id)
        create_meta_table(entity.name, data_sql)
        data_sql = generate_cte_sql(id,'display')

        full_data_sql = generate_action_sql(data_sql, id, action)
        data, col_names,msg = fetch_raw_query(full_data_sql)        
        entity_columns_meta = get_table_columns(f"{entity.name}_meta")
        level_field = get_level_of_fields(id)
        available_functions = FunctionMeta.objects.filter().exclude(type='GENERATED').exclude(type='DATASCIENCE')
        print(available_functions,'===========')
        # functions_viz_meta = FunctionMeta.objects.filter(type='VISUALIZATION')
        # functions_ds_meta = FunctionMeta.objects.filter(type='DATASCIENCE')

        filters = FieldFilter.objects.filter(entity=entity)

        if action == 'apply_table_filter':
            print(request.GET)
            fieldFilters = FieldFilter.objects.filter(entity=entity)
            new_filter_col = request.GET.get('filter_col_0')
            new_filter_op = request.GET.get('filter_op_0')
            new_filter_val = request.GET.get('filter_val_0')
            print(new_filter_col, new_filter_op, new_filter_val)

            field_filter_array = []
            if new_filter_col and new_filter_op and new_filter_val:
                field_filter_array = [{'filter_col': new_filter_col,
                                       'filter_op': new_filter_op, 'filter_val': new_filter_val}]

            for fieldFilter in fieldFilters:
                new_filter_col = request.GET.get(
                    f'filter_col_{fieldFilter.id}')
                new_filter_op = request.GET.get(f'filter_op_{fieldFilter.id}')
                new_filter_val = request.GET.get(
                    f'filter_val_{fieldFilter.id}')
                if new_filter_col and new_filter_op and new_filter_val:
                    field_filter_array.append(
                        {'filter_col': new_filter_col, 'filter_op': new_filter_op, 'filter_val': new_filter_val})
            print(field_filter_array)

            # Delete previous objects
            if (new_filter_col and new_filter_op and new_filter_val) or field_filter_array:
                FieldFilter.objects.filter(entity=entity).delete()

            # # create all new
            for field_filter in field_filter_array:
                FieldFilter.objects.create(
                    entity=entity, filter_col=field_filter['filter_col'], filter_op=field_filter['filter_op'], filter_val=field_filter['filter_val'])
            return HttpResponseRedirect(f'/datapreparation/display/{entity.id}')
        return render(request, "rapidiamapp/datapreparation.html",
                      context={'entities': entities,'entity': entity, 'fields': fields, 'data': data,
                               'col_names': col_names, 'level_field': level_field+1,
                               'available_functions': available_functions,
                               'filters': filters,
                               'entity_columns_meta': entity_columns_meta,
                               'action': action,
                               'derived_tree':derived_tree,
                               'msg':msg
                               })

    if action == 'delete_filter':
        field_filter_id = request.GET.get('filter_id')
        FieldFilter.objects.get(id=field_filter_id).delete()
        return HttpResponse('deleted')

    if action == 'apply_filter':
        
        fieldFilters = FieldFilter.objects.filter(entity=entity)
        new_filter_col = request.POST['filter_col_0']
        new_filter_op = request.POST['filter_op_0']
        new_filter_val = request.POST['filter_val_0']
        print(new_filter_col, new_filter_op, new_filter_val)
        field_filter_array = [{'filter_col': new_filter_col,
                               'filter_op': new_filter_op, 'filter_val': new_filter_val}]

        for fieldFilter in fieldFilters:
            new_filter_col = request.POST[f'filter_col_{fieldFilter.id}']
            new_filter_op = request.POST[f'filter_op_{fieldFilter.id}']
            new_filter_val = request.POST[f'filter_val_{fieldFilter.id}']
            field_filter_array.append(
                {'filter_col': new_filter_col, 'filter_op': new_filter_op, 'filter_val': new_filter_val})
        print(field_filter_array)
        # Delete previous objects
        # FieldFilter.objects.filter(entity=entity).delete()

        # # create all new
        for field_filter in field_filter_array:
            FieldFilter.objects.create(
                entity=entity, filter_col=field_filter['filter_col'], filter_op=field_filter['filter_op'], filter_val=field_filter['filter_val'])
        return HttpResponse('ok')
    if action == 'get_function_params' or action == 'get_function_params_with_values' or action == 'get_function_params_with_value_update':
        if action == 'get_function_params_with_value_update':
            print("********************VALUE UPDATE ")
            if request.POST.getlist('submit_action_delete'):
                arg_id = request.POST.getlist('derived_field_arguments')[0]                
                derived_field_arg = DerivedFieldArgument.objects.get(id=int(arg_id))
                field_id = derived_field_arg.field.id
                field = Field.objects.get(id=field_id)
                # if field is used in any derived field args

                for args in DerivedFieldArgument.objects.all():
                    if field.name in args.argument_value:
                        f_id = args.field.id
                        f = Field.objects.get(id=f_id)
                        return HttpResponse(f"This field is used in {args.argument_name}, first delete {f}")
                field.delete()
                return HttpResponse('Deleted')
            
            
            print( request.POST.getlist('derived_field_arguments'))            
            for arg_id in request.POST.getlist('derived_field_arguments'):
                print(arg_id)
                derived_field_arg = DerivedFieldArgument.objects.get(id=int(arg_id))
                derived_field_arg.argument_value = request.POST[derived_field_arg.argument_name]
                derived_field_arg.save()
                
            return HttpResponse('Saved')
        if action == 'get_function_params_with_values':
            
            try:
                field_functions_id = request.GET['field_id']

                field = Field.objects.get(id=field_functions_id)
                print(field)
                function_id = field.function.id                
                function_meta = FunctionMeta.objects.get(id=function_id)
                arguments_meta = ArgumentMeta.objects.filter(
                    function__id=function_id)
                derived_field = DerivedFieldArgument.objects.filter(field=field.id)
                entity_columns_meta = get_table_columns(f"{entity.name}_meta")
                entity_columns_names = [col['name'] for col in entity_columns_meta]
                return render(request,'rapidiamapp/datapreparation/existingfuncform.html', context = {'entities': entities,'field_functions_id':field_functions_id,'entity':entity,'field':field,'derived_field':derived_field,'entity_columns_names':entity_columns_names})
            except Exception as e:
                logging.getLogger("error_logger").error(
                    "Unable to upload file. "+repr(e))
                messages.error(request, "Unable to upload file. "+repr(e))

        else:
            try:
                function_id = request.POST['function_id']
                function_meta = FunctionMeta.objects.get(id=function_id)
                arguments_meta = ArgumentMeta.objects.filter(
                    function__id=function_id)
                entity_columns_meta = get_table_columns(f"{entity.name}_meta")
                entity_columns_names = [col['name'] for col in entity_columns_meta]
                print(entity_columns_names)
                html = '<label>Field Name</label><input type="text" name="derived_field_name"/>'
                for arg in arguments_meta:
                    html += f'<label>{arg.name}</label>'
                    if arg.type == 'COLUMN':
                        html += f'<select name={arg.name}>'
                        for entity_col in entity_columns_names:
                            html += f'<option value="{entity_col}">{entity_col}</option>'
                        html += '</select>'
                    else:
                        if arg.name == "model_data_sql":
                            val = generate_cte_sql(id=entity.id)
                            qt_val = f"'{val}'"
                            print(qt_val)
                            html += f'<textarea type="{arg.type}" name="{arg.name}" >{qt_val}</textarea>' 
                        elif arg.name == "model_name":
                            val = f"'model_name_{entity.id}_{function_id}'"
                            html += f'<input type="{arg.type}" name="{arg.name}" value="{val}"/>' 
                        else:
                            html += f'<input type="{arg.type}" name="{arg.name}"/>' 
                return HttpResponse(html)
            except Exception as e:
                logging.getLogger("error_logger").error(
                    "Unable to upload file. "+repr(e))
                messages.error(request, "Unable to upload file. "+repr(e))

    if action == 'add_derived_field':
        derived_field_name = request.POST['derived_field_name']
        function_id = request.POST['function_id']
        level_field = request.POST['level_field']

        function = FunctionMeta.objects.get(id=function_id)
        arguments_meta = ArgumentMeta.objects.filter(
            function__id=function_id)
        provided_argument = []
        for arg in arguments_meta:
            val = request.POST[arg.name]

            # Not required as this in CTE
            # if arg.type == 'COLUMN':
            #     val = f"'{val}'"

            provided_argument.append(
                {"name": arg.name, "value": val, "type": arg.type})

        # argument_type = models.CharField(max_length=30, null=True)
        # create the derived field
        # field_level = get_level_of_fields(entity.id)
        derived_field = Field.objects.create(
            actual_name=derived_field_name, name=derived_field_name, entity=entity, type='DERIVED', datatype=function.return_type ,derived_level=level_field, function=function)
        derived_field.save()
        for parg in provided_argument:
            DerivedFieldArgument.objects.create(
                field=derived_field, argument_name=parg['name'], argument_value=parg['value'], argument_type=parg['type']).save()
        html = f'<span class="badge text-bg-primary">{function.name}</span>'
        return HttpResponse(html)




def datascience(request, action, id):
    entities = Entity.objects.all()
    print("***********************************")
    print(action, id)
    print("***********************************")
    data = {}
    entity = Entity.objects.get(id=id)
    if "GET" == request.method:
      
        # tree view of all fields level wise
        data_sql = generate_cte_sql(id)
        
        create_meta_table(entity.name, data_sql)
        data_sql = generate_cte_sql(id,'display')

        full_data_sql = generate_action_sql(data_sql, id, action)
        data, col_names,msg = fetch_raw_query(full_data_sql)        
        entity_columns_meta = get_table_columns(f"{entity.name}_meta")
        available_functions = FunctionMeta.objects.filter(type='DATASCIENCE')

        ds_fields = DataScience.objects.filter(entity=entity)
        print(available_functions,'===========')
        # functions_viz_meta = FunctionMeta.objects.filter(type='VISUALIZATION')
        # functions_ds_meta = FunctionMeta.objects.filter(type='DATASCIENCE')

        filters = FieldFilter.objects.filter(entity=entity)

        if action == 'apply_table_filter':
            print(request.GET)
            fieldFilters = FieldFilter.objects.filter(entity=entity)
            new_filter_col = request.GET.get('filter_col_0')
            new_filter_op = request.GET.get('filter_op_0')
            new_filter_val = request.GET.get('filter_val_0')
            print(new_filter_col, new_filter_op, new_filter_val)

            field_filter_array = []
            if new_filter_col and new_filter_op and new_filter_val:
                field_filter_array = [{'filter_col': new_filter_col,
                                       'filter_op': new_filter_op, 'filter_val': new_filter_val}]

            for fieldFilter in fieldFilters:
                new_filter_col = request.GET.get(
                    f'filter_col_{fieldFilter.id}')
                new_filter_op = request.GET.get(f'filter_op_{fieldFilter.id}')
                new_filter_val = request.GET.get(
                    f'filter_val_{fieldFilter.id}')
                if new_filter_col and new_filter_op and new_filter_val:
                    field_filter_array.append(
                        {'filter_col': new_filter_col, 'filter_op': new_filter_op, 'filter_val': new_filter_val})
            print(field_filter_array)

            # Delete previous objects
            if (new_filter_col and new_filter_op and new_filter_val) or field_filter_array:
                FieldFilter.objects.filter(entity=entity).delete()

            # # create all new
            for field_filter in field_filter_array:
                FieldFilter.objects.create(
                    entity=entity, filter_col=field_filter['filter_col'], filter_op=field_filter['filter_op'], filter_val=field_filter['filter_val'])
            return HttpResponseRedirect(f'/datascience/display/{entity.id}')
        return render(request, "rapidiamapp/datascience.html",
                      context={'entities': entities,'entity': entity, 'data': data,
                               'col_names': col_names,
                               'available_functions': available_functions,
                               'filters': filters,
                               'entity_columns_meta': entity_columns_meta,
                               'action': action,
                               'msg':msg,
                               'ds_fields':ds_fields,
                               })

    if action == 'delete_filter':
        field_filter_id = request.GET.get('filter_id')
        FieldFilter.objects.get(id=field_filter_id).delete()
        return HttpResponse('deleted')

    if action == 'apply_filter':
        
        fieldFilters = FieldFilter.objects.filter(entity=entity)
        new_filter_col = request.POST['filter_col_0']
        new_filter_op = request.POST['filter_op_0']
        new_filter_val = request.POST['filter_val_0']
        print(new_filter_col, new_filter_op, new_filter_val)
        field_filter_array = [{'filter_col': new_filter_col,
                               'filter_op': new_filter_op, 'filter_val': new_filter_val}]

        for fieldFilter in fieldFilters:
            new_filter_col = request.POST[f'filter_col_{fieldFilter.id}']
            new_filter_op = request.POST[f'filter_op_{fieldFilter.id}']
            new_filter_val = request.POST[f'filter_val_{fieldFilter.id}']
            field_filter_array.append(
                {'filter_col': new_filter_col, 'filter_op': new_filter_op, 'filter_val': new_filter_val})
        print(field_filter_array)
        # Delete previous objects
        # FieldFilter.objects.filter(entity=entity).delete()

        # # create all new
        for field_filter in field_filter_array:
            FieldFilter.objects.create(
                entity=entity, filter_col=field_filter['filter_col'], filter_op=field_filter['filter_op'], filter_val=field_filter['filter_val'])
        return HttpResponse('ok')
    if action == 'execute_datascience_function':
        data_sql = generate_cte_sql(id)
        # create_meta_table(entity.name, data_sql)
        full_data_sql = generate_action_sql(data_sql, id, 'display')

        print(full_data_sql)
        print("------------------------------------------------")
        print("------------------------------------------------")
        ds_field_id = request.GET.get('ds_field_id')
        field = DataScience.objects.get(id=ds_field_id)
        derived_field_arg = DerivedDataScienceArgument.objects.filter(field=field)

        # Create Entity

        # e = Entity.objects.get_or_create(name=field.name)[0]
        # print(e)
        # sql = f'drop table if exists {e.name}'
        # execute_raw_query(sql)
        # sql = f'create table {e.name} as {full_data_sql}'
        # execute_raw_query(sql)
        

        full_data_sql = full_data_sql.replace("'","''")
        derived_values = {d.argument_name: d.argument_value for d in derived_field_arg}
        derived_values['model_data_sql'] = f"'{full_data_sql}'"
        derived_values['execution_mode'] = f"'datascience_execution'"
        derived_values['name'] = field.name
        derived_function_sql = field.function.return_sql.format(**derived_values)
        train_sql = f"select {derived_function_sql}"
        print("------------------------------------------------")
        # print(train_sql)
        rows,cols,msg = fetch_raw_query(train_sql)
        # print(rows,cols,msg)
        # with open(f"C:\\3Projects\\downloads\\{field.name}.csv",'w') as f:
        #     f.write(str(rows[0]))
        print("------------------------------------------------")
        # print(rows[0][0])
        # predicted_value = json.loads(rows[0][0])
        # column_name = [k for k in predicted_value.keys()][0]
        # values = [k for k in predicted_value[column_name].values()]
        # sql_row = ','.join([f'({k})' for k in values])
        # sql = f'ALTER TABLE {e.name} ADD COLUMN "{column_name}" numeric DEFAULT 0;'
        # execute_raw_query(sql)
        # sql = f'insert into {e.name}("{column_name}") values {sql_row}' 
        # execute_raw_query(sql)
        return HttpResponse(rows[0])
    if action == 'get_function_params' or action == 'get_function_params_with_values' or action == 'get_function_params_with_value_update':
        if action == 'get_function_params_with_value_update':
            print("********************VALUE UPDATE ")
            if request.POST.getlist('submit_action_train'):                
                print("TRAINING")
                arg_id = request.POST.getlist('derived_field_arguments')[0]    
                derived_field_arg = DerivedDataScienceArgument.objects.get(id=int(arg_id))
                field_id = derived_field_arg.field.id
                field = DataScience.objects.get(id=field_id)
                derived_field_arguments = DerivedDataScienceArgument.objects.filter(
                    field=field.id)

                derived_values = {
                    d.argument_name: d.argument_value for d in derived_field_arguments}
                derived_values['name'] = field.name
                derived_function_sql = field.function.return_sql.format(**derived_values)
                train_sql = f"select {derived_function_sql}"
                print(train_sql)
                rows,cols,msg = fetch_raw_query(train_sql)
                print(rows,cols,msg)
                print("------------------------------------------------")
            if request.POST.getlist('submit_action_delete'):
                arg_id = request.POST.getlist('derived_field_arguments')[0]                
                derived_field_arg = DerivedDataScienceArgument.objects.get(id=int(arg_id))
                field_id = derived_field_arg.field.id
                field = DataScience.objects.get(id=field_id)
                # if field is used in any derived field args

                # for args in DerivedDataScienceArgument.objects.all():
                #     if field.name in args.argument_value:
                #         f_id = args.field.id
                #         f = DataScience.objects.get(id=f_id)
                #         return HttpResponse(f"This field is used in {args.argument_name}, first delete {f}")
                field.delete()
                return HttpResponse('Deleted')
            
            
            print( request.POST.getlist('derived_field_arguments'))            
            for arg_id in request.POST.getlist('derived_field_arguments'):
                print(arg_id)
                derived_field_arg = DerivedDataScienceArgument.objects.get(id=int(arg_id))
                derived_field_arg.argument_value = request.POST[derived_field_arg.argument_name]
                derived_field_arg.save()
                
            return HttpResponse('Saved')
        if action == 'get_function_params_with_values':
            
            try:
                field_functions_id = request.GET['ds_field_id']

                field = DataScience.objects.get(id=field_functions_id)
                print(field)
                function_id = field.function.id                
                function_meta = FunctionMeta.objects.get(id=function_id)
                arguments_meta = ArgumentMeta.objects.filter(
                    function__id=function_id)
                derived_field = DerivedDataScienceArgument.objects.filter(field=field.id)
                entity_columns_meta = get_table_columns(f"{entity.name}_meta")
                entity_columns_names = [f"'{col['name']}'" for col in entity_columns_meta]
                
                return render(request,'rapidiamapp/datapreparation/existingdsfuncform.html', context = {'entities': entities,'field_functions_id':field_functions_id,'entity':entity,'field':field,'derived_field':derived_field,'entity_columns_names':entity_columns_names})
            except Exception as e:
                logging.getLogger("error_logger").error(
                    "Unable to upload file. "+repr(e))
                messages.error(request, "Unable to upload file. "+repr(e))

        else:
            try:
                function_id = request.POST['function_id']
                function_meta = FunctionMeta.objects.get(id=function_id)
                arguments_meta = ArgumentMeta.objects.filter(
                    function__id=function_id)
                entity_columns_meta = get_table_columns(f"{entity.name}_meta")
                entity_columns_names = [col['name'] for col in entity_columns_meta]
                print(entity_columns_names)
                html = '<label>Field Name</label><input type="text" name="derived_field_name"/>'
                for arg in arguments_meta:
                    html += f'<label>{arg.name}</label>'
                    if arg.type == 'COLUMN':
                        html += f'<select name={arg.name}>'
                        for entity_col in entity_columns_names:
                            html += f'<option value="{entity_col}">{entity_col}</option>'
                        html += '</select>'
                    else:
                        if arg.name == "model_data_sql":
                            val = generate_cte_sql(id=entity.id)                            
                            html += f'<textarea type="{arg.type}" name="{arg.name}" >{val}</textarea>' 
                        elif arg.name == "model_name":
                            val = f"model_name_{entity.id}_{function_id}"
                            html += f'<input type="{arg.type}" name="{arg.name}" value="{val}"/>' 
                        else:
                            html += f'<input type="{arg.type}" name="{arg.name}"/>' 
                return HttpResponse(html)
            except Exception as e:
                logging.getLogger("error_logger").error(
                    "Unable to upload file. "+repr(e))
                messages.error(request, "Unable to upload file. "+repr(e))

    if action == 'add_derived_field':
        derived_field_name = request.POST['derived_field_name']
        function_id = request.POST['function_id']
        

        function = FunctionMeta.objects.get(id=function_id)
        arguments_meta = ArgumentMeta.objects.filter(
            function__id=function_id)
        provided_argument = []
        for arg in arguments_meta:
            val = request.POST[arg.name]

            if arg.type == 'COLUMN' or arg.type == 'TEXT':
                val = f"'{val}'"

            provided_argument.append(
                {"name": arg.name, "value": val, "type": arg.type})
        print("X_X_X_X__X_X_X_X_X_X_X_X_")
        print(provided_argument)
        # argument_type = models.CharField(max_length=30, null=True)
        # create the derived field
        # field_level = get_level_of_fields(entity.id)
        ds_field = DataScience.objects.create(
          name=derived_field_name, entity=entity,function=function)
        ds_field.save()
        for parg in provided_argument:
            DerivedDataScienceArgument.objects.create(
                field=ds_field, argument_name=parg['name'], argument_value=parg['value'], argument_type=parg['type']).save()
        html = f'<span class="badge text-bg-primary">{function.name}</span>'
        return HttpResponse(html)



def dataviz(request, action, id):
    entities = Entity.objects.all()
    print("***********************************")
    print(action, id)
    print("***********************************")
    data = {}
    entity = Entity.objects.get(id=id)
    if "GET" == request.method:
      

        fields = Field.objects.filter(
            entity=entity, derived_level__gte=1).order_by('derived_level')
        print(fields)

        
        data_sql = generate_cte_sql(id)
        create_meta_table(entity.name, data_sql)
        data_sql = generate_cte_sql(id,'display')

        full_data_sql = generate_action_sql(data_sql, id, action)
        data, col_names,msg = fetch_raw_query(full_data_sql)
        entity_columns_meta = get_table_columns(f"{entity.name}_meta")
        level_field = get_level_of_fields(id)
        available_functions = FunctionMeta.objects.filter().exclude(type='GENERATED')
        print(available_functions,'===========')
        # functions_viz_meta = FunctionMeta.objects.filter(type='VISUALIZATION')
        # functions_ds_meta = FunctionMeta.objects.filter(type='DATASCIENCE')

        filters = FieldFilter.objects.filter(entity=entity)

        if action == 'apply_table_filter':
            print(request.GET)
            fieldFilters = FieldFilter.objects.filter(entity=entity)
            new_filter_col = request.GET.get('filter_col_0')
            new_filter_op = request.GET.get('filter_op_0')
            new_filter_val = request.GET.get('filter_val_0')
            print(new_filter_col, new_filter_op, new_filter_val)

            field_filter_array = []
            if new_filter_col and new_filter_op and new_filter_val:
                field_filter_array = [{'filter_col': new_filter_col,
                                       'filter_op': new_filter_op, 'filter_val': new_filter_val}]

            for fieldFilter in fieldFilters:
                new_filter_col = request.GET.get(
                    f'filter_col_{fieldFilter.id}')
                new_filter_op = request.GET.get(f'filter_op_{fieldFilter.id}')
                new_filter_val = request.GET.get(
                    f'filter_val_{fieldFilter.id}')
                if new_filter_col and new_filter_op and new_filter_val:
                    field_filter_array.append(
                        {'filter_col': new_filter_col, 'filter_op': new_filter_op, 'filter_val': new_filter_val})
            print(field_filter_array)

            # Delete previous objects
            if (new_filter_col and new_filter_op and new_filter_val) or field_filter_array:
                FieldFilter.objects.filter(entity=entity).delete()

            # # create all new
            for field_filter in field_filter_array:
                FieldFilter.objects.create(
                    entity=entity, filter_col=field_filter['filter_col'], filter_op=field_filter['filter_op'], filter_val=field_filter['filter_val'])
            return HttpResponseRedirect(f'/dataviz/display/{entity.id}')
        # PG Walker
        eda = None
        if action == 'visualize':
            # df = pd.read_csv('C:\\3Projects\\newstockup\\indexes\\sensex_historical_gen.csv',parse_dates=['Date'])
            print("=========VISUALIZE SQL ==========================")
            print(full_data_sql)
            # full_data_sql = full_data_sql +  " where trade_date = '2023-05-09'"
            df = pd.read_sql(full_data_sql, engine)

            eda  = pyg.walk(df,hiddenDataSourceConfig=True, vegaTheme='vega',return_html=True)
        return render(request, "rapidiamapp/dataviz.html",
                      context={'entities': entities,'entity': entity, 'fields': fields, 'data': data,
                               'col_names': col_names, 'level_field': level_field+1,
                               'available_functions': available_functions,
                               'filters': filters,
                               'entity_columns_meta': entity_columns_meta,
                               'action': action,
                               'eda':eda,
                               'msg':msg
                               })

    if action == 'delete_filter':
        field_filter_id = request.GET.get('filter_id')
        FieldFilter.objects.get(id=field_filter_id).delete()
        return HttpResponse('deleted')

    if action == 'apply_filter':
        
        fieldFilters = FieldFilter.objects.filter(entity=entity)
        new_filter_col = request.POST['filter_col_0']
        new_filter_op = request.POST['filter_op_0']
        new_filter_val = request.POST['filter_val_0']
        print(new_filter_col, new_filter_op, new_filter_val)
        field_filter_array = [{'filter_col': new_filter_col,
                               'filter_op': new_filter_op, 'filter_val': new_filter_val}]

        for fieldFilter in fieldFilters:
            new_filter_col = request.POST[f'filter_col_{fieldFilter.id}']
            new_filter_op = request.POST[f'filter_op_{fieldFilter.id}']
            new_filter_val = request.POST[f'filter_val_{fieldFilter.id}']
            field_filter_array.append(
                {'filter_col': new_filter_col, 'filter_op': new_filter_op, 'filter_val': new_filter_val})
        print(field_filter_array)
        # Delete previous objects
        # FieldFilter.objects.filter(entity=entity).delete()

        # # create all new
        for field_filter in field_filter_array:
            FieldFilter.objects.create(
                entity=entity, filter_col=field_filter['filter_col'], filter_op=field_filter['filter_op'], filter_val=field_filter['filter_val'])
        return HttpResponse('ok')
    if action == 'get_function_params':
        print("++++++++++++++++++++++++++++++++++++++++++++++")
        try:
            function_id = request.POST['function_id']
            function_meta = FunctionMeta.objects.get(id=function_id)
            arguments_meta = ArgumentMeta.objects.filter(
                function__id=function_id)
            entity_columns_meta = get_table_columns(f"{entity.name}_meta")
            print("++++++++++++++++++++++++++++++++++++++++++++++",entity_columns_meta)
            print("++++++++++++++++++++++++++++++++++++++++++++++",entity_columns_meta)
            print("++++++++++++++++++++++++++++++++++++++++++++++",entity_columns_meta)
            entity_columns_names = [col['name'] for col in entity_columns_meta]
            html = '<label>Field Name</label><input type="text" name="derived_field_name"/>'
            for arg in arguments_meta:
                html += f'<label>{arg.name}</label>'
                if arg.type == 'COLUMN':
                    html += f'<select name={arg.name}>'
                    for entity_col in entity_columns_names:
                        html += f'<option value="{entity_col}">{entity_col}</option>'
                    html += '</select>'
                else:
                    html += f'<input type="{arg.type}" name="{arg.name}"/>'
            return HttpResponse(html)
        except Exception as e:
            logging.getLogger("error_logger").error(
                "Unable to upload file. "+repr(e))
            messages.error(request, "Unable to upload file. "+repr(e))

    if action == 'add_derived_field':
        derived_field_name = request.POST['derived_field_name']
        function_id = request.POST['function_id']
        level_field = request.POST['level_field']

        function = FunctionMeta.objects.get(id=function_id)
        arguments_meta = ArgumentMeta.objects.filter(
            function__id=function_id)
        provided_argument = []
        for arg in arguments_meta:
            val = request.POST[arg.name]
            provided_argument.append(
                {"name": arg.name, "value": val, "type": arg.type})

        # argument_type = models.CharField(max_length=30, null=True)
        # create the derived field
        # field_level = get_level_of_fields(entity.id)
        derived_field = Field.objects.create(
            actual_name=derived_field_name, name=derived_field_name, entity=entity, type='DERIVED', datatype=function.return_type ,derived_level=-1, function=function)
        derived_field.save()
        for parg in provided_argument:
            DerivedFieldArgument.objects.create(
                field=derived_field, argument_name=parg['name'], argument_value=parg['value'], argument_type=parg['type']).save()
        html = f'<span class="badge text-bg-primary">{function.name}</span>'
        return HttpResponse(html)
def process_function_import(function_json_import):
    loaded_function = json.loads(function_json_import)
    f = FunctionMeta.objects.create(name=loaded_function['name'],
                                    type=loaded_function['type'],
                                    return_type=loaded_function['return_type'],
                                    function_code=loaded_function['function_code'],
                                    test_code=loaded_function['test_code'],
                                    return_sql=loaded_function['return_sql'])
    for arg in loaded_function['args']:
        ArgumentMeta.objects.create(function=f,name=arg['name'],type=arg['type'])
     
def fieldfunction(request,action,id):
    print(request,action,id)
    function = None
    args_meta = None
    function_all = None
    entities = Entity.objects.all()
    if id:
        function = FunctionMeta.objects.get(id=id)
        args_meta = ArgumentMeta.objects.filter(function=function)
    else:
        function = FunctionMeta()
        args_meta = ArgumentMeta()

    if "GET" == request.method:
        if action == 'display' and id == 0:
            function_all = FunctionMeta.objects.all()
        if action == 'export':
        
            function_export = {}
            data = FunctionMeta.objects.get(id=id)
            function_export['name'] = data.name
            function_export['type'] = data.type
            function_export['return_type'] = data.return_type
            function_export['function_code'] = data.function_code
            function_export['test_code'] = data.test_code
            function_export['return_sql'] = data.return_sql
            function_export['args'] = []

            args = ArgumentMeta.objects.filter(function=data)
            for arg in args:
                function_export['args'].append({'name':arg.name,'type':arg.type})
            func_json = json.dumps(function_export)
            
           
            
            response = HttpResponse(func_json, content_type='application/json')
            response['Content-Disposition'] = f'attachment; filename={data.name}.json'    
            return response

    if "POST" == request.method:     
        if action == 'createderived' or action == 'createcalculated' or action == 'createdatascience':
            action_keys = {'createderived':'DERIVED','createcalculated':'GENERATED','createdatascience':'DATASCIENCE'}
            function_name = request.POST['name']
            func = FunctionMeta.objects.create(name=function_name,type=action_keys[action],return_type='TEXT')
            func.save()
            return HttpResponseRedirect(f'/fieldfunction/edit/{func.id}')

    
        if action == 'uploadfunctiondata':
            
            fform = UploadFunctionForm(request.POST, request.FILES)  
            if fform.is_valid():  
                file = request.FILES['function_file']
                process_function_import(file.read())
                return HttpResponseRedirect('/fieldfunction/display/0')
            
        if action == 'edit':
            if 'submit_action_delete' in request.POST:
                # if used in derived field then don't delete
                function =   FunctionMeta.objects.get(id=id)
                field =  Field.objects.filter(function=function).values_list('name',flat=True)
                field_names = [f for f in field ]
                field_names_joined = ','.join(field_names)
                if len(field_names) > 0:
                    messages.error(request, f"Following fields references this functions [{field_names_joined}], please delete them first")
                else:
                    function.delete()
                    return HttpResponseRedirect(f'/fieldfunction/display/0')

            elif 'submit_action_edit' in request.POST:       
                form_params = request.POST
                name = form_params['name']
                return_type = form_params['return_type']
                return_sql = form_params['return_sql']
                function_code = form_params['function_code']
                test_code = form_params['test_code']
                
                function =   FunctionMeta.objects.get(id=id)
                function.name = name
                function.return_type = return_type
                function.return_sql = return_sql
                function.function_code = function_code
                function.test_code = test_code
                function.save()
                if function_code and len(function_code) > 0:
                    execute_raw_query(function_code)
            elif 'submit_action_test' in request.POST:    
                form_params = request.POST   
                
                test_code = form_params['test_code']
                rows,cols,msg = fetch_raw_query(test_code)
                if msg:
                    messages.error(request, f"there was an error: [{msg}]")
                else:
                    messages.error(request, rows)


            
        if action == 'change_param_datatype':
            
            param_name = request.POST['param_name']
            
            if param_name == 'new_parameter':
                # HTMX Does not pass field id
                field_id = request.POST['new_parameter_field_id']
                new_parameter_name = request.POST['new_parameter_name']
                param_type = request.POST['param-name-id-null']                
                function = FunctionMeta.objects.get(id=id)
                arg_meta = ArgumentMeta(function=function,name=new_parameter_name,type=param_type)
                arg_meta.save()
                print('saved',new_parameter_name,param_type)
                return HttpResponse()
            key = list(request.POST.keys())[3]
            type = request.POST[key]
           
            args_id = key.split('param-name-id-')[1]
            arg_meta = ArgumentMeta.objects.get(id=args_id)
            if type == 'DELETE':
                arg_meta.delete()
                return HttpResponse()
            arg_meta.type = type
            arg_meta.save()
            return HttpResponse()
    
        return HttpResponseRedirect(f'/fieldfunction/edit/{id}')
        
    
    return render(request,'rapidiamapp/fieldfunction.html', context={'entities':entities,'function_all':function_all,'function':function,'args_meta':args_meta,'action':action,'function_types':FUNCTION_TYPES,'upload_function_form':UploadFunctionForm()})


def dataalerts(request, action, id):
  
    entities = Entity.objects.all()
    print("***********************************")
    print(action, id)
    print("***********************************")
    data = {}
    entity = Entity.objects.get(id=id)
    if "GET" == request.method:
      

        fields = Field.objects.filter(
            entity=entity, derived_level__gte=1).order_by('derived_level')
        print(fields)

        data_sql = generate_cte_sql(id)
        create_meta_table(entity.name, data_sql)
        data_sql = generate_cte_sql(id,'display')
        

        full_data_sql = generate_action_sql(data_sql, id, 'alert')
        data, col_names,msg = fetch_raw_query(full_data_sql)

    
        entity_columns_meta = get_table_columns(f"{entity.name}_meta")

        filters = DataAlertFieldFilter.objects.filter(entity=entity)

        schedule_job = ScheduleJob.objects.filter(entity=entity).first()

        if action == 'apply_table_filter':
            print(request.GET)
            fieldFilters = DataAlertFieldFilter.objects.filter(entity=entity)
            new_filter_col = request.GET.get('filter_col_0')
            new_filter_op = request.GET.get('filter_op_0')
            new_filter_val = request.GET.get('filter_val_0')
            print(new_filter_col, new_filter_op, new_filter_val)

            field_filter_array = []
            if new_filter_col and new_filter_op and new_filter_val:
                field_filter_array = [{'filter_col': new_filter_col,
                                       'filter_op': new_filter_op, 'filter_val': new_filter_val}]

            for fieldFilter in fieldFilters:
                new_filter_col = request.GET.get(
                    f'filter_col_{fieldFilter.id}')
                new_filter_op = request.GET.get(f'filter_op_{fieldFilter.id}')
                new_filter_val = request.GET.get(
                    f'filter_val_{fieldFilter.id}')
                if new_filter_col and new_filter_op and new_filter_val:
                    field_filter_array.append(
                        {'filter_col': new_filter_col, 'filter_op': new_filter_op, 'filter_val': new_filter_val})
            print(field_filter_array)

            # Delete previous objects
            if (new_filter_col and new_filter_op and new_filter_val) or field_filter_array:
                DataAlertFieldFilter.objects.filter(entity=entity).delete()
            
           
            # # create all new
            for field_filter in field_filter_array:
                DataAlertFieldFilter.objects.create(
                    entity=entity, filter_col=field_filter['filter_col'], filter_op=field_filter['filter_op'], filter_val=field_filter['filter_val'])
            return HttpResponseRedirect(f'/dataalerts/display/{entity.id}')
        # PG Walker

        return render(request, "rapidiamapp/dataalerts.html",
                      context={'entities': entities,'entity': entity, 'fields': fields, 'data': data,
                               'col_names': col_names, 
                               'filters': filters,
                               'entity_columns_meta': entity_columns_meta,
                               'action': action,
                               'msg':msg,
                               'schedule_job':schedule_job,
                               })

    if action == 'delete_filter':
        field_filter_id = request.GET.get('filter_id')
        DataAlertFieldFilter.objects.get(id=field_filter_id).delete()
        return HttpResponse('deleted')

    if action == 'apply_schedule':
        data_sql = generate_cte_sql(id)
        full_data_sql = generate_action_sql(data_sql, id, 'alert')
       
        full_data_sql =f"with cte_json as ({full_data_sql}) select to_json(cte_json) from cte_json"

        # first delete exising schedule
        callback_url = request.POST['callback_url']
        sched_min = request.POST['sched_min']
      
        sc_job = ScheduleJob.objects.filter(entity=entity).first()
        if sc_job:            
            sc_job.callback_url = callback_url
            sc_job.sched_min = sched_min
            sc_job.save()
        else:      
            ScheduleJob.objects.create(entity=entity,job_sql=full_data_sql,sched_min=sched_min,callback_url=callback_url,last_run= timezone.localtime(timezone.now()))
        
        return HttpResponseRedirect(f'/dataalerts/display/{entity.id}')


def handle_uploaded_file(f):
    with open('some/file/name.txt', 'wb+') as destination:
        for chunk in f.chunks():
            destination.write(chunk)


def upload_csv(request):
    data = {}
    if "GET" == request.method:
        return render(request, "myapp/upload_csv.html", data)
    # if not GET, then proceed
    try:
        csv_file = request.FILES["csv_file"]
        if not csv_file.name.endswith('.csv'):
            messages.error(request, 'File is not CSV type')
            return HttpResponseRedirect(reverse("myapp:upload_csv"))
    # if file is too large, return
        if csv_file.multiple_chunks():
            messages.error(request, "Uploaded file is too big (%.2f MB)." % (
                csv_file.size/(1000*1000),))
            return HttpResponseRedirect(reverse("myapp:upload_csv"))

        file_data = csv_file.read().decode("utf-8")

        lines = file_data.split("\n")
        # loop over the lines and save them in db. If error , store as string and then display
        for line in lines:
            fields = line.split(",")
            data_dict = {}
            data_dict["name"] = fields[0]
            data_dict["start_date_time"] = fields[1]
            data_dict["end_date_time"] = fields[2]
            data_dict["notes"] = fields[3]
            try:
                form = EventsForm(data_dict)
                if form.is_valid():
                    form.save()
                else:
                    logging.getLogger("error_logger").error(
                        form.errors.as_json())
            except Exception as e:
                logging.getLogger("error_logger").error(repr(e))
                pass

    except Exception as e:
        logging.getLogger("error_logger").error(
            "Unable to upload file. "+repr(e))
        messages.error(request, "Unable to upload file. "+repr(e))

    return HttpResponseRedirect(reverse("myapp:upload_csv"))


def execute_raw_query(sql):
    # print("============execute_raw_query=====================")
    # print(sql)
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql)
    except Exception as e:
        print("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
        print("Exception in execute raw query")
        print(e)
        print(sql)


def fetch_raw_query(sql):
    print("============fetch_raw_query=====================")
    print(sql)
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql)
            col_names = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()

        return rows, col_names,None
    except Exception as e:
        print(e)
        return None,None,traceback.format_exc()


def get_level_of_fields(id):
    top_level = Field.objects.filter(entity__id=id).order_by(
        '-derived_level').values_list('derived_level')
    return top_level[0][0]


def generate_cte_sql(id, action=None):
    entity = Entity.objects.get(id=id)

    field_level = get_level_of_fields(id) + 1

    # field_filter = FieldFilter.objects.filter(entity=entity)
    # entity_columns_meta = get_table_columns(f"{entity.name}_meta")

    data_sql = None
    for i in range(field_level):
        if i == 0:
            all_child_entity_fields = None
            # parent fields
            fields = Field.objects.filter(entity__id=id, derived_level=i).values_list('name','type')
            fields = list(fields)
            # for field in fields:
            #     if field.child_entity_id and field.child_field_id:
            #         all_child_entity_fields = Field.objects.filter(
            #             entity__id=field.child_entity_id)
            # if all_child_entity_fields:
            #     f = fields.union(all_child_entity_fields,
            #                      all=True).values_list('name','type', named=True)
            #     f = [replace_clean(c) for c in f]
            #     fields_sql = ",".join(f)
                
            entity_childrens = EntityChildren.objects.filter(parent_entity=entity)
            
            if entity_childrens:
                # get all fields and children fields
                left_join = ''
                for children in entity_childrens:
                    field_list = Field.objects.filter(entity__id=children.child_entity.id, derived_level=i).values_list('name','type')
                    field_list = list(field_list)

                    fields = fields + field_list
              
                    left_join += f' left join {children.child_entity.name} on {children.parent_entity}.{children.parent_field.name} = {children.child_entity}.{children.child_field.name}'
                f = [replace_clean(list(c)) for c in fields]
                fields_sql = ",".join(f)
                
                data_sql = f" with cte_{i} as ( select {fields_sql} from {entity.name} {left_join} "
            else:
                f = [replace_clean(list(c)) for c in fields]
                fields_sql = ",".join(f)
                data_sql = f"with cte_{i} as ( select {fields_sql} from {entity.name}"

        else:
            # All fields from level 1 are derived
            fields = Field.objects.filter(entity__id=id, derived_level=i).exclude(type = 'CALCULATION')
            data_sql += f'),cte_{i} as ( select *,'
            for field in fields:
                function_meta = field.function

                derived_field_arguments = DerivedFieldArgument.objects.filter(
                    field=field.id)

                derived_values = {
                    d.argument_name: d.argument_value for d in derived_field_arguments}
                derived_values['name'] = field.name
                derived_function_sql = function_meta.return_sql.format(**derived_values)

                data_sql += f'{derived_function_sql},'

            data_sql = f'{data_sql[:-1]} from cte_{i-1}'

        

    # rank_sql = f"cte_3 as (select *,RANK() OVER (PARTITION BY code ORDER BY trade_date desc) AS rank from cte_2"
    all_fields =list(Field.objects.filter(entity=entity,visible=True).values_list('name',flat=True))
    print(all_fields)
    all_fields = ','.join(all_fields)

    if action == 'display':  
        data_sql += f") select {all_fields} from cte_{field_level-1}"
    else:
        data_sql += f") select * from cte_{field_level-1}"
    return data_sql


def generate_action_sql(sql, id, action=None):
    entity = Entity.objects.get(id=id)

    # this is for data alert
    field_filter = FieldFilter.objects.filter(entity=entity)
    if action == 'alert':
         field_filter = DataAlertFieldFilter.objects.filter(entity=entity)
   
    entity_columns_meta = get_table_columns(f"{entity.name}_meta")

    where_list = ''
    for filter in field_filter:
        # Get type, it will determine whether to wrap in '
        col_type = None
        for col_meta in entity_columns_meta:
            if filter.filter_col == col_meta['name']:
                col_type = col_meta['type']

        # Based on type identity
   
        if col_type and col_type.lower() in ['text', 'date']:
            where_list += f" {filter.filter_col} {SQL_OP_TYPES[filter.filter_op]} '{filter.filter_val}' and "
        else:
            where_list += f" {filter.filter_col} {SQL_OP_TYPES[filter.filter_op]} {filter.filter_val} and "


    
   
    where_list = where_list[:-4]

   
    if where_list and action in ['display','visualize','alert']:
        sql += f" where {where_list}"
    else:
        sql += f" limit 500 "

 

    return sql

def replace_clean_upload(str):
    return str.replace('.', '').replace('-', '_').replace("'", "").replace('"', '').replace(' ', '_').replace('\n', '').replace('\r', '').replace('\r\n', '')    

def replace_clean(str):
    print(str)
    name = str[0]
    type = str[1]
    name = name.replace('.', '').replace('-', '_').replace("'", "").replace('"', '').replace(' ', '_').replace('\n', '').replace('\r', '').replace('\r\n', '')
    if type and type == 'COLUMN':
        return f'"{name}"'
    return name

def create_meta_table(entity_name, data_sql):
    sql = f"drop table if exists {entity_name}_meta"
    execute_raw_query(sql)

    sql = f"create table {entity_name}_meta as {data_sql} limit 1"
    execute_raw_query(sql)


def get_table_columns(tname):
    sql = f'''
        SELECT column_name, data_type 
        FROM information_schema.columns
        WHERE table_name = '{tname}' AND table_schema = 'public';
    '''
    table_cols = fetch_raw_query(sql)
    cols = []
    for col in table_cols[0]:
        cols.append({"name": col[0], 'type': col[1]})
    return cols

import threading
import time

import schedule


def run_continuously(interval=1):
    """Continuously run, while executing pending jobs at each
    elapsed time interval.
    @return cease_continuous_run: threading. Event which can
    be set to cease continuous run. Please note that it is
    *intended behavior that run_continuously() does not run
    missed jobs*. For example, if you've registered a job that
    should run every minute and you set a continuous run
    interval of one hour then your job won't be run 60 times
    at each interval but only once.
    """
    cease_continuous_run = threading.Event()

    class ScheduleThread(threading.Thread):
        @classmethod
        def run(cls):
            while not cease_continuous_run.is_set():
                schedule.run_pending()
                time.sleep(interval)

    continuous_thread = ScheduleThread()
    continuous_thread.start()
    return cease_continuous_run

from datetime import datetime,timezone
import requests
import json
from django.utils import timezone

def background_job():
    print('Hello from the background thread')
    jobs = ScheduleJob.objects.all()
    for job in jobs:
        if job.last_run:
            
            current_date = timezone.localtime(timezone.now())
            print(current_date,job.last_run)
            duration = current_date - job.last_run
            duration_in_s = duration.total_seconds() 
            duration_in_m = duration_in_s/60
            print("XXXXXXXXXXXXXXXXXXXXXXXXXX",duration_in_m)
            if True or duration_in_m > job.sched_min:
                # run the job                
                res,cols,msg = fetch_raw_query(job.job_sql)

                x = requests.post(job.callback_url, json={"payload":res})
                print(x)
                sc_job = ScheduleJob.objects.get(id=job.id)
                sc_job.last_run = timezone.localtime(timezone.now())
                sc_job.save()
                                                     
      
        


schedule.every().minute.do(background_job)

# Start the background thread
# stop_run_continuously = run_continuously()

# Do some other things...
# time.sleep(10)

# Stop the background thread
# stop_run_continuously.set()
from django.views.decorators.csrf import csrf_exempt

@csrf_exempt
def callback_url(request):
    if request.method == 'GET':
        return HttpResponse("hello world")
    else:      
        received_json_data=json.loads(request.body)
        print(received_json_data)
        return HttpResponse('ok')