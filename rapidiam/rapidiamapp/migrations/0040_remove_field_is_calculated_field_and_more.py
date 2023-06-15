# Generated by Django 4.2 on 2023-05-10 07:59

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('rapidiamapp', '0039_field_is_calculated_field'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='field',
            name='is_calculated_field',
        ),
        migrations.AlterField(
            model_name='functionmeta',
            name='type',
            field=models.CharField(choices=[('CALCULATION', 'Calculation'), ('VISUALIZE', 'Visualize'), ('DATASCIENCE', 'DataScience'), ('CALCULATED', 'CALCULATED')], default='CALCULATION', max_length=20),
        ),
    ]
