# Generated by Django 4.1.7 on 2023-06-15 11:04

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('rapidiamapp', '0056_remove_entitychildren_child_field_id_and_more'),
    ]

    operations = [
        migrations.AddField(
            model_name='entitychildren',
            name='child_entity',
            field=models.ForeignKey(null=True, on_delete=django.db.models.deletion.DO_NOTHING, related_name='child_entity', to='rapidiamapp.entity'),
        ),
        migrations.AlterField(
            model_name='entitychildren',
            name='parent_entity',
            field=models.ForeignKey(null=True, on_delete=django.db.models.deletion.DO_NOTHING, related_name='parent_entity', to='rapidiamapp.entity'),
        ),
    ]
