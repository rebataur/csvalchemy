# Generated by Django 4.1.7 on 2023-06-15 14:29

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('rapidiamapp', '0060_remove_field_child_entity_id_and_more'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='field',
            name='entity_field_name',
        ),
    ]
