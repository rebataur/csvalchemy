# Generated by Django 4.1.6 on 2023-02-06 21:09

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('rapidiamapp', '0019_remove_field_derived_type_and_more'),
    ]

    operations = [
        migrations.RenameModel(
            old_name='DerivedFieldArguments',
            new_name='DerivedFieldArgument',
        ),
    ]
