# Generated by Django 4.1.6 on 2023-02-17 10:25

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('rapidiamapp', '0027_rename_filter_value_fieldfilter_filter_val_and_more'),
    ]

    operations = [
        migrations.RenameField(
            model_name='fieldfilter',
            old_name='field_col',
            new_name='filter_col',
        ),
    ]
