# Generated by Django 4.1.7 on 2023-06-12 17:51

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('rapidiamapp', '0050_datasciencetraining'),
    ]

    operations = [
        migrations.DeleteModel(
            name='DataScienceTraining',
        ),
        migrations.AddField(
            model_name='functionmeta',
            name='test_code',
            field=models.TextField(default=''),
            preserve_default=False,
        ),
        migrations.AlterField(
            model_name='argumentmeta',
            name='type',
            field=models.CharField(choices=[('TEXT', 'Text'), ('INTEGER', 'Integer'), ('NUMERIC', 'Numeric'), ('DATE', 'Date'), ('BINARY', 'Binary'), ('PNG_IMAGE', 'PNG Image'), ('COLUMN', 'Column Name')], default='TEXT', max_length=20),
        ),
        migrations.AlterField(
            model_name='field',
            name='datatype',
            field=models.CharField(choices=[('TEXT', 'Text'), ('INTEGER', 'Integer'), ('NUMERIC', 'Numeric'), ('DATE', 'Date'), ('BINARY', 'Binary'), ('PNG_IMAGE', 'PNG Image'), ('COLUMN', 'Column Name')], default='TEXT', max_length=64),
        ),
        migrations.AlterField(
            model_name='functionmeta',
            name='return_type',
            field=models.CharField(choices=[('TEXT', 'Text'), ('INTEGER', 'Integer'), ('NUMERIC', 'Numeric'), ('DATE', 'Date'), ('BINARY', 'Binary'), ('PNG_IMAGE', 'PNG Image'), ('COLUMN', 'Column Name')], default='TEXT', max_length=20),
        ),
    ]
