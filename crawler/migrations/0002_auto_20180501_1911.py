# -*- coding: utf-8 -*-
# Generated by Django 1.11.12 on 2018-05-01 19:11
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('crawler', '0001_initial'),
    ]

    operations = [
        migrations.AlterField(
            model_name='domain',
            name='domain',
            field=models.CharField(max_length=255, unique=True),
        ),
    ]
