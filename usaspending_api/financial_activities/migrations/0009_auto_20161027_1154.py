# -*- coding: utf-8 -*-
# Generated by Django 1.10.1 on 2016-10-27 15:54
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('financial_activities', '0008_auto_20161006_1351'),
    ]

    operations = [
        migrations.AddField(
            model_name='financialaccountsbyprogramactivityobjectclass',
            name='certified_date',
            field=models.DateField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='financialaccountsbyprogramactivityobjectclass',
            name='last_modified_date',
            field=models.DateField(blank=True, null=True),
        ),
    ]
