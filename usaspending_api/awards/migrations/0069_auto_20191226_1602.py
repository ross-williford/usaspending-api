# -*- coding: utf-8 -*-
# Generated by Django 1.11.26 on 2019-12-26 16:02
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('awards', '0068_auto_20191113_1134'),
    ]

    operations = [
        migrations.AddField(
            model_name='subaward',
            name='prime_award_primary_place_of_performance_scope',
            field=models.TextField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='transactionfabs',
            name='primary_place_of_performance_scope',
            field=models.TextField(blank=True, null=True),
        ),
    ]
