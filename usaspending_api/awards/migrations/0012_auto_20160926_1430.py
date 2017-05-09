# -*- coding: utf-8 -*-
# Generated by Django 1.10.1 on 2016-09-26 18:30
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('awards', '0011_auto_20160926_1358'),
    ]

    operations = [
        migrations.AddField(
            model_name='financialassistanceaward',
            name='award_transaction_usaspend',
            field=models.DecimalField(blank=True, decimal_places=0, max_digits=20, null=True),
        ),
        migrations.AddField(
            model_name='financialassistanceaward',
            name='current_total_value_award',
            field=models.DecimalField(blank=True, decimal_places=0, max_digits=20, null=True),
        ),
        migrations.AddField(
            model_name='financialassistanceaward',
            name='potential_total_value_adju',
            field=models.DecimalField(blank=True, decimal_places=0, max_digits=20, null=True),
        ),
        migrations.AddField(
            model_name='procurement',
            name='award_transaction_usaspend',
            field=models.DecimalField(blank=True, decimal_places=0, max_digits=20, null=True),
        ),
    ]