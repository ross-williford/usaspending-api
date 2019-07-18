# -*- coding: utf-8 -*-
# Generated by Django 1.11.20 on 2019-07-05 18:40
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('references', '0026_toptieragency_justification'),
    ]

    operations = [
        migrations.CreateModel(
            name='CGAC',
            fields=[
                ('cgac_code', models.TextField(primary_key=True, serialize=False)),
                ('agency_name', models.TextField()),
                ('agency_abbreviation', models.TextField(blank=True, null=True)),
            ],
            options={
                'db_table': 'cgac',
            },
        ),
    ]