# -*- coding: utf-8 -*-
# Generated by Django 1.11.27 on 2020-01-07 19:46
from __future__ import unicode_literals

from django.db import migrations


class Migration(migrations.Migration):

    initial = True

    dependencies = [
        ('awards', '0070_move_matview_models'),
    ]

    # You may notice that the unmanaged models here have no fields.  This is an intentional
    # attempt to cut back a bit on unnecessary migration code.  Unmanaged models do not require
    # fields unless they are referenced by another model so this should cause no harm.
    operations = [
        migrations.CreateModel(
            name='AwardSearchView',
            fields=[],
            options={
                'db_table': 'vw_award_search',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='AwardSummaryMatview',
            fields=[],
            options={
                'db_table': 'mv_award_summary',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='ContractAwardSearchMatview',
            fields=[],
            options={
                'db_table': 'mv_contract_award_search',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='DirectPaymentAwardSearchMatview',
            fields=[],
            options={
                'db_table': 'mv_directpayment_award_search',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='GrantAwardSearchMatview',
            fields=[],
            options={
                'db_table': 'mv_grant_award_search',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='IDVAwardSearchMatview',
            fields=[],
            options={
                'db_table': 'mv_idv_award_search',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='LoanAwardSearchMatview',
            fields=[],
            options={
                'db_table': 'mv_loan_award_search',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='OtherAwardSearchMatview',
            fields=[],
            options={
                'db_table': 'mv_other_award_search',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='Pre2008AwardSearchMatview',
            fields=[],
            options={
                'db_table': 'mv_pre2008_award_search',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='SubawardView',
            fields=[],
            options={
                'db_table': 'subaward_view',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='SummaryCfdaNumbersView',
            fields=[],
            options={
                'db_table': 'summary_view_cfda_number',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='SummaryNaicsCodesView',
            fields=[],
            options={
                'db_table': 'summary_view_naics_codes',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='SummaryPscCodesView',
            fields=[],
            options={
                'db_table': 'summary_view_psc_codes',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='SummaryStateView',
            fields=[],
            options={
                'db_table': 'summary_state_view',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='SummaryTransactionFedAcctView',
            fields=[],
            options={
                'db_table': 'summary_transaction_fed_acct_view',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='SummaryTransactionGeoView',
            fields=[],
            options={
                'db_table': 'summary_transaction_geo_view',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='SummaryTransactionMonthView',
            fields=[],
            options={
                'db_table': 'summary_transaction_month_view',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='SummaryTransactionRecipientView',
            fields=[],
            options={
                'db_table': 'summary_transaction_recipient_view',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='SummaryTransactionView',
            fields=[],
            options={
                'db_table': 'summary_transaction_view',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='SummaryView',
            fields=[],
            options={
                'db_table': 'summary_view',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='UniversalTransactionView',
            fields=[],
            options={
                'db_table': 'universal_transaction_matview',
                'managed': False,
            },
        ),
    ]
