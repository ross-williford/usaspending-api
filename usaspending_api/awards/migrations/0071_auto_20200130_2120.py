# Generated by Django 2.2.9 on 2020-01-30 21:20

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('awards', '0070_auto_20200116_1354'),
    ]

    operations = [
        migrations.AlterField(
            model_name='award',
            name='awarding_agency',
            field=models.ForeignKey(help_text='The awarding agency for the award', null=True, on_delete=django.db.models.deletion.DO_NOTHING, related_name='+', to='references.Agency'),
        ),
        migrations.AlterField(
            model_name='award',
            name='earliest_transaction',
            field=models.ForeignKey(help_text='The earliest transaction by action_date and mod associated with this award', null=True, on_delete=django.db.models.deletion.DO_NOTHING, related_name='earliest_for_award', to='awards.TransactionNormalized'),
        ),
        migrations.AlterField(
            model_name='award',
            name='funding_agency',
            field=models.ForeignKey(help_text='The funding agency for the award', null=True, on_delete=django.db.models.deletion.DO_NOTHING, related_name='+', to='references.Agency'),
        ),
        migrations.AlterField(
            model_name='award',
            name='latest_transaction',
            field=models.ForeignKey(help_text='The latest transaction by action_date and mod associated with this award', null=True, on_delete=django.db.models.deletion.DO_NOTHING, related_name='latest_for_award', to='awards.TransactionNormalized'),
        ),
        migrations.AlterField(
            model_name='transactionnormalized',
            name='award',
            field=models.ForeignKey(help_text='The award which this transaction is contained in', on_delete=django.db.models.deletion.DO_NOTHING, to='awards.Award'),
        ),
        migrations.AlterField(
            model_name='transactionnormalized',
            name='awarding_agency',
            field=models.ForeignKey(help_text='The agency which awarded this transaction', null=True, on_delete=django.db.models.deletion.DO_NOTHING, related_name='awards_transactionnormalized_awarding_agency', to='references.Agency'),
        ),
        migrations.AlterField(
            model_name='transactionnormalized',
            name='funding_agency',
            field=models.ForeignKey(help_text='The agency which is funding this transaction', null=True, on_delete=django.db.models.deletion.DO_NOTHING, related_name='awards_transactionnormalized_funding_agency', to='references.Agency'),
        ),
    ]
