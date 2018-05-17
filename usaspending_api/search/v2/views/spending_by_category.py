import copy
import logging

from django.conf import settings
from django.db.models import Sum
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.awards.v2.filters.sub_award import subaward_filter
from usaspending_api.awards.v2.filters.view_selector import spending_by_category as sbc_view_queryset
from usaspending_api.common.api_versioning import api_transformations, API_TRANSFORM_FUNCTIONS
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.helpers.api_helper import alias_response
from usaspending_api.common.helpers.generic_helper import get_simple_pagination_metadata
from usaspending_api.core.validator.award_filter import AWARD_FILTER
from usaspending_api.core.validator.pagination import PAGINATION
from usaspending_api.core.validator.tinyshield import TinyShield

logger = logging.getLogger(__name__)

API_VERSION = settings.API_VERSION

ALIAS_DICT = {
    'awarding_agency': {'awarding_toptier_agency_name': 'agency_name', 'awarding_subtier_agency_name': 'agency_name',
                        'awarding_toptier_agency_abbreviation': 'agency_abbreviation',
                        'awarding_subtier_agency_abbreviation': 'agency_abbreviation'},
    'funding_agency': {'funding_toptier_agency_name': 'agency_name', 'funding_subtier_agency_name': 'agency_name',
                       'funding_toptier_agency_abbreviation': 'agency_abbreviation',
                       'funding_subtier_agency_abbreviation': 'agency_abbreviation'},
    'duns': {'recipient_unique_id': 'legal_entity_id'},
    'cfda': {'cfda_number': 'cfda_program_number', 'cfda_popular_name': 'popular_name',
             'cfda_title': 'popular_title'},
    'psc': {'product_or_service_code': 'psc_code'},
    'naics': {},

}
ALIAS_DICT['awarding_subagency'] = ALIAS_DICT['awarding_agency']
ALIAS_DICT['funding_subagency'] = ALIAS_DICT['funding_agency']
ALIAS_DICT['parent_duns'] = ALIAS_DICT['duns']


@api_transformations(api_version=API_VERSION, function_list=API_TRANSFORM_FUNCTIONS)
class SpendingByCategoryVisualizationViewSet(APIView):
    """
    This route takes award filters, and returns spending by the defined category/scope.
    The category is defined by the category keyword, and the scope is defined by is denoted by the scope keyword.
    endpoint_doc: /advanced_award_search/spending_by_category.md
    """
    @cache_response()
    def post(self, request: dict):
        """Return all budget function/subfunction titles matching the provided search text"""
        categories = [
            'awarding_agency', 'awarding_subagency', 'funding_agency', 'funding_subagency',
            'duns', 'parent_duns',
            'cfda', 'psc', 'naics']
        models = [
            {'name': 'category', 'key': 'category', 'type': 'enum', 'enum_values': categories, 'optional': False},
            {'name': 'subawards', 'key': 'subawards', 'type': 'boolean', 'default': False, 'optional': True}
        ]
        models.extend(copy.deepcopy(AWARD_FILTER))
        models.extend(copy.deepcopy(PAGINATION))

        # Apply/enforce POST body schema and data validation in request
        validated_payload = TinyShield(models).block(request.data)

        # Execute the business logic for the endpoint and return a python dict to be converted to a Django response
        return Response(BusinessLogic(validated_payload).results())


class BusinessLogic:
    # __slots__ will keep this object smaller
    __slots__ = (
        'subawards', 'category', 'page', 'limit', 'obligation_column',
        'lower_limit', 'upper_limit', 'filters', 'queryset',
    )

    def __init__(self, payload: dict):
        """
            payload is tightly integrated with
        """
        self.subawards = payload['subawards']
        self.category = payload['category']
        self.page = payload['page']
        self.limit = payload['limit']
        self.filters = payload.get('filters', {})

        self.lower_limit = (self.page - 1) * self.limit
        self.upper_limit = self.page * self.limit + 1  # Add 1 for simple "Next Page" check

        if self.subawards:
            self.queryset = subaward_filter(self.filters)
            self.obligation_column = 'amount'
        else:
            self.queryset = sbc_view_queryset(self.category, self.filters)
            self.obligation_column = 'generated_pragmatic_obligation'

    def raise_not_implemented(self):
        msg = "Category '{}' is not implemented"
        if self.subawards:
            msg += ' when `subawards` is True'
        raise InvalidParameterException(msg.format(self.category))

    def common_db_query(self, filters, values):
        return self.queryset \
            .filter(**filters) \
            .values(*values) \
            .annotate(aggregated_amount=Sum(self.obligation_column)) \
            .order_by('-aggregated_amount')

    def results(self) -> dict:
        # filter the transactions by category
        if self.category in ('awarding_agency', 'awarding_subagency'):
            results = self.awarding_agency()
        elif self.category in ('funding_agency', 'funding_subagency'):
            results = self.funding_agency()
        elif self.category in ('duns', 'parent_duns'):
            results = self.duns()
        elif self.category in ('cfda', 'psc', 'naics'):
            results = self.industry_and_other_codes()

        page_metadata = get_simple_pagination_metadata(len(results), self.limit, self.page)

        response = {
            'category': self.category,
            'limit': self.limit,
            'page_metadata': page_metadata,
            # alias_response is a workaround for tests instead of applying any aliases in the querysets
            'results': alias_response(ALIAS_DICT[self.category], results[:self.limit]),
        }
        return response

    def awarding_agency(self) -> list:
        if self.category == 'awarding_agency':
            filters = {'awarding_toptier_agency_name__isnull': False}
            values = ['awarding_toptier_agency_name', 'awarding_toptier_agency_abbreviation']
        elif self.category == 'awarding_subagency':
            filters = {'awarding_subtier_agency_name__isnull': False}
            values = ['awarding_subtier_agency_name', 'awarding_subtier_agency_abbreviation']

        self.queryset = self.common_db_query(filters, values)

        # DB hit here
        return list(self.queryset[self.lower_limit:self.upper_limit])

    def funding_agency(self) -> list:
        if self.subawards:
            self.raise_not_implemented()
        if self.category == 'funding_agency':
            filters = {'funding_toptier_agency_name__isnull': False}
            values = ['funding_toptier_agency_name', 'funding_toptier_agency_abbreviation']
        elif self.category == 'funding_subagency':
            filters = {'funding_subtier_agency_name__isnull': False}
            values = ['funding_subtier_agency_name', 'funding_subtier_agency_abbreviation']

        self.queryset = self.common_db_query(filters, values)

        # DB hit here
        return list(self.queryset[self.lower_limit:self.upper_limit])

    def duns(self) -> list:
        if self.category == 'duns':
            filters = {'recipient_unique_id__isnull': False}
            values = ['recipient_name', 'recipient_unique_id']

        elif self.category == 'parent_duns':
            # TODO: check if we can aggregate on recipient name and parent duns,
            #    since parent recipient name isn't available
            filters = {'parent_recipient_unique_id__isnull': False}
            values = ['recipient_name', 'parent_recipient_unique_id']

        self.queryset = self.common_db_query(filters, values)

        # DB hit here
        return list(self.queryset[self.lower_limit:self.upper_limit])

    def industry_and_other_codes(self) -> list:
        if self.category == 'cfda':
            filters = {'{}__isnull'.format(self.obligation_column): False, 'cfda_number__isnull': False}
            values = ['cfda_number', 'cfda_popular_name', 'cfda_title']
        elif self.category == 'psc':
            if self.subawards:
                self.raise_not_implemented()
            filters = {'product_or_service_code__isnull': False}
            values = ['product_or_service_code']

        elif self.category == 'naics':
            if self.subawards:
                # TODO: get subaward NAICS from Broker
                self.raise_not_implemented()
            filters = {'naics_code__isnull': False}
            values = ['naics_code', 'naics_description']

        self.queryset = self.common_db_query(filters, values)
        # DB hit here
        return list(self.queryset[self.lower_limit:self.upper_limit])
