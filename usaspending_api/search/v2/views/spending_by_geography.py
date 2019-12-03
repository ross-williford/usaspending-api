import logging
import copy

from collections import OrderedDict
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.common.cache_decorator import cache_response
from django.db.models import Sum, FloatField
from django.db.models.functions import Cast
from django.conf import settings

from usaspending_api.awards.models_matviews import SubawardView
from usaspending_api.awards.v2.filters.location_filter_geocode import geocode_filter_locations
from usaspending_api.awards.v2.filters.sub_award import subaward_filter
from usaspending_api.awards.v2.filters.view_selector import spending_by_geography
from usaspending_api.common.api_versioning import api_transformations, API_TRANSFORM_FUNCTIONS
from usaspending_api.common.elasticsearch.client import es_client_query
from usaspending_api.common.validator.award_filter import AWARD_FILTER
from usaspending_api.common.validator.pagination import PAGINATION
from usaspending_api.common.validator.tinyshield import TinyShield
from usaspending_api.references.abbreviations import code_to_state, fips_to_code, pad_codes
from usaspending_api.search.v2.elasticsearch_helper import elasticsearch_dollar_sum_aggregation, base_awards_query

logger = logging.getLogger(__name__)
API_VERSION = settings.API_VERSION


@api_transformations(api_version=API_VERSION, function_list=API_TRANSFORM_FUNCTIONS)
class SpendingByGeographyVisualizationViewSet(APIView):
    """
    This route takes award filters, and returns spending by state code, county code, or congressional district code.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/search/spending_by_geography.md"

    subawards = None
    scope = None
    filters = None
    scope_field_name = None
    loc_lookup = None
    geo_layer = None  # State, county or District
    geo_layer_filters = None  # Specific geo_layers to filter on
    queryset = None  # Transaction queryset
    geo_queryset = None  # Aggregate queryset based on scope

    @cache_response()
    def post(self, request):
        models = [
            {"name": "subawards", "key": "subawards", "type": "boolean", "default": False},
            {
                "name": "scope",
                "key": "scope",
                "type": "enum",
                "optional": False,
                "enum_values": ["place_of_performance", "recipient_location"],
            },
            {
                "name": "geo_layer",
                "key": "geo_layer",
                "type": "enum",
                "optional": False,
                "enum_values": ["state", "county", "district"],
            },
            {
                "name": "geo_layer_filters",
                "key": "geo_layer_filters",
                "type": "array",
                "array_type": "text",
                "text_type": "search",
            },
        ]
        models.extend(copy.deepcopy(AWARD_FILTER))
        models.extend(copy.deepcopy(PAGINATION))
        json_request = TinyShield(models).block(request.data)

        self.subawards = json_request["subawards"]
        self.scope = json_request["scope"]
        self.filters = json_request.get("filters", None)
        self.geo_layer = json_request["geo_layer"]
        self.geo_layer_filters = json_request.get("geo_layer_filters", None)

        fields_list = []  # fields to include in the aggregate query

        loc_dict = {"state": "state_code", "county": "county_code", "district": "congressional_code"}

        model_dict = {
            "place_of_performance": "pop",
            "recipient_location": "recipient_location",
            # 'subawards_place_of_performance': 'pop',
            # 'subawards_recipient_location': 'recipient_location'
        }

        # Build the query based on the scope fields and geo_layers
        # Fields not in the reference objects above then request is invalid

        self.scope_field_name = model_dict.get(self.scope)
        loc_field_name = loc_dict.get(self.geo_layer)
        self.loc_lookup = "{}_{}".format(self.scope_field_name, loc_field_name)

        if self.filters.get("elasticsearch") and not self.subawards:
            return Response(
                OrderedDict(
                    [("geo_layer", self.geo_layer), ("scope", self.scope), ("results", self.query_elasticsearch())]
                )
            )
        elif self.subawards:
            # We do not use matviews for Subaward filtering, just the Subaward download filters
            self.queryset = subaward_filter(self.filters)
            self.model_name = SubawardView
        else:
            self.queryset, self.model_name = spending_by_geography(self.filters)

        if self.geo_layer == "state":
            # State will have one field (state_code) containing letter A-Z
            column_isnull = "generated_pragmatic_obligation__isnull"
            if self.subawards:
                column_isnull = "amount__isnull"
            kwargs = {"{}_country_code".format(self.scope_field_name): "USA", column_isnull: False}

            # Only state scope will add its own state code
            # State codes are consistent in database i.e. AL, AK
            fields_list.append(self.loc_lookup)

            state_response = {
                "scope": self.scope,
                "geo_layer": self.geo_layer,
                "results": self.state_results(kwargs, fields_list),
            }

            return Response(state_response)

        else:
            # County and district scope will need to select multiple fields
            # State code is needed for county/district aggregation
            state_lookup = "{}_{}".format(self.scope_field_name, loc_dict["state"])
            fields_list.append(state_lookup)

            # Adding regex to county/district codes to remove entries with letters since can't be surfaced by map
            kwargs = {"{}__isnull".format("amount" if self.subawards else "generated_pragmatic_obligation"): False}

            if self.geo_layer == "county":
                # County name added to aggregation since consistent in db
                county_name_lookup = "{}_county_name".format(self.scope_field_name)
                fields_list.append(county_name_lookup)
                self.county_district_queryset(kwargs, fields_list, state_lookup)

                county_response = {
                    "scope": self.scope,
                    "geo_layer": self.geo_layer,
                    "results": self.county_results(state_lookup, county_name_lookup),
                }

                return Response(county_response)
            else:
                self.county_district_queryset(kwargs, fields_list, state_lookup)

                district_response = {
                    "scope": self.scope,
                    "geo_layer": self.geo_layer,
                    "results": self.district_results(state_lookup),
                }

                return Response(district_response)

    def state_results(self, filter_args, lookup_fields):
        # Adding additional state filters if specified
        if self.geo_layer_filters:
            self.queryset = self.queryset.filter(**{"{}__{}".format(self.loc_lookup, "in"): self.geo_layer_filters})
        else:
            # Adding null filter for state for specific partial index
            # when not using geocode_filter
            filter_args["{}__isnull".format(self.loc_lookup)] = False

        self.geo_queryset = self.queryset.filter(**filter_args).values(*lookup_fields)

        if self.subawards:
            self.geo_queryset = self.geo_queryset.annotate(transaction_amount=Sum("amount"))
        else:
            self.geo_queryset = self.geo_queryset.annotate(
                transaction_amount=Sum("generated_pragmatic_obligation")
            ).values("transaction_amount", *lookup_fields)
        # State names are inconsistent in database (upper, lower, null)
        # Used lookup instead to be consistent
        results = [
            {
                "shape_code": x[self.loc_lookup],
                "aggregated_amount": x["transaction_amount"],
                "display_name": code_to_state.get(x[self.loc_lookup], {"name": "None"}).get("name").title(),
            }
            for x in self.geo_queryset
        ]

        return results

    def county_district_queryset(self, kwargs, fields_list, state_lookup):
        # Filtering queryset to specific county/districts if requested
        # Since geo_layer_filters comes as concat of state fips and county/district codes
        # need to split for the geocode_filter
        if self.geo_layer_filters:
            geo_layers_list = [
                {"state": fips_to_code.get(x[:2]), self.geo_layer: x[2:], "country": "USA"}
                for x in self.geo_layer_filters
            ]
            self.queryset = self.queryset.filter(geocode_filter_locations(self.scope_field_name, geo_layers_list, True))
        else:
            # Adding null, USA, not number filters for specific partial index when not using geocode_filter
            kwargs["{}__{}".format(self.loc_lookup, "isnull")] = False
            kwargs["{}__{}".format(state_lookup, "isnull")] = False
            kwargs["{}_country_code".format(self.scope_field_name)] = "USA"
            kwargs["{}__{}".format(self.loc_lookup, "iregex")] = r"^[0-9]*(\.\d+)?$"

        # Turn county/district codes into float since inconsistent in database
        # Codes in location table ex: '01', '1', '1.0'
        # Cast will group codes as a float and will combine inconsistent codes
        self.geo_queryset = (
            self.queryset.filter(**kwargs)
            .values(*fields_list)
            .annotate(code_as_float=Cast(self.loc_lookup, FloatField()))
        )

        if self.subawards:
            self.geo_queryset = self.geo_queryset.annotate(transaction_amount=Sum("amount"))
        else:
            self.geo_queryset = self.geo_queryset.annotate(
                transaction_amount=Sum("generated_pragmatic_obligation")
            ).values("transaction_amount", "code_as_float", *fields_list)

        return self.geo_queryset

    def county_results(self, state_lookup, county_name):
        # Returns county results formatted for map
        results = [
            {
                "shape_code": code_to_state.get(x[state_lookup])["fips"]
                + pad_codes(self.geo_layer, x["code_as_float"]),
                "aggregated_amount": x["transaction_amount"],
                "display_name": x[county_name].title() if x[county_name] is not None else x[county_name],
            }
            for x in self.geo_queryset
        ]

        return results

    def district_results(self, state_lookup):
        # Returns congressional district results formatted for map
        results = [
            {
                "shape_code": code_to_state.get(x[state_lookup])["fips"]
                + pad_codes(self.geo_layer, x["code_as_float"]),
                "aggregated_amount": x["transaction_amount"],
                "display_name": x[state_lookup] + "-" + pad_codes(self.geo_layer, x["code_as_float"]),
            }
            for x in self.geo_queryset
        ]

        return results

    def create_elasticsearch_aggregation(self) -> dict:
        all_aggs = {
            "aggs": {
                "filter_by_usa": {
                    "filter": {"term": {f"{self.scope_field_name}_country_code.keyword": "USA"}},
                    "aggs": {
                        "group_by_state_code": {
                            "terms": {
                                "field": f"{self.scope_field_name}_state_code.keyword",
                                "order": {"_key": "asc"},
                                "size": 5000,
                            }
                        }
                    },
                }
            }
        }

        if self.geo_layer == "state":

            agg_sum = elasticsearch_dollar_sum_aggregation("generated_pragmatic_obligation")
            all_aggs["aggs"]["filter_by_usa"]["aggs"]["group_by_state_code"]["aggs"] = agg_sum

        elif self.geo_layer == "county":
            # County is tricky since it is possible to have multiple counties with the same county code
            # but different county names. As a result, multiple sums and a max aggregation are needed.
            sum_by_county_as_cents_agg = {
                "sum_by_county_as_cents": {
                    "sum": {
                        "script": {"lang": "painless", "source": "doc['generated_pragmatic_obligation'].value * 100"}
                    }
                }
            }
            sum_all_counties_as_cents_agg = {
                "sum_all_counties_as_cents": {
                    "sum_bucket": {"buckets_path": "group_by_county_name>sum_by_county_as_cents"}
                }
            }
            sum_all_counties_as_dollars_agg = {
                "sum_all_counties_as_dollars": {
                    "bucket_script": {
                        "buckets_path": {"sum_as_cents": "sum_all_counties_as_cents"},
                        "script": "params.sum_as_cents / 100",
                    }
                }
            }
            max_sum_county_name_agg = {
                "max_sum_county_name": {"max_bucket": {"buckets_path": "group_by_county_name>sum_by_county_as_cents"}}
            }
            county_group_by_agg = {
                "aggs": {
                    "group_by_county_code": {
                        "terms": {"field": f"{self.loc_lookup}.keyword", "order": {"_key": "asc"}, "size": "2000"},
                        "aggs": {
                            "group_by_county_name": {
                                "terms": {"field": f"{self.scope_field_name}_county_name.keyword"},
                                "aggs": sum_by_county_as_cents_agg,
                            },
                            **sum_all_counties_as_cents_agg,
                            **sum_all_counties_as_dollars_agg,
                            **max_sum_county_name_agg,
                        },
                    }
                }
            }
            all_aggs["aggs"]["filter_by_usa"]["aggs"][f"group_by_state_code"].update(county_group_by_agg)

        elif self.geo_layer == "district":

            district_group_by_aggs = {
                "aggs": {
                    "group_by_congressional_code": {
                        "terms": {"field": f"{self.loc_lookup}.keyword", "order": {"_key": "asc"}, "size": "2000"},
                        "aggs": elasticsearch_dollar_sum_aggregation("generated_pragmatic_obligation"),
                    }
                }
            }
            all_aggs["aggs"]["filter_by_usa"]["aggs"][f"group_by_state_code"].update(district_group_by_aggs)

        return all_aggs

    def parse_elasticsearch_response(self, hits: dict) -> list:
        results = []

        for state_bucket in hits["aggregations"]["filter_by_usa"]["group_by_state_code"]["buckets"]:
            state_code = state_bucket["key"]
            state_fips = code_to_state.get(state_code, {"fips": "None"}).get("fips")

            if self.geo_layer == "state":
                results.append(
                    OrderedDict(
                        [
                            ("aggregated_amount", state_bucket["sum_as_dollars"]["value"]),
                            ("display_name", code_to_state.get(state_code, {"name": "None"}).get("name").title()),
                            ("shape_code", state_code),
                        ]
                    )
                )

            elif self.geo_layer == "county":
                for county_bucket in state_bucket["group_by_county_code"]["buckets"]:
                    county_code = county_bucket["key"]
                    display_name = (
                        county_bucket["max_sum_county_name"]["keys"][0].title()
                        if len(county_bucket["max_sum_county_name"]["keys"]) > 0
                        else None
                    )
                    results.append(
                        OrderedDict(
                            [
                                ("aggregated_amount", county_bucket["sum_all_counties_as_dollars"]["value"]),
                                ("display_name", display_name),
                                ("shape_code", state_fips + county_code),
                            ]
                        )
                    )

            elif self.geo_layer == "district":
                for congressional_bucket in state_bucket["group_by_congressional_code"]["buckets"]:
                    congressional_code = congressional_bucket["key"]
                    results.append(
                        OrderedDict(
                            [
                                ("aggregated_amount", congressional_bucket["sum_as_dollars"]["value"]),
                                ("display_name", f"{state_code}-{congressional_code}"),
                                ("shape_code", state_fips + congressional_code),
                            ]
                        )
                    )

        return results

    def combine_geo_layer_filters_with_search_filters(self) -> None:
        search_filter_key = None

        if self.scope == "place_of_performance":
            search_filter_key = "place_of_performance_locations"
        elif self.scope == "recipient_location":
            search_filter_key = "recipient_locations"

        if self.filters.get(search_filter_key) is None:
            self.filters[search_filter_key] = []

        for geo_layer_filter in self.geo_layer_filters:
            if self.geo_layer == "state":
                self.filters[search_filter_key].append({"country": "USA", "state": geo_layer_filter})
            elif self.geo_layer == "county" or self.geo_layer == "district":
                self.filters[search_filter_key].append(
                    {
                        "country": "USA",
                        "state": fips_to_code.get(geo_layer_filter[:2]),
                        self.geo_layer: geo_layer_filter[2:],
                    }
                )

    def query_elasticsearch(self) -> dict:
        if self.geo_layer_filters:
            self.combine_geo_layer_filters_with_search_filters()

        query = {
            "query": base_awards_query(self.filters, is_for_transactions=True),
            **self.create_elasticsearch_aggregation(),
            "size": 0,
        }

        hits = es_client_query(index="*future-{}".format(settings.ES_TRANSACTIONS_NAME_SUFFIX), body=query)

        results = []
        if hits and hits["hits"]["total"] > 0:
            results = self.parse_elasticsearch_response(hits)
        return results
