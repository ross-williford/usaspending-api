from abc import ABCMeta
from typing import List

from django.db.models import QuerySet
from elasticsearch_dsl import A, Q as ES_Q
from usaspending_api.common.helpers.api_helper import alias_response
from usaspending_api.common.elasticsearch.search_wrappers import TransactionSearch
from usaspending_api.search.helpers.spending_by_category_helpers import fetch_naics_description_from_code
from usaspending_api.search.v2.elasticsearch_helper import get_sum_aggregations
from usaspending_api.search.v2.views.spending_by_category_views.base_spending_by_category import (
    BaseSpendingByCategoryViewSet,
)
from usaspending_api.search.v2.views.spending_by_category_views.base_spending_by_category import Category


ALIAS_DICT = {"naics_code": "code", "naics_description": "name"}


class NaicsView(BaseSpendingByCategoryViewSet, metaclass=ABCMeta):

    category = Category(name="naics", primary_field="naics_code")

    def build_elasticsearch_search_with_aggregations(
        self, filter_query: ES_Q, curr_partition: int, num_partitions: int, size: int
    ) -> TransactionSearch:
        # Create filtered Search object
        search = TransactionSearch().filter(filter_query)

        # Define all aggregations needed to build the response
        group_by_agency_name = A(
            "terms",
            field=f"{self.agency_type.value}_agency_name.keyword",
            include={"partition": curr_partition, "num_partitions": num_partitions},
            size=size,
        )
        group_by_agency_abbreviation = A("terms", field=f"{self.agency_type.value}_agency_abbreviation.keyword")
        group_by_agency_id = A("terms", field=f"{self.agency_type.value}_agency_id")

        sum_aggregations = get_sum_aggregations("generated_pragmatic_obligation", self.pagination)
        sum_as_cents = sum_aggregations["sum_as_cents"]
        sum_as_dollars = sum_aggregations["sum_as_dollars"]
        sum_bucket_sort = sum_aggregations["sum_bucket_sort"]

        # Apply the aggregations to TransactionSearch object
        search.aggs.bucket("group_by_agency_name", group_by_agency_name)
        search.aggs["group_by_agency_name"].bucket("group_by_agency_abbreviation", group_by_agency_abbreviation)
        search.aggs["group_by_agency_name"].bucket("group_by_agency_id", group_by_agency_id)
        search.aggs["group_by_agency_name"].metric("sum_as_cents", sum_as_cents).pipeline(
            "sum_as_dollars", sum_as_dollars
        ).pipeline("sum_bucket_sort", sum_bucket_sort)

        return search

    def build_elasticsearch_result(self, response: dict) -> List[dict]:
        results = []
        agency_name_buckets = response.get("group_by_agency_name", {}).get("buckets", [])
        for bucket in agency_name_buckets:
            agency_abbreviation_buckets = bucket.get("group_by_agency_abbreviation", {}).get("buckets", [])
            agency_id_buckets = bucket.get("group_by_agency_id", {}).get("buckets", [])
            results.append(
                {
                    "amount": bucket.get("sum_as_dollars", {"value": 0})["value"],
                    "name": bucket.get("key"),
                    "code": agency_abbreviation_buckets[0].get("key") if len(agency_abbreviation_buckets) > 0 else None,
                    "id": int(agency_id_buckets[0].get("key")) if len(agency_id_buckets) > 0 else None,
                }
            )
        return results

    def query_django(self, base_queryset: QuerySet):
        if self.subawards:
            # TODO: get subaward NAICS from Broker
            self.raise_not_implemented()
        django_filters = {"naics_code__isnull": False}
        django_values = ["naics_code", "naics_description"]

        queryset = self.common_db_query(base_queryset, django_filters, django_values)

        lower_limit = self.pagination.lower_limit
        upper_limit = self.pagination.upper_limit
        query_results = list(queryset[lower_limit:upper_limit])
        results = alias_response(ALIAS_DICT, query_results)
        for row in results:
            row["id"] = None
            row["name"] = fetch_naics_description_from_code(row["code"], row["name"])
        return query_results
