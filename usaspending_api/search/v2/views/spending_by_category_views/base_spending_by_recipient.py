import itertools
from abc import ABCMeta

from django.db.models import QuerySet, Value, When, Case, IntegerField
from elasticsearch_dsl import A, Q as ES_Q
from typing import List
from usaspending_api.common.elasticsearch.search_wrappers import TransactionSearch
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.helpers.api_helper import alias_response
from usaspending_api.common.recipient_lookups import combine_recipient_hash_and_level
from usaspending_api.recipient.models import RecipientLookup, RecipientProfile
from usaspending_api.recipient.v2.lookups import SPECIAL_CASES
from usaspending_api.search.v2.elasticsearch_helper import get_sum_aggregations
from usaspending_api.search.v2.views.spending_by_category_views.base_spending_by_category import (
    BaseSpendingByCategoryViewSet,
)


class BaseRecipientViewSet(BaseSpendingByCategoryViewSet, metaclass=ABCMeta):
    """
    Base class used by the different Recipients
    """

    def build_elasticsearch_search_with_aggregations(
        self, filter_query: ES_Q, curr_partition: int, num_partitions: int, size: int
    ) -> TransactionSearch:
        # Create filtered Search object
        search = TransactionSearch().filter(filter_query).extra(size=0)

        # Define all aggregations needed to build the response
        group_by_recipient_hash = A(
            "terms",
            field=self.category.primary_field,
            include={"partition": curr_partition, "num_partitions": num_partitions},
            size=size,
        )

        sum_aggregations = get_sum_aggregations("generated_pragmatic_obligation", self.pagination)
        sum_as_cents = sum_aggregations["sum_as_cents"]
        sum_as_dollars = sum_aggregations["sum_as_dollars"]
        sum_bucket_sort = sum_aggregations["sum_bucket_sort"]

        # Apply the aggregations to TransactionSearch object
        search.aggs.bucket("group_by_recipient_hash", group_by_recipient_hash)
        search.aggs["group_by_recipient_hash"].metric("sum_as_cents", sum_as_cents).pipeline(
            "sum_as_dollars", sum_as_dollars
        ).pipeline("sum_bucket_sort", sum_bucket_sort)

        return search

    def query_elasticsearch(self, filter_query: ES_Q) -> list:
        results = self.elasticsearch_results_generator(filter_query, size=100000000)
        flat_results = list(itertools.chain.from_iterable(results))
        return flat_results

    def build_elasticsearch_result(self, response: dict) -> List[dict]:
        results = []
        recipient_hash_buckets = response.get("group_by_recipient_hash", {}).get("buckets", [])
        for bucket in recipient_hash_buckets:
            recipient_hash = bucket.get("key")
            recipient_name, duns = self._get_recipient_name_and_duns(recipient_hash)
            results.append(
                {
                    "amount": bucket.get("sum_as_dollars", {"value": 0})["value"],
                    "recipient_id": self._get_recipient_id({"recipient_hash": recipient_hash}),
                    "name": recipient_name,
                    "code": duns,
                }
            )

        return results

    def query_django(self, base_queryset: QuerySet):

        if self.category.name == "recipient_duns":
            if self.subawards:
                values = ["recipient_name", "recipient_unique_id"]
            else:
                values = ["recipient_hash"]

        elif self.category.name == "recipient_parent_duns":
            # TODO: check if we can aggregate on recipient name and parent duns,
            #    since parent recipient name isn't available
            # Not implemented until "Parent Recipient Name" is in matviews
            raise InvalidParameterException(f"Category '{self.category.name}' is not yet implemented")
            # filters = {'parent_recipient_unique_id__isnull': False}
            # values = ['recipient_name', 'parent_recipient_unique_id']

        else:
            raise InvalidParameterException(f"Invalid category '{self.category.name}'")

        queryset = self.common_db_query(base_queryset, {}, values)
        query_results = list(queryset[self.pagination.lower_limit : self.pagination.upper_limit])
        for row in query_results:
            row["recipient_id"] = self._get_recipient_id(row)
            if not self.subawards:
                recipient_hash = row.pop("recipient_hash")
                row["recipient_name"], row["recipient_unique_id"] = self._get_recipient_name_and_duns(recipient_hash)

        return alias_response({"recipient_name": "name", "recipient_unique_id": "code"}, query_results)

    @staticmethod
    def _get_recipient_name_and_duns(recipient_hash):
        recipient_name_and_duns = (
            RecipientLookup.objects.filter(recipient_hash=recipient_hash).values("legal_business_name", "duns").first()
        )

        # The Recipient Name + DUNS should always be retrievable in RecipientLookup
        # For odd edge cases or data sync issues, handle gracefully:
        if recipient_name_and_duns is None:
            return None, "DUNS Number not provided"

        return recipient_name_and_duns["legal_business_name"], recipient_name_and_duns["duns"]

    @staticmethod
    def _get_recipient_id(row):
        """
        In the recipient_profile table there is a 1 to 1 relationship between hashes and DUNS
        (recipient_unique_id) and the hashes+duns match exactly between recipient_profile and
        recipient_lookup where there are matches.  Grab the level from recipient_profile by
        hash if we have one or by DUNS if we have one of those.
        """
        if "recipient_hash" in row:
            profile_filter = {"recipient_hash": row["recipient_hash"]}
        elif "recipient_unique_id" in row:
            profile_filter = {"recipient_unique_id": row["recipient_unique_id"]}
        else:
            raise RuntimeError(
                "Attempted to lookup recipient profile using a queryset that contains neither "
                "'recipient_hash' nor 'recipient_unique_id'"
            )

        profile = (
            RecipientProfile.objects.filter(**profile_filter)
            .exclude(recipient_name__in=SPECIAL_CASES)
            .annotate(
                sort_order=Case(
                    When(recipient_level="C", then=Value(0)),
                    When(recipient_level="R", then=Value(1)),
                    default=Value(2),
                    output_field=IntegerField(),
                )
            )
            .values("recipient_hash", "recipient_level")
            .order_by("sort_order")
            .first()
        )

        return (
            combine_recipient_hash_and_level(profile["recipient_hash"], profile["recipient_level"]) if profile else None
        )
