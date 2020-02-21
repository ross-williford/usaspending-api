import json

from rest_framework import status

from usaspending_api.common.experimental_api_flags import ELASTICSEARCH_HEADER_VALUE, EXPERIMENTAL_API_HEADER
from usaspending_api.search.tests.data.search_filters_test_data import non_legacy_filters
from usaspending_api.search.tests.integration.spending_by_category.utilities import setup_elasticsearch_test


def test_success_with_all_filters(client, monkeypatch, elasticsearch_transaction_index, naics_award):
    """
    General test to make sure that all groups respond with a Status Code of 200 regardless of the filters.
    """

    logging_statements = []
    setup_elasticsearch_test(monkeypatch, elasticsearch_transaction_index, logging_statements)

    resp = client.post(
        "/api/v2/search/spending_by_category/naics",
        content_type="application/json",
        data=json.dumps({"filters": non_legacy_filters()}),
        **{EXPERIMENTAL_API_HEADER: ELASTICSEARCH_HEADER_VALUE},
    )
    assert resp.status_code == status.HTTP_200_OK, "Failed to return 200 Response"
    assert len(logging_statements) == 1, "Expected one logging statement"
