from copy import deepcopy

from django.db.models import F
from rest_framework.request import Request
from rest_framework.response import Response

from usaspending_api.awards.models import TransactionNormalized
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.helpers.generic_helper import get_simple_pagination_metadata
from usaspending_api.common.views import APIDocumentationView
from usaspending_api.core.validator.pagination import PAGINATION
from usaspending_api.core.validator.tinyshield import TinyShield


class TransactionViewSet(APIDocumentationView):
    """
    endpoint_doc: /awards/transactions.md
    """
    transaction_lookup = {
        # "Display Name": "database_column"
        "id": "transaction_unique_id",
        "type": "type",
        "type_description": "type_description",
        "action_date": "action_date",
        "action_type": "action_type",
        "action_type_description": "action_type_description",
        "modification_number": "modification_number",
        "description": "description",
        "federal_action_obligation": "federal_action_obligation",
        "face_value_loan_guarantee": "face_value_loan_guarantee",
        "original_loan_subsidy_cost": "original_loan_subsidy_cost",
        # necessary columns which are only present for Django Mock Queries
        "award_id": "award_id",
        "is_fpds": "is_fpds",
    }

    def _parse_and_validate_request(self, request_dict: dict) -> dict:
        models = deepcopy(PAGINATION)
        models.append({"key": "award_id", "name": "award_id", "type": "text", "text_type": "search", "optional": False})
        for model in models:
            # Change sort to an enum of the desired values
            if model["name"] == "sort":
                model["type"] = "enum"
                model["enum_values"] = list(self.transaction_lookup.keys())
                model["default"] = "action_date"

        validated_request_data = TinyShield(models).block(request_dict)
        return validated_request_data

    def _business_logic(self, request_data: dict) -> list:
        lower_limit = (request_data["page"] - 1) * request_data["limit"]
        upper_limit = request_data["page"] * request_data["limit"]

        if request_data["award_id"].isdigit():
            # Award ID
            filter = {'award_id': request_data["award_id"]}
        else:
            # Generated Award ID
            filter = {'award__generated_unique_award_id': request_data["award_id"]}

        queryset = (TransactionNormalized.objects.all()
                    .values(*list(self.transaction_lookup.values()))
                    .filter(**filter))

        if request_data["order"] == "desc":
            queryset = queryset.order_by(F(request_data["sort"]).desc(nulls_last=True))
        else:
            queryset = queryset.order_by(F(request_data["sort"]).asc(nulls_first=True))

        rows = list(queryset[lower_limit:upper_limit + 1])
        return self._format_results(rows)

    def _format_results(self, rows):
        results = []
        for row in rows:
            unique_prefix = 'ASST_TX'
            result = {k: row[v] for k, v in self.transaction_lookup.items() if k != "award_id"}
            if result['is_fpds']:
                unique_prefix = 'CONT_TX'
            result['id'] = '{}_{}'.format(unique_prefix, result['id'])
            del result['is_fpds']
            results.append(result)
        return results

    @cache_response()
    def post(self, request: Request) -> Response:
        request_data = self._parse_and_validate_request(request.data)
        results = self._business_logic(request_data)
        page_metadata = get_simple_pagination_metadata(len(results), request_data["limit"], request_data["page"])

        response = {
            "page_metadata": page_metadata,
            "results": results[:request_data["limit"]],
        }

        return Response(response)
