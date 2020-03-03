from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.validator.tinyshield import TinyShield
from usaspending_api.references.v2.views.filter_trees.tas_filter_tree import TASFilterTree


class TASViewSet(APIView):
    """

    """

    endpoint_doc = ""

    def _parse_and_validate(self, request):

        data = {"depth": request.get("depth") or 1}
        models = [
            {"key": "depth", "name": "depth", "type": "integer", "allow_nulls": True, "default": 1, "optional": True}
        ]
        return TinyShield(models).block(data)

    @cache_response()
    def get(self, request: Request, tier1: str = None, tier2: str = None, tier3: str = None) -> Response:
        request_values = self._parse_and_validate(request.GET)

        filter_tree = TASFilterTree()
        return Response([elem.toJSON() for elem in filter_tree.search(tier1, tier2, tier3, request_values["depth"])])