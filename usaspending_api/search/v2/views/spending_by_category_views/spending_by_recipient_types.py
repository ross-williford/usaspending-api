from usaspending_api.search.v2.views.spending_by_category_views.base_spending_by_category import Category
from usaspending_api.search.v2.views.spending_by_category_views.base_spending_by_recipient import BaseRecipientViewSet


class ParentRecipientViewSet(BaseRecipientViewSet):
    """
    This route takes award filters, and returns spending by parent recipient.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/search/spending_by_category/parent_recipient.md"
    category = Category(name="recipient_parent_duns", primary_field="parent_recipient_unique_id.keyword")


class RecipientViewSet(BaseRecipientViewSet):
    """
    This route takes award filters, and returns spending by recipient.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/search/spending_by_category/recipient.md"
    category = Category(name="recipient_duns", primary_field="recipient_hash")
