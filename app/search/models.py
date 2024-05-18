import pydantic


class Result(pydantic.BaseModel):
    sale_date: str
    address: str
    county: str
    eircode: str | None
    price: float
    is_full_market_price: bool
    vat_exclusive: bool
    property_size_description: str | None
    is_second_hand: bool


class SearchResult(pydantic.BaseModel):
    total_num_documents: int
    has_next_page: bool
    next_page_num: int | None
    num_documents_returned: int
    results: list[Result]
