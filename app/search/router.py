import fastapi

import logging

import app.search.handlers
import app.search.models

search_router = fastapi.APIRouter()
logger = logging.getLogger(__name__)


@search_router.get("/properties", response_model=app.search.models.SearchResult)
async def search_properties(
    address: str = None,
    county: str = None,
    start_date: str = None,
    end_date: str = None,
    min_price: float = None,
    max_price: float = None,
    is_second_hand: bool = None,
    is_descending: bool = True,
    page_num: int = 1,
):
    try:
        function_args = locals()
        results = await app.search.handlers.search_properties_handler(**function_args)
        return results
    except ValueError as e:
        logger.error(f"{str(e)} returned for input data: {function_args}")
        raise fastapi.HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception(str(e))
        raise fastapi.HTTPException(
            status_code=500, detail="Something went wrong! Please try again."
        )
