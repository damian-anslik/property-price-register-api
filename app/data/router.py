import fastapi

import datetime
import logging

import app.data.handlers

data_router = fastapi.APIRouter()
logger = logging.getLogger(__name__)


@data_router.get("/download", include_in_schema=False)
async def download(date: str = None):
    try:
        if date is not None:
            date = datetime.datetime.strptime(date, "%Y-%m-%d")
        else:
            date = datetime.datetime.today()
        response = app.data.handlers.download_handler(date)
        logger.info(f"Downloaded data - {response}")
        return response
    except Exception as e:
        raise fastapi.HTTPException(status_code=500, detail=str(e))
