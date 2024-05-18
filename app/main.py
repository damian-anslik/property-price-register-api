import fastapi

import logging

from app.data.router import data_router
from app.search.router import search_router

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s - %(name)s - %(asctime)s - %(message)s",
)
app = fastapi.FastAPI()
app.include_router(search_router, prefix="/public")
app.include_router(data_router, prefix="/private")
