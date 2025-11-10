from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse

from core.stats import topic_stats

router = APIRouter()

@router.get("/report")
async def get_list():
    """This API returns the list of simulation/ai requests created by the users.
    In addition, for each request, it is also provided the current state of progress.

    Returns:
        JSONResponse: object containing the headers and the body of the response.
    """

    content = topic_stats.get_report()
    headers = {"Access-Control-Allow-Origin": "*"}
    return JSONResponse(content=content, headers=headers)
