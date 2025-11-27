from fastapi import APIRouter, status
from fastapi.responses import JSONResponse

from core.stats import topic_stats

router = APIRouter()

@router.get("/report", status_code=status.HTTP_200_OK)
async def get_list():
    """This API returns the list of simulation/ai requests created by the users.
    In addition, for each request, it is also provided the current state of progress.

    Returns:
        JSONResponse: object containing the headers and the body of the response.
    """

    content = topic_stats.get_report()
    headers = {"Access-Control-Allow-Origin": "*"}
    return JSONResponse(content=content, headers=headers)

@router.delete("/report", status_code=status.HTTP_200_OK)
async def clear_report_data():
    """
    Deletes all statistics from the report database.
    """
    deleted_count = topic_stats.clear_report()
    return {"message": "Report data cleared successfully.", "deleted_records": deleted_count}
