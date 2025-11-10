from fastapi import FastAPI
import uvicorn
from api import stats
from core.stats import topic_stats
from core.settings import settings
import logging

# Ensure logging is configured
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title=settings.app_name,
    description=settings.app_description,
    version=settings.version
)

@app.on_event("startup")
async def startup_event():
    logger.info("=== STARTUP EVENT CALLED ===")
    topic_stats.start()
    logger.info("=== STARTUP EVENT COMPLETE ===")

@app.on_event("shutdown")
async def shutdown_event():
    logger.info("=== SHUTDOWN EVENT CALLED ===")
    topic_stats.stop()
    logger.info("=== SHUTDOWN EVENT COMPLETE ===")

# The requests that are used to retrieve the messages statistics are handled by the stats router.
app.include_router(stats.router,
    tags=["Get Report"],
    responses={404: settings.response_404}
)

if __name__ == "__main__":
    """Main thread of the backend that has to guarantee the execution
    of the REST requests sent by the client applications.
    Meanwhile, it has to compute the statistics of the messages that comes from Kafka.
    These updates are in charge of a parallel daemon thread.
    """
    logger.info("=== STARTING UVICORN ===")
    
    uvicorn.run(
        app,
        host="0.0.0.0", 
        port=settings.port,
        log_level="info"
    )