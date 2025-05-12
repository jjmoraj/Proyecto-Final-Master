import os
import pickle
import logging
from datetime import timedelta
from functools import lru_cache

import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List

# --- Configure logging ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Response model definition ---
class ForecastItem(BaseModel):
    ds: str  # Forecast date in YYYY-MM-DD format
    yhat: float  # Predicted value for the date

# --- FastAPI application instance ---
app = FastAPI(
    title="Holt–Winters Forecast API",
    description="Provides a 7-day forecast based on a pre-trained Holt-Winters model.",
    version="1.0.0"
)

@lru_cache(maxsize=1)
def load_artifact() -> dict:
    """
    Load the Holt-Winters model artifact from disk.
    Uses LRU cache to ensure the artifact is loaded only once per process.

    Returns:
        A dictionary with keys 'model' (the trained model) and 'last_date' (string or pd.Timestamp).

    Raises:
        FileNotFoundError: If the artifact file does not exist.
        pickle.UnpicklingError: If loading fails due to corruption or incompatible format.
    """
    artifact_path = os.getenv(
        "MODEL_PATH",
        "/app/model/holt_winters_artifact.pkl"
    )
    if not os.path.isfile(artifact_path):
        logger.error("Model artifact not found at %s", artifact_path)
        raise FileNotFoundError(f"Model artifact not found at {artifact_path}")

    with open(artifact_path, "rb") as f:
        artifact = pickle.load(f)
    logger.info("Model artifact loaded from %s", artifact_path)
    return artifact

@app.on_event("startup")
def on_startup():
    """
    Application startup event: pre-load the model and last date into memory.
    Stores them in the global state of the FastAPI app.
    """
    try:
        artifact = load_artifact()
        # Ensure last_date is a pandas Timestamp for consistent date arithmetic
        last_date = artifact.get("last_date")
        if not isinstance(last_date, pd.Timestamp):
            last_date = pd.to_datetime(last_date)
        app.state.model = artifact.get("model")
        app.state.last_date = last_date
        logger.info("Model and last_date stored in application state.")
    except Exception as e:
        logger.exception("Failed to load model artifact on startup: %s", e)
        # In a real deployment, you might stop the application or retry

@app.get(
    "/forecast",
    response_model=List[ForecastItem],
    summary="Get 7-day forecast",
    description="Returns a list of date/value pairs for the next 7 days based on the Holt-Winters model."
)
def get_forecast() -> List[ForecastItem]:
    """
    Generate and return a 7-day forecast using the pre-loaded Holt-Winters model.

    Returns:
        A list of ForecastItem objects with fields 'ds' and 'yhat'.

    Raises:
        HTTPException: If the model is not loaded in application state.
    """
    # Validate that model and last_date are available
    model = getattr(app.state, "model", None)
    last_date = getattr(app.state, "last_date", None)
    if model is None or last_date is None:
        logger.error("Model or last_date not found in application state.")
        raise HTTPException(
            status_code=503,
            detail="Model not available. Please try again later."
        )

    # Compute the date index for the next 7 days
    start_date = last_date + timedelta(days=1)
    future_idx = pd.date_range(start=start_date, periods=7, freq="D")

    # Perform forecasting
    try:
        forecast_values = model.forecast(7)
    except Exception as e:
        logger.exception("Error during model forecasting: %s", e)
        raise HTTPException(
            status_code=500,
            detail="Error generating forecast."
        )

    # Package results into Pydantic models
    forecast_list = []
    for dt, yhat in zip(future_idx, forecast_values.values):
        forecast_list.append(
            ForecastItem(ds=dt.strftime("%Y-%m-%d"), yhat=float(yhat))
        )

    return forecast_list

# --- Entry point for local development ---
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "api:app", host="0.0.0.0", port=int(os.getenv("PORT", 8000)), reload=True
    )