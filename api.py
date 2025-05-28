"""
FastAPI application for Galaxy Digital MongoDB Sync

This API provides REST endpoints to interact with the Galaxy Digital sync functionality,
allowing external programs to trigger syncs, generate reports, and query data.
"""

from fastapi import FastAPI, HTTPException, BackgroundTasks, Query
from fastapi.responses import JSONResponse, HTMLResponse
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime, date
import os
from enum import Enum
from loguru import logger

# Import the sync tool
from galaxy_api_sync import GalaxyAPISync

# Create FastAPI app
app = FastAPI(
    title="Galaxy Digital MongoDB Sync API",
    description="REST API for synchronizing Galaxy Digital data with MongoDB and generating reports",
    version="1.0.0"
)

# Global sync tool instance
sync_tool = None

# Enum for report types
class ReportType(str, Enum):
    user = "user"
    needs = "needs"
    opportunity = "opportunity"
    agency = "agency"
    time = "time"
    shift_status = "shift_status"
    checkin_analysis = "checkin_analysis"

# Pydantic models for request/response
class SyncResponse(BaseModel):
    status: str
    message: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)

class ReportRequest(BaseModel):
    report_type: ReportType
    sync_first: bool = True

class ResourceSyncRequest(BaseModel):
    resource_name: str
    params: Optional[Dict[str, Any]] = None
    since_field: Optional[str] = None

class QueryRequest(BaseModel):
    collection: str
    filter: Dict[str, Any] = {}
    projection: Optional[Dict[str, Any]] = None
    limit: int = 100
    skip: int = 0
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None

class CheckinStatusQuery(BaseModel):
    status: Optional[str] = None
    need_id: Optional[int] = None
    user_id: Optional[int] = None
    has_checkin: Optional[bool] = None
    has_checkout: Optional[bool] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    limit: int = 100

# Initialize sync tool on startup
@app.on_event("startup")
async def startup_event():
    global sync_tool
    try:
        sync_tool = GalaxyAPISync()
        logger.info("Galaxy Digital Sync Tool initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize sync tool: {str(e)}")
        raise

# Health check endpoint
@app.get("/health")
async def health_check():
    """Check if the API and database connection are healthy"""
    try:
        # Test database connection
        sync_tool.db.list_collection_names()
        return {
            "status": "healthy",
            "database": "connected",
            "timestamp": datetime.utcnow()
        }
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Service unhealthy: {str(e)}")

# Sync endpoints
@app.post("/sync/all", response_model=SyncResponse)
async def sync_all_resources(background_tasks: BackgroundTasks):
    """Trigger a full sync of all resources"""
    try:
        background_tasks.add_task(sync_tool.sync_all_resources)
        return SyncResponse(
            status="started",
            message="Full sync started in background"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/sync/resource", response_model=SyncResponse)
async def sync_specific_resource(request: ResourceSyncRequest, background_tasks: BackgroundTasks):
    """Sync a specific resource"""
    try:
        background_tasks.add_task(
            sync_tool._sync_resource,
            request.resource_name,
            request.params,
            request.since_field
        )
        return SyncResponse(
            status="started",
            message=f"Sync for {request.resource_name} started in background"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Report generation endpoints
@app.post("/reports/generate", response_model=SyncResponse)
async def generate_report(request: ReportRequest, background_tasks: BackgroundTasks):
    """Generate a specific report"""
    try:
        if request.sync_first:
            sync_tool.sync_all_resources()
        
        background_tasks.add_task(
            sync_tool.generate_specific_report,
            request.report_type.value
        )
        
        return SyncResponse(
            status="started",
            message=f"Report generation for {request.report_type.value} started"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/reports/generate-all", response_model=SyncResponse)
async def generate_all_reports(background_tasks: BackgroundTasks, sync_first: bool = True):
    """Generate all activity reports"""
    try:
        if sync_first:
            sync_tool.sync_all_resources()
        
        background_tasks.add_task(sync_tool.generate_activity_reports)
        
        return SyncResponse(
            status="started",
            message="All reports generation started in background"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Query endpoints
@app.post("/query")
async def query_collection(request: QueryRequest):
    """Query a MongoDB collection with custom filters and date range"""
    try:
        collection = sync_tool.db[request.collection]
        filter_query = request.filter.copy()
        
        # Add date range filters if provided
        if request.start_date or request.end_date:
            date_filter = {}
            if request.start_date:
                date_filter["$gte"] = request.start_date
            if request.end_date:
                date_filter["$lte"] = request.end_date
            
            # Apply date filter to appropriate field based on collection
            date_field = "created_at"  # default
            if request.collection == "hours":
                date_field = "date_of_service"
            elif request.collection == "shift_status":
                date_field = "start"
            
            filter_query[date_field] = date_filter
        
        cursor = collection.find(
            filter_query,
            request.projection
        ).skip(request.skip).limit(request.limit)
        
        results = list(cursor)
        
        # Convert ObjectId to string for JSON serialization
        for doc in results:
            if "_id" in doc:
                doc["_id"] = str(doc["_id"])
        
        return {
            "collection": request.collection,
            "count": len(results),
            "data": results,
            "filter": filter_query
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/shifts/checkin-status")
async def get_checkin_status(
    status: Optional[str] = None,
    need_id: Optional[int] = None,
    user_id: Optional[int] = None,
    has_checkin: Optional[bool] = None,
    has_checkout: Optional[bool] = None,
    start_date: Optional[datetime] = Query(None),
    end_date: Optional[datetime] = Query(None),
    limit: int = Query(100, le=1000)
):
    """Get shifts filtered by check-in/check-out status and date range"""
    try:
        # Sync the 'hours' resource and regenerate aggregations before querying
        sync_tool._sync_resource('hours', since_field='since_updated')
        sync_tool._generate_shift_status()
        sync_tool._generate_checkin_checkout_analysis()

        filter_query = {}
        
        if status:
            filter_query["users.checkout_status"] = status
        if need_id:
            filter_query["need_id"] = need_id
        if user_id:
            filter_query["users.id"] = user_id
        if has_checkin is not None:
            filter_query["users.has_checkin"] = has_checkin
        if has_checkout is not None:
            filter_query["users.has_checkout"] = has_checkout
            
        # Add date range filters
        if start_date or end_date:
            date_filter = {}
            if start_date:
                date_filter["$gte"] = start_date
            if end_date:
                date_filter["$lte"] = end_date
            filter_query["start"] = date_filter
        
        collection = sync_tool.db["shift_status"]
        results = list(collection.find(filter_query).limit(limit))
        
        # Convert ObjectId to string
        for doc in results:
            if "_id" in doc:
                doc["_id"] = str(doc["_id"])
        
        return {
            "count": len(results),
            "filter": filter_query,
            "data": results
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/shifts/today")
async def get_today_shifts(
    status: Optional[str] = None,
    need_id: Optional[int] = None,
    user_id: Optional[int] = None,
    has_checkin: Optional[bool] = None,
    has_checkout: Optional[bool] = None,
    limit: int = Query(100, le=1000)
):
    """Get shifts for the current date"""
    try:
        # Get today's date range
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        tomorrow = today.replace(hour=23, minute=59, second=59)
        
        filter_query = {
            "start": {
                "$gte": today,
                "$lte": tomorrow
            }
        }
        
        if status:
            filter_query["users.checkout_status"] = status
        if need_id:
            filter_query["need_id"] = need_id
        if user_id:
            filter_query["users.id"] = user_id
        if has_checkin is not None:
            filter_query["users.has_checkin"] = has_checkin
        if has_checkout is not None:
            filter_query["users.has_checkout"] = has_checkout
        
        collection = sync_tool.db["shift_status"]
        results = list(collection.find(filter_query).limit(limit))
        
        # Convert ObjectId to string
        for doc in results:
            if "_id" in doc:
                doc["_id"] = str(doc["_id"])
        
        return {
            "count": len(results),
            "date": today.date().isoformat(),
            "filter": filter_query,
            "data": results
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/users/pending-checkout")
async def get_users_pending_checkout(limit: int = Query(100, le=1000)):
    """Get all users who have checked in but not checked out"""
    try:
        collection = sync_tool.db["shift_status"]
        
        # Find shifts with users who have checked in but not out
        pipeline = [
            {"$unwind": "$users"},
            {"$match": {
                "users.checkout_status": "checked_in_only",
                "users.hour_status": "pending"
            }},
            {"$project": {
                "shift_id": "$id",
                "shift_title": "$title",
                "shift_start": "$start",
                "shift_end": "$end",
                "need_id": "$need_id",
                "user": "$users"
            }},
            {"$limit": limit}
        ]
        
        results = list(collection.aggregate(pipeline))
        
        return {
            "count": len(results),
            "data": results
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/stats/summary")
async def get_summary_stats():
    """Get summary statistics across all collections"""
    try:
        stats = {
            "timestamp": datetime.utcnow(),
            "collections": {}
        }
        
        # Get counts for main collections
        collections = ["agencies", "users", "needs", "hours", "responses", "shift_status"]
        
        for coll_name in collections:
            if coll_name in sync_tool.db.list_collection_names():
                count = sync_tool.db[coll_name].count_documents({})
                stats["collections"][coll_name] = count
        
        # Get specific stats for shift_status
        if "shift_status" in sync_tool.db.list_collection_names():
            shift_collection = sync_tool.db["shift_status"]
            
            # Count users by checkout status
            pipeline = [
                {"$unwind": "$users"},
                {"$group": {
                    "_id": "$users.checkout_status",
                    "count": {"$sum": 1}
                }}
            ]
            
            checkout_stats = list(shift_collection.aggregate(pipeline))
            stats["checkout_status_summary"] = {
                item["_id"]: item["count"] for item in checkout_stats if item["_id"]
            }
        
        return stats
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/metadata/last-sync")
async def get_last_sync_times():
    """Get last sync times for all resources"""
    try:
        metadata_collection = sync_tool.db["sync_metadata"]
        metadata = list(metadata_collection.find({}))
        
        sync_times = {}
        for item in metadata:
            resource = item.get("resource")
            last_sync = item.get("last_sync")
            if resource and last_sync:
                sync_times[resource] = last_sync
        
        return {
            "sync_times": sync_times,
            "current_time": datetime.utcnow()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/vol_dash", response_class=HTMLResponse)
async def serve_vol_dash():
    """Serve the Volunteer Dashboard HTML page"""
    try:
        with open("vol_dash.html", "r", encoding="utf-8") as f:
            html_content = f.read()
        return HTMLResponse(content=html_content, status_code=200)
    except Exception as e:
        return HTMLResponse(content=f"<h1>Error loading dashboard</h1><p>{str(e)}</p>", status_code=500)

# Run the app
if __name__ == "__main__":
    import uvicorn
    import os
    import sys
    
    # Allow port to be set via environment variable or command-line argument
    port = 8000
    # Check environment variable first
    if os.getenv("PORT"):
        try:
            port = int(os.getenv("PORT"))
        except ValueError:
            pass
    # Check command-line argument (e.g., python api.py 8080)
    if len(sys.argv) > 1:
        try:
            port = int(sys.argv[1])
        except ValueError:
            pass
    uvicorn.run(app, host="0.0.0.0", port=port) 