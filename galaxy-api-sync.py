import os
import time
import json
import logging
import datetime
import requests
from typing import Dict, List, Any, Optional, Union
import pymongo
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("galaxy_sync.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("galaxy_api_sync")

class GalaxyAPISync:
    """
    A class to synchronize data between the Galaxy Digital API and a MongoDB database.
    """
    
    def __init__(self, config_path: str = "config.json"):
        """
        Initialize the sync tool.
        
        Args:
            config_path: Path to the configuration file
        """
        self.config = self._load_config(config_path)
        self.api_base_url = self.config.get("api_base_url", "https://api.galaxydigital.com/api")
        self.token = os.getenv("GALAXY_API_TOKEN")
        if not self.token:
            raise ValueError("API token not found in environment variables")
        
        # Connect to MongoDB
        self.client = MongoClient(self.config.get("mongodb_uri", "mongodb://localhost:27017/"))
        self.db = self.client[self.config.get("mongodb_database", "galaxy_digital")]
        
        # Setup API headers
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        
        logger.info(f"Initialized Galaxy API Sync with base URL: {self.api_base_url}")
    
    def _load_config(self, config_path: str) -> Dict:
        """
        Load configuration from a JSON file.
        
        Args:
            config_path: Path to the configuration file
            
        Returns:
            Dictionary containing configuration
        """
        try:
            with open(config_path, "r") as f:
                return json.load(f)
        except FileNotFoundError:
            logger.warning(f"Config file {config_path} not found. Using default configuration.")
            return {}
    
    def _make_api_request(self, endpoint: str, params: Dict = None) -> Dict:
        """
        Make a request to the Galaxy Digital API.
        
        Args:
            endpoint: API endpoint
            params: Query parameters
            
        Returns:
            API response as dictionary
        """
        url = f"{self.api_base_url}/{endpoint}"
        try:
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {str(e)}, URL: {url}")
            raise
    
    def _update_document(self, collection: Collection, document: Dict, id_field: str = "id") -> None:
        """
        Update a document in MongoDB.
        
        Args:
            collection: MongoDB collection
            document: Document to update
            id_field: Field name to use as the identifier
        """
        # Add sync metadata
        document["_synced_at"] = datetime.datetime.utcnow()
        document["_sync_source"] = "galaxy_api"
        
        # Ensure the document has an ID
        if id_field not in document:
            logger.warning(f"Document missing ID field: {id_field}")
            return
        
        # Use the API ID as a string for the MongoDB _id
        mongo_id = str(document[id_field])
        
        # Try to update the document, insert if not exists
        result = collection.update_one(
            {id_field: document[id_field]},
            {"$set": document},
            upsert=True
        )
        
        if result.upserted_id:
            logger.debug(f"Inserted new document with ID: {mongo_id}")
        else:
            logger.debug(f"Updated document with ID: {mongo_id}")
    
    def _sync_resource(self, resource_name: str, params: Dict = None, since_field: str = None) -> None:
        """
        Sync a resource from the API to MongoDB.
        
        Args:
            resource_name: Name of the resource/endpoint
            params: Additional query parameters
            since_field: Field to use for incremental sync
        """
        logger.info(f"Syncing resource: {resource_name}")
        collection = self.db[resource_name]
        
        # Prepare query parameters
        query_params = params or {}
        
        # Add a since parameter if specified and we have a last sync time
        if since_field:
            last_sync = self._get_last_sync_time(resource_name)
            if last_sync:
                query_params[since_field] = last_sync.strftime("%Y-%m-%d %H:%M")
                logger.info(f"Incremental sync using {since_field} since {last_sync}")
        
        # Add pagination parameters
        query_params.setdefault("per_page", 100)
        
        try:
            # Make the API request
            response = self._make_api_request(resource_name, query_params)
            
            # Check if we have data
            if "data" not in response:
                logger.warning(f"No data found in response for {resource_name}")
                return
            
            # Process each item
            for item in response["data"]:
                self._update_document(collection, item)
            
            logger.info(f"Synced {len(response['data'])} items for {resource_name}")
            
            # Update the last sync time
            self._update_sync_metadata(resource_name)
            
        except Exception as e:
            logger.error(f"Error syncing {resource_name}: {str(e)}")
            raise
    
    def _get_last_sync_time(self, resource_name: str) -> Optional[datetime.datetime]:
        """
        Get the last sync time for a resource.
        
        Args:
            resource_name: Name of the resource
            
        Returns:
            Datetime of last sync or None
        """
        metadata = self.db["sync_metadata"].find_one({"resource": resource_name})
        if metadata and "last_sync" in metadata:
            return metadata["last_sync"]
        return None
    
    def _update_sync_metadata(self, resource_name: str) -> None:
        """
        Update the sync metadata for a resource.
        
        Args:
            resource_name: Name of the resource
        """
        now = datetime.datetime.utcnow()
        self.db["sync_metadata"].update_one(
            {"resource": resource_name},
            {"$set": {"last_sync": now, "last_success": now}},
            upsert=True
        )
    
    def sync_all_resources(self) -> None:
        """
        Sync all configured resources.
        """
        logger.info("Starting sync of all resources")
        
        resources = self.config.get("resources", [
            {"name": "agencies", "since_field": "since_updated"},
            {"name": "users", "since_field": "since_updated"},
            {"name": "needs", "since_field": "since_updated"},
            {"name": "events", "since_field": "since_updated"},
            {"name": "hours", "since_field": "since_updated"},
            {"name": "responses", "since_field": "since_updated"},
        ])
        
        for resource in resources:
            name = resource["name"]
            since_field = resource.get("since_field")
            params = resource.get("params", {})
            
            try:
                self._sync_resource(name, params, since_field)
            except Exception as e:
                logger.error(f"Failed to sync {name}: {str(e)}")
    
    def create_indexes(self) -> None:
        """
        Create useful indexes in the MongoDB collections.
        """
        logger.info("Creating indexes")
        
        # Define indexes for each collection
        indexes = {
            "agencies": [
                ("id", pymongo.ASCENDING),
                ("agency_name", pymongo.ASCENDING),
                ("agency_status", pymongo.ASCENDING),
            ],
            "users": [
                ("id", pymongo.ASCENDING),
                ("user_email", pymongo.ASCENDING),
                ("user_fname", pymongo.ASCENDING),
                ("user_lname", pymongo.ASCENDING),
                ("user_status", pymongo.ASCENDING),
            ],
            "needs": [
                ("id", pymongo.ASCENDING),
                ("agency_id", pymongo.ASCENDING),
                ("need_title", pymongo.TEXT),
                ("need_status", pymongo.ASCENDING),
            ],
            "events": [
                ("id", pymongo.ASCENDING),
                ("event_area_id", pymongo.ASCENDING),
                ("event_date_start", pymongo.ASCENDING),
            ],
            "hours": [
                ("id", pymongo.ASCENDING),
                ("user.id", pymongo.ASCENDING),
                ("need.id", pymongo.ASCENDING),
                ("hour_date_start", pymongo.ASCENDING),
            ],
            "responses": [
                ("id", pymongo.ASCENDING),
                ("user.id", pymongo.ASCENDING),
                ("need.id", pymongo.ASCENDING),
            ],
        }
        
        # Create indexes for each collection
        for collection_name, collection_indexes in indexes.items():
            collection = self.db[collection_name]
            for field, direction in collection_indexes:
                try:
                    if direction == pymongo.TEXT:
                        collection.create_index([(field, direction)])
                    else:
                        collection.create_index(field, direction)
                    logger.info(f"Created index on {collection_name}.{field}")
                except Exception as e:
                    logger.error(f"Failed to create index on {collection_name}.{field}: {str(e)}")
    
    def run_scheduled_sync(self, interval_minutes: int = 60) -> None:
        """
        Run a scheduled sync at specified intervals.
        
        Args:
            interval_minutes: Interval between syncs in minutes
        """
        logger.info(f"Starting scheduled sync every {interval_minutes} minutes")
        
        # Initial setup - create indexes
        self.create_indexes()
        
        # Run sync loop
        while True:
            try:
                start_time = time.time()
                logger.info("Starting scheduled sync")
                
                # Sync all resources
                self.sync_all_resources()
                
                # Log completion
                elapsed = time.time() - start_time
                logger.info(f"Completed sync in {elapsed:.2f} seconds")
                
                # Sleep until next interval
                time.sleep(interval_minutes * 60)
                
            except KeyboardInterrupt:
                logger.info("Sync interrupted. Exiting.")
                break
            except Exception as e:
                logger.error(f"Error in sync loop: {str(e)}")
                # Sleep a bit before retrying
                time.sleep(60)

if __name__ == "__main__":
    # Create and run the sync tool
    try:
        sync_tool = GalaxyAPISync()
        
        # Check if we should run a one-time sync
        if os.getenv("SYNC_ONCE", "").lower() == "true":
            sync_tool.sync_all_resources()
        else:
            # Run scheduled sync every hour by default, or as specified in config
            interval = int(os.getenv("SYNC_INTERVAL_MINUTES", "60"))
            sync_tool.run_scheduled_sync(interval)
            
    except Exception as e:
        logger.critical(f"Failed to initialize or run sync tool: {str(e)}")
