import logging
import os
import time
import json
from loguru import logger
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
        
        # Get credentials from environment variables
        self.api_key = os.getenv("GALAXY_API_KEY")
        self.email = os.getenv("GALAXY_EMAIL")
        self.password = os.getenv("GALAXY_PASSWORD")
        
        if not self.email or not self.password or not self.api_key:
            raise ValueError("API key, email, and password must be set in environment variables")
        
        # Connect to MongoDB using environment variables
        mongodb_uri = os.getenv("MONGODB_URI", self.config.get("mongodb_uri", "mongodb://localhost:27017/"))
        mongodb_database = os.getenv("MONGODB_DATABASE", self.config.get("mongodb_database", "galaxy_digital"))
        
        logger.info(f"Connecting to MongoDB database: {mongodb_database}")
        logger.info(f"Using connection string: {mongodb_uri.split('@')[0].split('://')[0]}://*****@{mongodb_uri.split('@')[1] if '@' in mongodb_uri else 'localhost'}")
        
        # Connect with proper authentication handling and retries
        max_retries = 3
        retry_delay = 2
        last_error = None
        
        for attempt in range(max_retries):
            try:
                logger.info(f"Connecting to MongoDB (attempt {attempt+1}/{max_retries})...")
                
                # Parse connection options from URI
                connection_options = {
                    "serverSelectionTimeoutMS": 5000,
                    "connectTimeoutMS": 10000,
                    "socketTimeoutMS": 45000,
                    "retryWrites": True,
                    "w": "majority"
                }
                
                self.client = MongoClient(mongodb_uri, **connection_options)
                
                # Test the connection with a simple command
                self.client.admin.command('ping')
                
                # Set the database
                self.db = self.client[mongodb_database]
                
                # Test a simple operation on the database
                self.db.list_collection_names()
                
                logger.info(f"Successfully connected to MongoDB database: {mongodb_database}")
                break
            except pymongo.errors.ConfigurationError as e:
                last_error = e
                logger.error(f"MongoDB configuration error: {str(e)}")
                raise  # Configuration errors should not be retried
            except pymongo.errors.OperationFailure as e:
                last_error = e
                if e.code == 18 or e.code == 8000:  # Authentication error codes
                    logger.error(f"MongoDB authentication failed: {str(e)}")
                    if attempt < max_retries - 1:
                        logger.warning(f"Retrying in {retry_delay} seconds... (attempt {attempt+1}/{max_retries})")
                        time.sleep(retry_delay)
                    else:
                        logger.error("Authentication failed after maximum retries. Please check your credentials.")
                        raise
                else:
                    logger.error(f"MongoDB operation error: {str(e)}")
                    raise
            except pymongo.errors.ServerSelectionTimeoutError as e:
                last_error = e
                logger.warning(f"MongoDB server selection timeout (attempt {attempt+1}/{max_retries}): {str(e)}")
                if attempt < max_retries - 1:
                    logger.warning(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    logger.error(f"Failed to connect to MongoDB after {max_retries} attempts: {str(e)}")
                    raise
            except Exception as e:
                last_error = e
                if attempt < max_retries - 1:
                    logger.warning(f"Failed to connect to MongoDB (attempt {attempt+1}/{max_retries}): {str(e)}")
                    logger.warning(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    logger.error(f"Failed to connect to MongoDB after {max_retries} attempts: {str(e)}")
                    raise
        
        # Initialize session and token
        self.session = requests.Session()
        self.token = None
        self.login_response = None
        self.debug = self.config.get("debug", False)
        self.base_url = self.api_base_url  # Fix for _login method

        # Login to get token
        self._login()
        
        # Setup API headers after login
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        
        logger.info(f"Initialized Galaxy API Sync with base URL: {self.api_base_url}")

    def _login(self, max_retries: int = 3, retry_delay: int = 2) -> Optional[str]:
        """
        Authenticate with the Galaxy Digital API.
        
        Args:
            max_retries: Maximum number of login attempts
            retry_delay: Delay between retries in seconds
            
        Returns:
            Authentication token or None if authentication failed
        """
        login_url = f"{self.base_url}/users/login"
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }
        data = {
            'key': self.api_key,
            'user_email': self.email,
            'user_password': self.password,
        }
        
        if self.debug:
            # Mask password in debug output
            debug_data = data.copy()
            if 'user_password' in debug_data:
                debug_data['user_password'] = '********'
            logging.debug(f"Login request to {login_url} with data: {json.dumps(debug_data)}")
        
        for attempt in range(max_retries):
            try:
                response = requests.post(login_url, headers=headers, json=data)
                
                if self.debug:
                    logging.debug(f"Login response status: {response.status_code}")
                    if response.status_code != 200:
                        logging.debug(f"Response content: {response.text[:500]}...")
                
                # Handle different response status codes
                if response.status_code == 200:
                    resp_data = response.json()
                    self.login_response = resp_data.get('data', {})
                    self.token = self.login_response.get('token')
                    
                    if not self.token:
                        logging.error("Authentication succeeded but no token was returned")
                        return None
                    
                    # Update session headers with token
                    self.session.headers.update({
                        'Accept': 'application/json',
                        'Authorization': f"Bearer {self.token}"
                    })
                    
                    logging.info("Successfully authenticated with Galaxy Digital API")
                    return self.token
                elif response.status_code == 401:
                    logging.error("Authentication failed: Invalid credentials")
                    return None
                elif response.status_code == 500:
                    if attempt < max_retries - 1:
                        logging.warning(f"Server error during login (attempt {attempt+1}/{max_retries}). Retrying in {retry_delay} seconds...")
                        time.sleep(retry_delay)
                    else:
                        logging.error("Server error during login. Max retries exceeded.")
                        response.raise_for_status()
                else:
                    response.raise_for_status()
                    
            except requests.exceptions.RequestException as e:
                logging.error(f"Error authenticating with Galaxy Digital API: {str(e)}")
                if hasattr(e, 'response') and e.response:
                    logging.error(f"Response: {e.response.text[:500]}...")
                
                if attempt < max_retries - 1:
                    logging.warning(f"Retrying login in {retry_delay} seconds... (attempt {attempt+1}/{max_retries})")
                    time.sleep(retry_delay)
                else:
                    logging.error("Max retries exceeded for login")
                    raise
        
        return None
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
            response = self.session.request('GET', url, headers=self.headers, params=params)
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
        
        # Try to update the document with retries
        max_retries = 3
        retry_delay = 2
        
        for attempt in range(max_retries):
            try:
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
                    
                # If successful, break the retry loop
                break
                
            except pymongo.errors.AutoReconnect as e:
                if attempt < max_retries - 1:
                    logger.warning(f"MongoDB connection error, retrying in {retry_delay}s: {str(e)}")
                    time.sleep(retry_delay)
                else:
                    logger.error(f"Failed to update document after {max_retries} attempts: {str(e)}")
                    raise
            except Exception as e:
                logger.error(f"Error updating document: {str(e)}")
                raise
    
    def _sync_resource(self, resource_name: str, params: Dict = None, since_field: str = None) -> None:
        """
        Sync a resource from the API to MongoDB.
        
        Args:
            resource_name: Name of the resource/endpoint
            params: Additional query parameters
            since_field: Field to use for incremental sync
        """
        logger.info(f"Syncing resource: {resource_name}")
        collection = self.db[f"{resource_name}"]
        
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
            
            # Process each item with MongoDB error handling
            successful_items = 0
            failed_items = 0
            
            for item in response["data"]:
                try:
                    self._update_document(collection, item)
                    successful_items += 1
                except pymongo.errors.PyMongoError as e:
                    failed_items += 1
                    logger.error(f"MongoDB error while updating item {item.get('id', 'unknown')} for {resource_name}: {str(e)}")
            
            logger.info(f"Synced {successful_items} items for {resource_name} (failed: {failed_items})")
            
            # Only update the last sync time if we had some successful updates
            if successful_items > 0:
                self._update_sync_metadata(resource_name)
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request error for {resource_name}: {str(e)}")
            raise
        except pymongo.errors.PyMongoError as e:
            logger.error(f"MongoDB error for {resource_name}: {str(e)}")
            raise
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
        
        # Create indexes for each collection with retries
        max_retries = 3
        retry_delay = 2
        
        # Create indexes for each collection
        for collection_name, collection_indexes in indexes.items():
            collection = self.db[collection_name]
            for field, direction in collection_indexes:
                for attempt in range(max_retries):
                    try:
                        if direction == pymongo.TEXT:
                            # For text indexes, we need to use a list of tuples
                            collection.create_index([(field, direction)])
                        else:
                            # For regular indexes, we need to use a list of tuples as well
                            collection.create_index([(field, direction)])
                        logger.info(f"Created index on {collection_name}.{field}")
                        break  # Break the retry loop if successful
                    except pymongo.errors.AutoReconnect as e:
                        if attempt < max_retries - 1:
                            logger.warning(f"MongoDB connection error while creating index on {collection_name}.{field}, retrying in {retry_delay}s: {str(e)}")
                            time.sleep(retry_delay)
                        else:
                            logger.error(f"Failed to create index on {collection_name}.{field} after {max_retries} attempts: {str(e)}")
                    except Exception as e:
                        logger.error(f"Failed to create index on {collection_name}.{field}: {str(e)}")
                        break  # Break the retry loop for non-connection errors
    
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
