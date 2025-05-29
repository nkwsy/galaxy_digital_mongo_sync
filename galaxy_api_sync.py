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
import sys
try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except ImportError:
    from backports.zoneinfo import ZoneInfo  # For older Python with backports

print("Script is starting...")

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
        
        # Initialize geocoding flag
        self.enable_geocoding = self.config.get("enable_geocoding", False)
        
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
        Make a request to the Galaxy Digital API with retries and rate limiting handling.
        
        Args:
            endpoint: API endpoint
            params: Query parameters
            
        Returns:
            API response as dictionary
        """
        url = f"{self.api_base_url}/{endpoint}"
        max_retries = 3
        retry_delay = 2
        rate_limit_delay = 60  # Default delay for rate limiting
        
        # Ensure headers are set correctly for each request
        headers = {
            'Accept': 'application/json',
            'Authorization': f"Bearer {self.token}",
            'Content-Type': 'application/json'
        }
        
        for attempt in range(max_retries):
            try:
                logger.debug(f"Making API request to {url} (attempt {attempt+1}/{max_retries})")
                if params:
                    logger.debug(f"Query parameters: {params}")
                
                # Use params parameter for GET requests with query parameters
                response = requests.get(url, headers=headers, params=params)
                
                # Check for rate limiting (status code 429)
                if response.status_code == 429:
                    # Get retry-after header if available
                    retry_after = int(response.headers.get('Retry-After', rate_limit_delay))
                    logger.warning(f"Rate limit exceeded. Waiting for {retry_after} seconds before retrying...")
                    time.sleep(retry_after)
                    continue
                
                # Check for authentication issues (status code 401)
                if response.status_code == 401:
                    logger.warning("Authentication token expired. Refreshing token...")
                    self._login()  # Refresh the token
                    headers["Authorization"] = f"Bearer {self.token}"
                    continue
                
                # Check for server errors (5xx)
                if 500 <= response.status_code < 600:
                    if attempt < max_retries - 1:
                        wait_time = retry_delay * (attempt + 1)  # Exponential backoff
                        logger.warning(f"Server error {response.status_code}. Retrying in {wait_time} seconds...")
                        time.sleep(wait_time)
                        continue
                        
                # For 404 errors, log more details about the request
                if response.status_code == 404:
                    # Check if this is a "no results" response vs actual error
                    try:
                        error_data = response.json()
                        error_message = error_data.get("message", "").lower()
                        if "no results" in error_message or "not found" in error_message:
                            logger.info(f"No results found for API request to {url}")
                            # Return empty data response instead of retrying
                            return {"data": []}
                    except (ValueError, KeyError):
                        # If we can't parse the response, treat as normal 404
                        pass
                    
                    logger.warning(f"API request failed: {response.status_code} {response.reason} for url: {response.url}. Response: {response.text[:200]}. Retrying in {retry_delay * (attempt + 1)} seconds...")
                    if attempt < max_retries - 1:
                        wait_time = retry_delay * (attempt + 1)
                        time.sleep(wait_time)
                        continue
                
                # Raise for other error status codes
                response.raise_for_status()
                
                # Parse and return the JSON response
                return response.json()
                
            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1:
                    wait_time = retry_delay * (attempt + 1)
                    logger.warning(f"API request failed: {str(e)}. Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"API request failed after {max_retries} attempts: {str(e)}")
                    raise
    
    def _update_document(self, collection: Collection, document: Dict, id_field: str = "id") -> None:
        """
        Update a document in MongoDB with proper type handling.
        
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
        
        # Ensure proper data types for MongoDB
        document = self._normalize_document_types(document)
        
        # Add geocoding information if enabled
        if self.enable_geocoding:
            document = self._add_geocoding_to_document(document)
        
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
    
    def _normalize_document_types(self, document: Dict) -> Dict:
        """
        Normalize data types in a document for MongoDB compatibility.
        
        Args:
            document: Document to normalize
            
        Returns:
            Normalized document with proper data types
        """
        normalized = {}
        
        for key, value in document.items():
            # Handle nested dictionaries recursively
            if isinstance(value, dict):
                normalized[key] = self._normalize_document_types(value)
            
            # Handle lists/arrays
            elif isinstance(value, list):
                normalized[key] = [
                    self._normalize_document_types(item) if isinstance(item, dict) else self._normalize_value_type(item)
                    for item in value
                ]
            
            # Handle scalar values
            else:
                normalized[key] = self._normalize_value_type(value)
        
        return normalized
    
    def _normalize_value_type(self, value: Any) -> Any:
        """
        Normalize a single value's type for MongoDB compatibility.
        
        Args:
            value: Value to normalize
            
        Returns:
            Normalized value with proper type
        """
        # Handle None values
        if value is None:
            return None
        
        # Handle datetime.date objects - convert to datetime.datetime for MongoDB compatibility
        if isinstance(value, datetime.date) and not isinstance(value, datetime.datetime):
            # Convert date to datetime at midnight UTC
            return datetime.datetime.combine(value, datetime.time.min)
        
        # Handle date strings (common in API responses)
        if isinstance(value, str):
            # Try to parse ISO format dates
            try:
                if len(value) >= 10 and (
                    (value[4] == '-' and value[7] == '-') or  # ISO format: YYYY-MM-DD
                    (value[2] == '/' and value[5] == '/')     # US format: MM/DD/YYYY
                ):
                    # Check if it has time component
                    if 'T' in value or ' ' in value:
                        try:
                            # Try ISO format with time
                            return datetime.datetime.fromisoformat(value.replace('Z', '+00:00'))
                        except (ValueError, TypeError):
                            try:
                                # Try common datetime formats
                                for fmt in [
                                    '%Y-%m-%d %H:%M:%S',
                                    '%Y-%m-%d %H:%M',
                                    '%m/%d/%Y %H:%M:%S',
                                    '%m/%d/%Y %H:%M'
                                ]:
                                    try:
                                        return datetime.datetime.strptime(value, fmt)
                                    except ValueError:
                                        continue
                            except Exception:
                                # If all parsing fails, keep as string
                                pass
                    else:
                        # Date without time - convert to datetime at midnight
                        try:
                            for fmt in ['%Y-%m-%d', '%m/%d/%Y']:
                                try:
                                    date_obj = datetime.datetime.strptime(value, fmt).date()
                                    # Convert to datetime at midnight UTC
                                    return datetime.datetime.combine(date_obj, datetime.time.min)
                                except ValueError:
                                    continue
                        except Exception:
                            # If parsing fails, keep as string
                            pass
            except Exception:
                # If any error occurs during date parsing, keep as string
                pass
        
        # Handle numeric strings
        if isinstance(value, str) and value.strip():
            # Try to convert to int or float if it looks like a number
            if value.strip().replace('.', '', 1).isdigit():
                try:
                    if '.' in value:
                        return float(value)
                    else:
                        return int(value)
                except (ValueError, TypeError):
                    pass
        
        # Handle boolean strings
        if isinstance(value, str) and value.lower() in ['true', 'false']:
            return value.lower() == 'true'
        
        # Return the original value if no conversion is needed
        return value
    
    def _sync_resource(self, resource_name: str, params: Dict = None, since_field: str = None) -> None:
        """
        Sync a resource from the API to MongoDB.
        
        Args:
            resource_name: Name of the resource/endpoint
            params: Additional query parameters
            since_field: Field to use for incremental sync (e.g., "since_updated")
        """
        logger.info(f"Syncing resource: {resource_name}")
        collection = self.db[f"{resource_name}"]
        
        # Prepare query parameters
        query_params = params.copy() if params else {}
        
        # Add pagination parameters
        query_params.setdefault("per_page", 150)
        query_params.setdefault("show_inactive", "Yes")
        
        # Get last sync time for incremental sync
        last_sync_time = self._get_last_sync_time(resource_name)
        if last_sync_time and since_field:
            # Ensure last_sync_time is timezone-aware UTC
            if last_sync_time.tzinfo is None:
                last_sync_time = last_sync_time.replace(tzinfo=datetime.timezone.utc)
            else:
                last_sync_time = last_sync_time.astimezone(datetime.timezone.utc)
            # Convert to America/Chicago timezone
            chicago_time = last_sync_time.astimezone(ZoneInfo("America/Chicago"))
            # Format as 'YYYY-MM-DD HH:MM:SS'
            since_updated = chicago_time.strftime("%Y-%m-%d %H:%M:%S")
            query_params[since_field] = since_updated
            logger.info(f"Using incremental sync for {resource_name} since: {since_updated} (America/Chicago)")
        else:
            logger.info(f"Performing full sync for {resource_name} (no previous sync time found)")
        
        # Initialize counters for total items
        total_successful_items = 0
        total_failed_items = 0
        has_more_pages = True
        last_id = None
        
        # Process all pages
        while has_more_pages:
            try:
                # If we have a last_id from previous page, use since_id for pagination
                if last_id:
                    query_params["since_id"] = last_id
                    logger.debug(f"Using since_id for pagination: {last_id}")
                
                logger.info(f"Fetching data for {resource_name}")
                
                # Make the API request
                response = self._make_api_request(resource_name, query_params)
                
                # Check if we have data
                if "data" not in response:
                    logger.warning(f"No data found in response for {resource_name}")
                    break
                
                # Get the items for this page
                items = response["data"]
                
                # If we got no items, this is the last page
                if len(items) == 0:
                    has_more_pages = False
                    logger.info(f"Reached last page for {resource_name}, got {len(items)} items")
                    break
                
                # Check if we got fewer items than per_page, indicating last page
                per_page = int(query_params.get("per_page", 150))
                if len(items) < per_page:
                    has_more_pages = False
                    logger.info(f"Reached last page for {resource_name}, got {len(items)} items (less than {per_page})")
                
                # Process each item with MongoDB error handling
                page_successful_items = 0
                page_failed_items = 0
                
                for item in items:
                    try:
                        self._update_document(collection, item)
                        page_successful_items += 1
                        
                        # Track the last ID for pagination
                        if "id" in item and (last_id is None or int(str(item["id"])) > int(str(last_id))):
                            last_id = str(item["id"])
                            logger.debug(f"Updated last_id to: {last_id} for {resource_name}")
                    except pymongo.errors.PyMongoError as e:
                        page_failed_items += 1
                        logger.error(f"MongoDB error while updating item {item.get('id', 'unknown')} for {resource_name}: {str(e)}")
                
                # Update totals
                total_successful_items += page_successful_items
                total_failed_items += page_failed_items
                
                logger.info(f"Synced {page_successful_items} items for {resource_name} (failed: {page_failed_items})")
                
            except requests.exceptions.RequestException as e:
                logger.error(f"API request error for {resource_name}: {str(e)}")
                raise
            except pymongo.errors.PyMongoError as e:
                logger.error(f"MongoDB error for {resource_name}: {str(e)}")
                raise
            except Exception as e:
                logger.error(f"Error syncing {resource_name}: {str(e)}")
                raise
        
        # Log the total number of items synced
        logger.info(f"Completed sync for {resource_name}: {total_successful_items} items synced successfully, {total_failed_items} failed")
        
        # Only update the last sync time if we had some successful updates
        if total_successful_items > 0:
            self._update_sync_metadata(resource_name)
    
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
            {"name": "hours", "since_field": "since_updated"},
            {"name": "responses", "since_field": "since_updated"},
            {"name": "qualifications", "since_field": "since_updated"},
            {"name": "teams", "since_field": "since_updated"}
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
            # Indexes for aggregated collections
            "user_activity_summary": [
                ("_id", pymongo.ASCENDING),
                ("total_hours", pymongo.DESCENDING),
                ("shifts_attended", pymongo.DESCENDING),
                ("last_activity", pymongo.DESCENDING),
                ("days_since_last_activity", pymongo.ASCENDING),
                ("user_info.user_email", pymongo.ASCENDING),
                ("user_info.user_fname", pymongo.ASCENDING),
                ("user_info.user_lname", pymongo.ASCENDING),
            ],
            "opportunity_activity": [
                ("_id", pymongo.ASCENDING),
                ("total_hours", pymongo.DESCENDING),
                ("volunteer_count", pymongo.DESCENDING),
                ("last_activity", pymongo.DESCENDING),
                ("need_info.need_title", pymongo.TEXT),
                ("agency_id", pymongo.ASCENDING),
            ],
            "agency_activity": [
                ("_id", pymongo.ASCENDING),
                ("total_hours", pymongo.DESCENDING),
                ("volunteer_count", pymongo.DESCENDING),
                ("opportunity_count", pymongo.DESCENDING),
                ("agency_name", pymongo.ASCENDING),
            ],
            "monthly_activity": [
                ("_id", pymongo.ASCENDING),  # Year-month
                ("total_hours", pymongo.DESCENDING),
            ],
            "shift_status": [
                ("id", pymongo.ASCENDING),
                ("start", pymongo.ASCENDING),
                ("need_id", pymongo.ASCENDING),
                ("users.id", pymongo.ASCENDING),
                ("users.checkin_status", pymongo.ASCENDING),
                ("users.checkout_status", pymongo.ASCENDING),
                ("users.has_checkin", pymongo.ASCENDING),
                ("users.has_checkout", pymongo.ASCENDING),
                ("users.has_manager_approval", pymongo.ASCENDING),
            ],
            "checkin_checkout_analysis": [
                ("_id", pymongo.ASCENDING),
                ("user_info.id", pymongo.ASCENDING),
                ("need_info.id", pymongo.ASCENDING),
                ("hour_date_start", pymongo.ASCENDING),
                ("has_checkin", pymongo.ASCENDING),
                ("has_checkout", pymongo.ASCENDING),
                ("has_manager_approval", pymongo.ASCENDING),
                ("user_info.user_lname", pymongo.ASCENDING),
                ("user_info.user_fname", pymongo.ASCENDING),
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
                
                # Generate aggregated reports
                self.generate_activity_reports()
                
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
                
    def generate_activity_reports(self) -> None:
        """
        Generate aggregated reports on volunteer activity.
        
        This method creates various MongoDB aggregations to analyze volunteer activity,
        including hours logged, shifts attended, and participation patterns.
        
        Reports generated:
        1. User Activity Summary - total hours, shifts, and last activity per user
        2. Opportunity Activity - hours and participation by opportunity
        3. Agency Activity - volunteer engagement by agency
        4. Time-based Activity - activity patterns over time
        5. Shift Status - track current and upcoming shift participation
        6. Check-in/Check-out Analysis - analyze pending hours with check-in patterns
        """
        logger.info("Generating activity reports...")
        
        try:
            # 1. User Activity Summary - aggregate total hours, shifts, and last activity date per user
            self._generate_user_activity_summary()
            
            # 2. Opportunity Activity - analyze hours and participation by opportunity
            self._generate_opportunity_activity()
            
            # 3. Agency Activity - analyze volunteer engagement by agency
            self._generate_agency_activity()
            
            # 4. Time-based Activity - analyze activity patterns over time
            self._generate_time_based_activity()
            
            # 5. Shift Status - track current and upcoming shift participation
            self._generate_shift_status(future_only=False)
            
            # 6. Check-in/Check-out Analysis - analyze pending hours with check-in patterns
            self._generate_checkin_checkout_analysis()
            
            logger.info("Successfully generated all activity reports")
            
        except Exception as e:
            logger.error(f"Error generating activity reports: {str(e)}")
            raise
    
    def generate_specific_report(self, report_type: str) -> None:
        """
        Generate a specific activity report.
        
        Args:
            report_type: Type of report to generate ('user', 'needs', 'opportunity', 'agency', 'time', 'shift_status', 'checkin_analysis')
        """
        logger.info(f"Generating specific report: {report_type}")
        
        try:
            if report_type.lower() == 'user':
                self._generate_user_activity_summary()
            elif report_type.lower() == 'needs' or report_type.lower() == 'opportunity':
                self._generate_opportunity_activity()
            elif report_type.lower() == 'agency':
                self._generate_agency_activity()
            elif report_type.lower() == 'time':
                self._generate_time_based_activity()
            elif report_type.lower() == 'shift_status':
                self._generate_shift_status()
            elif report_type.lower() == 'checkin_analysis' or report_type.lower() == 'checkin':
                self._generate_checkin_checkout_analysis()
            else:
                logger.warning(f"Unknown report type: {report_type}")
                raise ValueError(f"Unknown report type: {report_type}. Valid options are: user, needs, opportunity, agency, time, shift_status, checkin_analysis")
                
            logger.info(f"Successfully generated {report_type} report")
            
        except Exception as e:
            logger.error(f"Error generating {report_type} report: {str(e)}")
            raise
    
    def _generate_shift_status(self, future_only: bool = False) -> None:
        """
        Generate a collection of aggregated data on volunteer shifts with detailed status tracking.
        
        This method creates a comprehensive collection that tracks each shift and the status of
        all volunteers who have signed up, checked in, or logged hours. It's particularly useful
        for real-time monitoring of who is currently working on shifts and for tracking attendance.
        
        The collection includes detailed information on:
        - Who has signed up for a shift but not yet checked in (pending)
        - Who has checked in and is currently working (active)
        - Who has completed their shift with logged hours (completed)
        - Who signed up but didn't show up (absent)
        - Who cancelled their shift participation (cancelled)
        
        Args:
            future_only: If True, only includes shifts that start in the future.
                         If False, includes all shifts (past, current, and future).
        """
        logger.info("Generating shift status collection...")
        
        try:
            # Check if the necessary collections exist
            required_collections = ["needs", "responses", "hours"]
            missing_collections = [coll for coll in required_collections if coll not in self.db.list_collection_names()]
            
            if missing_collections:
                logger.warning(f"Missing required collections: {missing_collections}. Skipping shift status generation.")
                return
            
            # Get the current date for filtering future shifts
            now = datetime.datetime.utcnow()
            
            # Extract needs and create basic shift records
            shift_status_list = self._create_shifts_from_needs(future_only, now)
            
            # For each shift, assign users based on responses
            self._assign_users_from_responses(shift_status_list)
            
            # Update user status based on hours
            self._correlate_hours_to_shifts(shift_status_list)
            
            # Save the shift status data to MongoDB
            self._save_shift_status_data(shift_status_list)
            
            # Create indexes for efficient querying
            self.db["shift_status"].create_index([("id", pymongo.ASCENDING)], unique=True)
            self.db["shift_status"].create_index([("start", pymongo.ASCENDING)])
            self.db["shift_status"].create_index([("need_id", pymongo.ASCENDING)])
            self.db["shift_status"].create_index([("users.id", pymongo.ASCENDING)])
            self.db["shift_status"].create_index([("users.checkin_status", pymongo.ASCENDING)])
            
            # Update sync metadata to track when this was last generated
            self._update_sync_metadata("shift_status")
            
        except Exception as e:
            logger.error(f"Error generating shift status collection: {str(e)}")
            raise
            
    def _create_shifts_from_needs(self, future_only: bool, current_time: datetime.datetime) -> list:
        """
        Create shift records from needs documents.
        
        This function extracts shifts from needs documents and creates basic shift records
        with their properties (id, start time, end time, duration, etc.)
        
        Args:
            future_only: If True, only include shifts that start in the future
            current_time: The current time to use for filtering future shifts
            
        Returns:
            List of shift status records (dictionaries)
        """
        logger.info("Creating shifts from needs...")
        
        # Query all needs that have shifts array
        needs_filter = {"shifts": {"$exists": True, "$ne": []}}
        
        # Add future only filter if requested
        if future_only:
            logger.info(f"Generating shift status for future shifts only (after {current_time})")
            needs_filter["shifts.start"] = {"$gte": current_time}
        else:
            logger.info("Generating shift status for all shifts (past, current, and future)")
            
        # Count before query for debugging
        total_needs = self.db["needs"].count_documents({})
        needs_with_shifts = self.db["needs"].count_documents(needs_filter)
        logger.info(f"Total needs in database: {total_needs}")
        logger.info(f"Needs matching filter: {needs_with_shifts}")
        
        needs = list(self.db["needs"].find(needs_filter))
        logger.info(f"Found {len(needs)} needs with shifts to process")
        
        # If no needs with shifts were found, check alternative fields
        if len(needs) == 0:
            logger.warning("No needs with 'shifts' field found. Checking for alternative fields...")
            # Try looking for needs with date fields that could represent shifts
            alt_filter = {
                "need_date_start": {"$exists": True},
                "need_date_end": {"$exists": True}
            }
            alt_needs_count = self.db["needs"].count_documents(alt_filter)
            logger.info(f"Found {alt_needs_count} needs with date fields")
            
            if alt_needs_count > 0:
                # Use these needs instead
                needs = list(self.db["needs"].find(alt_filter))
                logger.info(f"Using {len(needs)} needs with date fields")
        
        # Create a list to store the processed shifts
        shift_status_list = []
        
        # Process each need and its shifts
        for need in needs:
            try:
                need_id = need.get("id")
                
                if not need_id:
                    logger.warning(f"Need missing ID, skipping: {need}")
                    continue
                
                # Process each shift in the need
                shifts = need.get("shifts", [])
                logger.debug(f"Processing {len(shifts)} shifts for need {need_id}: {need.get('need_title')}")
                
                # Special case for problematic need IDs
                problematic_need_ids = [800197]
                is_problematic_need = need_id in problematic_need_ids
                
                if is_problematic_need and (not shifts or len(shifts) == 0):
                    # Handle special cases
                    synthetic_shifts = self._create_synthetic_shifts_for_need(need_id)
                    if synthetic_shifts:
                        shifts = synthetic_shifts
                        logger.info(f"Using {len(shifts)} synthetic shifts for need {need_id}")
                
                for shift_index, shift in enumerate(shifts):
                    try:
                        # Add additional debug logging
                        logger.debug(f"  Processing shift {shift_index+1}/{len(shifts)}: {shift}")
                        
                        # Safely get shift ID
                        shift_id = shift.get("id") if shift else None
                        if not shift_id:
                            logger.warning(f"Shift missing ID in need {need_id}, skipping: {shift}")
                            continue
                        
                        # Create the basic shift status entry
                        shift_status = {
                            "id": shift_id,
                            "start": shift.get("start"),
                            "end": shift.get("end"),
                            "duration": shift.get("duration") or need.get("need_hours"),
                            "slots": shift.get("slots") or 0,
                            "need_id": need_id,
                            "title": need.get("need_title"),
                            "users": [],  # Will be populated later
                            "slots_filled": 0,  # Will be calculated later
                            "_synced_at": datetime.datetime.utcnow(),
                            "_sync_source": "aggregation"
                        }
                        
                        # Add to our collection of shift statuses
                        shift_status_list.append(shift_status)
                    except Exception as e:
                        logger.error(f"Error processing shift {shift_index if 'shift_index' in locals() else '?'} for need {need_id}: {str(e)}")
            except Exception as e:
                logger.error(f"Error processing need {need.get('id')}: {str(e)}")
                
        logger.info(f"Created {len(shift_status_list)} shift records from needs")
        return shift_status_list
        
    def _create_synthetic_shifts_for_need(self, need_id: int) -> list:
        """
        Create synthetic shifts for a need that doesn't have a shifts array.
        
        This function looks at hours data to create synthetic shifts when the
        standard shifts data is not available.
        
        Args:
            need_id: The ID of the need to create synthetic shifts for
            
        Returns:
            List of synthetic shift dictionaries
        """
        logger.info(f"Special handling for known problematic need ID: {need_id}")
        synthetic_shifts = []
        
        try:
            # Find all hours for this need to create synthetic shifts
            hours = list(self.db["hours"].find({"need.id": need_id}))
            if hours:
                logger.info(f"Found {len(hours)} hours for problematic need {need_id}. Creating synthetic shifts.")
                
                # Group hours by day to create one shift per day
                hours_by_day = {}
                
                for hour in hours:
                    # Get the date portion only to group by day
                    hour_start = hour.get("hour_date_start") or hour.get("date_start")
                    
                    if not hour_start:
                        continue
                        
                    # Convert to date for grouping if it's a datetime
                    if isinstance(hour_start, datetime.datetime):
                        day_key = hour_start.strftime("%Y-%m-%d")
                    else:
                        # If not a datetime, try to extract date portion
                        if isinstance(hour_start, str):
                            day_key = hour_start.split(' ')[0]
                        else:
                            # Skip if we can't determine the day
                            continue
                            
                    if day_key not in hours_by_day:
                        hours_by_day[day_key] = []
                        
                    hours_by_day[day_key].append(hour)
                
                # Create synthetic shifts for each day
                for day, day_hours in hours_by_day.items():
                    # Find earliest start and latest end for the day
                    min_start = None
                    max_end = None
                    
                    for hour in day_hours:
                        hour_start = hour.get("hour_date_start") or hour.get("date_start")
                        hour_end = hour.get("hour_date_end") or hour.get("date_end")
                        
                        if hour_start and (min_start is None or hour_start < min_start):
                            min_start = hour_start
                            
                        if hour_end and (max_end is None or hour_end > max_end):
                            max_end = hour_end
                    
                    if min_start and max_end:
                        # Create a synthetic shift for this day using first hour's ID as shift ID
                        synthetic_shift = {
                            "id": day_hours[0].get("id") if day_hours else f"synthetic_{day}",
                            "start": min_start,
                            "end": max_end,
                            "duration": sum(float(h.get("hour_duration") or 0) for h in day_hours) / len(day_hours),
                            "slots": len(day_hours)
                        }
                        synthetic_shifts.append(synthetic_shift)
                        logger.info(f"Created synthetic shift for {day} with {len(day_hours)} hours")
            else:
                logger.warning(f"No hours found for problematic need {need_id}")
        except Exception as e:
            logger.error(f"Error creating synthetic shifts for need {need_id}: {str(e)}")
            
        return synthetic_shifts

    def _generate_time_based_activity(self) -> None:
        """
        Generate time-based activity reports.
        
        Creates collections with time-based metrics including:
        - Monthly activity summary
        - Day of week patterns
        - Hour of day patterns
        """
        logger.info("Generating time-based activity reports...")
        
        try:
            # Check if the hours collection exists and has data
            if "hours" not in self.db.list_collection_names() or self.db["hours"].count_documents({}) == 0:
                logger.warning("No data available in hours collection. Skipping time-based activity reports generation.")
                return
                
            # 1. Monthly activity summary
            monthly_pipeline = [
                # Match only approved hours
                {"$match": {"hour_status": "Approved"}},
                
                # Extract year and month from date
                {"$addFields": {
                    "year_month": {"$substr": ["$hour_date_start", 0, 7]}  # YYYY-MM format
                }},
                
                # Group by year and month
                {"$group": {
                    "_id": "$year_month",
                    "total_hours": {"$sum": "$hour_duration"},
                    "volunteer_count": {"$addToSet": "$user.id"},
                    "opportunity_count": {"$addToSet": "$need.id"},
                    "agency_count": {"$addToSet": "$need.agency_id"},
                    "shifts_count": {"$sum": 1}
                }},
                
                # Add calculated fields
                {"$addFields": {
                    "volunteer_count": {"$size": "$volunteer_count"},
                    "opportunity_count": {"$size": "$opportunity_count"},
                    "agency_count": {"$size": "$agency_count"},
                    "avg_hours_per_volunteer": {"$divide": ["$total_hours", {"$size": "$volunteer_count"}]},
                    "avg_shift_duration": {"$divide": ["$total_hours", "$shifts_count"]}
                }},
                
                # Sort by year and month
                {"$sort": {"_id": 1}},
                
                # Add metadata
                {"$addFields": {
                    "_synced_at": datetime.datetime.utcnow(),
                    "_sync_source": "aggregation"
                }}
            ]
            
            # Run the monthly aggregation and store results
            monthly_result = list(self.db["hours"].aggregate(monthly_pipeline, allowDiskUse=True))
            
            # Clear the existing collection
            self.db["monthly_activity"].delete_many({})
            
            # Insert the aggregation results
            if monthly_result:
                self.db["monthly_activity"].insert_many(monthly_result)
                logger.info(f"Monthly activity report generated successfully with {len(monthly_result)} records")
            else:
                logger.warning("No data available for monthly activity report after aggregation")
            
            # 2. Day of week activity patterns
            # This would require date processing which is complex in MongoDB aggregation
            # Consider implementing this in a separate method if needed
            
        except Exception as e:
            logger.error(f"Error generating time-based activity reports: {str(e)}")
            raise

    def _generate_user_activity_summary(self) -> None:
        """
        Generate a collection of user activity summary data.
        
        This method creates an aggregated collection that summarizes each volunteer's
        activity, including total hours logged, shifts attended, and participation patterns.
        """
        logger.info("Generating user activity summary collection...")
        
        try:
            # Check if the necessary collections exist
            required_collections = ["hours", "users"]
            missing_collections = [coll for coll in required_collections if coll not in self.db.list_collection_names()]
            
            if missing_collections:
                logger.warning(f"Missing required collections: {missing_collections}. Skipping user activity summary generation.")
                return
                
            # Build the aggregation pipeline for user activity
            pipeline = [
                # Match only approved hours
                {"$match": {"hour_status": "Approved"}},
                
                # Group by user
                {"$group": {
                    "_id": "$user.id",
                    "user_info": {"$first": "$user"},
                    "total_hours": {"$sum": {"$toDouble": "$hour_duration"}},
                    "shifts_attended": {"$sum": 1},
                    "opportunities": {"$addToSet": "$need.id"},
                    "first_activity": {"$min": "$hour_date_start"},
                    "last_activity": {"$max": "$hour_date_start"},
                    "all_hours": {"$push": {
                        "need_id": "$need.id", 
                        "need_title": "$need.title", 
                        "hour_date_start": "$hour_date_start",
                        "hour_duration": "$hour_duration",
                        "hour_status": "$hour_status"
                    }}
                }},
                
                # Add calculated fields
                {"$addFields": {
                    "opportunity_count": {"$size": "$opportunities"},
                    "avg_hours_per_shift": {"$divide": ["$total_hours", "$shifts_attended"]},
                    "days_since_last_activity": {
                        "$divide": [
                            {"$subtract": [datetime.datetime.utcnow(), "$last_activity"]},
                            24 * 60 * 60 * 1000  # Convert milliseconds to days
                        ]
                    }
                }},
                
                # Add metadata
                {"$addFields": {
                    "_synced_at": datetime.datetime.utcnow(),
                    "_sync_source": "aggregation"
                }}
            ]
            
            # Run the aggregation and store results
            result = list(self.db["hours"].aggregate(pipeline, allowDiskUse=True))
            
            # Clear the existing collection
            self.db["user_activity_summary"].delete_many({})
            
            if result:
                # Insert the aggregation results
                self.db["user_activity_summary"].insert_many(result)
                logger.info(f"User activity summary collection generated successfully with {len(result)} records")
            else:
                logger.warning("No data available for user activity summary collection")
                
            # Create useful indexes for the collection
            self.db["user_activity_summary"].create_index([("total_hours", -1)])
            self.db["user_activity_summary"].create_index([("shifts_attended", -1)])
            self.db["user_activity_summary"].create_index([("last_activity", -1)])
            self.db["user_activity_summary"].create_index([("days_since_last_activity", 1)])
            self.db["user_activity_summary"].create_index([("user_info.user_email", 1)])
            
            # Update sync metadata to track when this was last generated
            self._update_sync_metadata("user_activity_summary")
            
        except Exception as e:
            logger.error(f"Error generating user activity summary collection: {str(e)}")
            raise
            
    def _generate_opportunity_activity(self) -> None:
        """
        Generate a collection of opportunity activity data.
        
        This method creates an aggregated collection that analyzes participation
        and hours for each volunteer opportunity.
        """
        logger.info("Generating opportunity activity collection...")
        
        try:
            # Check if the necessary collections exist
            required_collections = ["hours", "needs"]
            missing_collections = [coll for coll in required_collections if coll not in self.db.list_collection_names()]
            
            if missing_collections:
                logger.warning(f"Missing required collections: {missing_collections}. Skipping opportunity activity generation.")
                return
                
            # Build the aggregation pipeline for opportunity activity
            pipeline = [
                # Match only approved hours
                {"$match": {"hour_status": "Approved"}},
                
                # Group by need/opportunity
                {"$group": {
                    "_id": "$need.id",
                    "need_info": {"$first": "$need"},
                    "agency_id": {"$first": "$need.agency_id"},
                    "total_hours": {"$sum": {"$toDouble": "$hour_duration"}},
                    "volunteer_count": {"$addToSet": "$user.id"},
                    "first_activity": {"$min": "$hour_date_start"},
                    "last_activity": {"$max": "$hour_date_start"},
                    "shifts_count": {"$sum": 1},
                    "hours_by_month": {
                        "$push": {
                            "date": "$hour_date_start",
                            "duration": "$hour_duration"
                        }
                    }
                }},
                
                # Add calculated fields
                {"$addFields": {
                    "volunteer_count": {"$size": "$volunteer_count"},
                    "avg_hours_per_volunteer": {"$divide": ["$total_hours", {"$size": "$volunteer_count"}]},
                    "avg_shift_duration": {"$divide": ["$total_hours", "$shifts_count"]},
                    "days_since_last_activity": {
                        "$divide": [
                            {"$subtract": [datetime.datetime.utcnow(), "$last_activity"]},
                            24 * 60 * 60 * 1000  # Convert milliseconds to days
                        ]
                    }
                }},
                
                # Add metadata
                {"$addFields": {
                    "_synced_at": datetime.datetime.utcnow(),
                    "_sync_source": "aggregation"
                }}
            ]
            
            # Run the aggregation and store results
            result = list(self.db["hours"].aggregate(pipeline, allowDiskUse=True))
            
            # Clear the existing collection
            self.db["opportunity_activity"].delete_many({})
            
            if result:
                # Insert the aggregation results
                self.db["opportunity_activity"].insert_many(result)
                logger.info(f"Opportunity activity collection generated successfully with {len(result)} records")
            else:
                logger.warning("No data available for opportunity activity collection")
                
            # Create useful indexes for the collection
            self.db["opportunity_activity"].create_index([("total_hours", -1)])
            self.db["opportunity_activity"].create_index([("volunteer_count", -1)])
            self.db["opportunity_activity"].create_index([("last_activity", -1)])
            self.db["opportunity_activity"].create_index([("agency_id", 1)])
            
            # Update sync metadata to track when this was last generated
            self._update_sync_metadata("opportunity_activity")
            
        except Exception as e:
            logger.error(f"Error generating opportunity activity collection: {str(e)}")
            raise
            
    def _generate_agency_activity(self) -> None:
        """
        Generate a collection of agency activity data.
        
        This method creates an aggregated collection that analyzes volunteer
        engagement metrics for each agency.
        """
        logger.info("Generating agency activity collection...")
        
        try:
            # Check if the necessary collections exist
            required_collections = ["hours", "agencies"]
            missing_collections = [coll for coll in required_collections if coll not in self.db.list_collection_names()]
            
            if missing_collections:
                logger.warning(f"Missing required collections: {missing_collections}. Skipping agency activity generation.")
                return
                
            # Build the aggregation pipeline for agency activity
            pipeline = [
                # Match only approved hours
                {"$match": {"hour_status": "Approved"}},
                
                # Group by agency
                {"$group": {
                    "_id": "$need.agency_id",
                    "agency_name": {"$first": "$need.agency_name"},
                    "total_hours": {"$sum": {"$toDouble": "$hour_duration"}},
                    "volunteer_count": {"$addToSet": "$user.id"},
                    "opportunity_count": {"$addToSet": "$need.id"},
                    "first_activity": {"$min": "$hour_date_start"},
                    "last_activity": {"$max": "$hour_date_start"},
                    "opportunities": {
                        "$push": {
                            "need_id": "$need.id",
                            "need_title": "$need.title",
                            "hours": "$hour_duration"
                        }
                    }
                }},
                
                # Add calculated fields
                {"$addFields": {
                    "volunteer_count": {"$size": "$volunteer_count"},
                    "opportunity_count": {"$size": "$opportunity_count"},
                    "avg_hours_per_volunteer": {"$divide": ["$total_hours", {"$size": "$volunteer_count"}]},
                    "days_since_last_activity": {
                        "$divide": [
                            {"$subtract": [datetime.datetime.utcnow(), "$last_activity"]},
                            24 * 60 * 60 * 1000  # Convert milliseconds to days
                        ]
                    }
                }},
                
                # Look up additional agency information
                {"$lookup": {
                    "from": "agencies",
                    "localField": "_id",
                    "foreignField": "id",
                    "as": "agency_info"
                }},
                
                {"$addFields": {
                    "agency_info": {"$arrayElemAt": ["$agency_info", 0]}
                }},
                
                # Add metadata
                {"$addFields": {
                    "_synced_at": datetime.datetime.utcnow(),
                    "_sync_source": "aggregation"
                }}
            ]
            
            # Run the aggregation and store results
            result = list(self.db["hours"].aggregate(pipeline, allowDiskUse=True))
            
            # Clear the existing collection
            self.db["agency_activity"].delete_many({})
            
            if result:
                # Insert the aggregation results
                self.db["agency_activity"].insert_many(result)
                logger.info(f"Agency activity collection generated successfully with {len(result)} records")
            else:
                logger.warning("No data available for agency activity collection")
                
            # Create useful indexes for the collection
            self.db["agency_activity"].create_index([("total_hours", -1)])
            self.db["agency_activity"].create_index([("volunteer_count", -1)])
            self.db["agency_activity"].create_index([("opportunity_count", -1)])
            self.db["agency_activity"].create_index([("agency_name", 1)])
            
            # Update sync metadata to track when this was last generated
            self._update_sync_metadata("agency_activity")
            
        except Exception as e:
            logger.error(f"Error generating agency activity collection: {str(e)}")
            raise

    def validate_and_repair_data_types(self) -> None:
        """
        Validate and repair data types in existing MongoDB collections.
        
        This method scans collections and fixes any type inconsistencies.
        """
        logger.info("Validating and repairing data types in MongoDB collections")
        
        # Collections to check
        collections_to_check = [
            "agencies", "users", "needs", "events", "hours", "responses"
        ]
        
        for collection_name in collections_to_check:
            if collection_name not in self.db.list_collection_names():
                logger.info(f"Collection {collection_name} does not exist, skipping")
                continue
            
            collection = self.db[collection_name]
            count = collection.count_documents({})
            
            if count == 0:
                logger.info(f"Collection {collection_name} is empty, skipping")
                continue
            
            logger.info(f"Checking {count} documents in {collection_name}")
            
            # Process in batches to avoid memory issues
            batch_size = 100
            processed = 0
            fixed = 0
            
            for skip in range(0, count, batch_size):
                batch = list(collection.find().skip(skip).limit(batch_size))
                
                for document in batch:
                    original_id = document.get("_id")
                    
                    # Skip the _id field as it's immutable
                    if "_id" in document:
                        del document["_id"]
                    
                    # Normalize document types
                    normalized = self._normalize_document_types(document)
                    
                    # Check if anything changed
                    if normalized != document:
                        try:
                            # Update the document with normalized types
                            # Use individual field updates to avoid issues with complex objects
                            update_ops = {}
                            
                            for key, value in normalized.items():
                                # Handle nested fields by using dot notation
                                self._build_update_ops(update_ops, key, value)
                            
                            if update_ops:
                                collection.update_one(
                                    {"_id": original_id},
                                    {"$set": update_ops}
                                )
                                fixed += 1
                                
                        except Exception as e:
                            logger.error(f"Error updating document {original_id} in {collection_name}: {str(e)}")
                    
                    processed += 1
                    
                    if processed % 100 == 0:
                        logger.info(f"Processed {processed}/{count} documents in {collection_name}, fixed {fixed}")
            
            logger.info(f"Completed validation of {collection_name}: processed {processed}, fixed {fixed} documents")

    def _build_update_ops(self, update_ops: Dict, key: str, value: Any, prefix: str = "") -> None:
        """
        Build update operations for MongoDB, handling nested objects.
        
        Args:
            update_ops: Dictionary to store update operations
            key: Current field key
            value: Current field value
            prefix: Prefix for nested fields
        """
        field_name = f"{prefix}{key}" if prefix else key
        
        # Handle nested dictionaries
        if isinstance(value, dict):
            for k, v in value.items():
                self._build_update_ops(update_ops, k, v, f"{field_name}.")
        # Handle lists with special care
        elif isinstance(value, list):
            # For lists, we need to update the entire list at once
            # Check if any item in the list is a date that needs conversion
            needs_conversion = False
            for i, item in enumerate(value):
                if isinstance(item, datetime.date) and not isinstance(item, datetime.datetime):
                    needs_conversion = True
                    break
                
            if needs_conversion:
                # Convert all items in the list
                converted_list = []
                for item in value:
                    if isinstance(item, dict):
                        # Recursively normalize nested dictionaries
                        converted_list.append(self._normalize_document_types(item))
                    else:
                        # Normalize scalar values
                        converted_list.append(self._normalize_value_type(item))
                update_ops[field_name] = converted_list
            else:
                # No conversion needed, use the list as is
                update_ops[field_name] = value
        else:
            # Convert date objects to datetime
            if isinstance(value, datetime.date) and not isinstance(value, datetime.datetime):
                update_ops[field_name] = datetime.datetime.combine(value, datetime.time.min)
            else:
                update_ops[field_name] = value

    def _add_geocoding_to_document(self, document: Dict) -> Dict:
        """
        Add geocoding information to a document if address fields are present.
        
        Args:
            document: Document to add geocoding information to
            
        Returns:
            Document with geocoding information added
        """
        # Skip if geocoding is not enabled
        if not self.enable_geocoding:
            return document
            
        # Look for address fields
        address_fields = ['address', 'city', 'state', 'postal', 'country']
        prefix = ''
        
        # Determine the prefix for address fields based on document type
        if 'agency_name' in document:
            prefix = 'agency_'
        elif 'need_title' in document:
            prefix = 'need_'
        elif 'event_title' in document:
            prefix = 'event_'
            
        # Check if document already has coordinates
        lat_field = f"{prefix}latitude"
        lng_field = f"{prefix}longitude"
        
        # If coordinates already exist, no need to geocode
        if document.get(lat_field) and document.get(lng_field):
            return document
            
        # Since actual geocoding implementation would require additional dependencies,
        # we'll just log a message for now
        logger.debug(f"Geocoding not performed for document (id: {document.get('id', 'unknown')})")
        
        return document

    def _assign_users_from_responses(self, shift_status_list: list) -> None:
        """
        Assign users to shifts based on their response records.
        
        This function finds all responses for each shift and adds the user
        to the shift's user list with a pending status.
        
        Args:
            shift_status_list: List of shift status records to update
        """
        logger.info("Assigning users to shifts based on responses...")
        
        # Process each shift
        for shift in shift_status_list:
            try:
                need_id = shift.get("need_id")
                shift_id = shift.get("id")
                
                if not need_id or not shift_id:
                    logger.warning(f"Shift missing need_id or id, skipping: {shift}")
                    continue
                
                # Find all responses for this shift
                try:
                    responses = list(self.db["responses"].find({"need.id": need_id, "shift.id": shift_id}))
                    logger.debug(f"Found {len(responses)} responses for shift {shift_id}")
                except Exception as e:
                    logger.error(f"Error fetching responses for need {need_id}: {str(e)} on shift {shift_id}")
                    responses = []
                
                # Process each response and add the user to the shift
                for response in responses:
                    if not response:
                        continue
                        
                    # Get user info from response
                    user_obj = response.get("user", {})
                    user_id = user_obj.get("id") if user_obj else None
                    
                    if not user_id:
                        logger.warning(f"Response missing user ID, skipping: {response}")
                        continue
                    
                    # Get response status
                    response_status = response.get("response_status") or response.get("status")
                    
                    # Determine initial checkin status based on response
                    if response_status and response_status.lower() == "active":
                        checkin_status = "pending"
                    elif response_status and response_status.lower() == "inactive":
                        checkin_status = "cancelled"
                    else:
                        # Default is absent until we process hours
                        checkin_status = "absent"
                    
                    # Create user entry
                    user_entry = {
                        "id": user_id,
                        "domain_id": user_obj.get("domain_id"),
                        "user_fname": user_obj.get("user_fname"),
                        "user_lname": user_obj.get("user_lname"),
                        "user_email": user_obj.get("user_email"),
                        "checkin_status": checkin_status
                    }
                    
                    # Add user to shift's users list (avoiding duplicates)
                    existing_user = next((u for u in shift["users"] if u.get("id") == user_id), None)
                    if existing_user:
                        # Update existing user if status would change
                        if checkin_status != "absent" and existing_user.get("checkin_status") == "absent":
                            existing_user["checkin_status"] = checkin_status
                    else:
                        shift["users"].append(user_entry)
                        
            except Exception as e:
                logger.error(f"Error assigning users from responses for shift {shift.get('id')}: {str(e)}")
                
        logger.info(f"Assigned users to {len(shift_status_list)} shifts based on responses")

    def _correlate_hours_to_shifts(self, shift_status_list: list) -> None:
        """
        Correlate hours records to shifts and update user status.
        
        This function finds all hours that match each shift's need_id and
        correlates them to the correct user, updating the user's status
        and adding hour details.
        
        Args:
            shift_status_list: List of shift status records to update
        """
        logger.info("Correlating hours to shifts and updating user status...")
        
        # Build an index of shifts by need_id for faster lookup
        shifts_by_need = {}
        for shift in shift_status_list:
            need_id = shift.get("need_id")
            if need_id:
                if need_id not in shifts_by_need:
                    shifts_by_need[need_id] = []
                shifts_by_need[need_id].append(shift)
        
        # Process each need
        for need_id, shifts in shifts_by_need.items():
            try:
                # Find all hours for this need
                hours = list(self.db["hours"].find({"need.id": need_id}))
                logger.debug(f"Found {len(hours)} hours for need {need_id}")
                
                # Process each hour
                for hour in hours:
                    if not hour:
                        continue
                        
                    # Get user info
                    user_obj = hour.get("user", {})
                    user_id = user_obj.get("id") if user_obj else None
                    
                    if not user_id:
                        logger.warning(f"Hour missing user ID, skipping: {hour}")
                        continue
                    
                    # Get hour details
                    hour_start = hour.get("hour_date_start") or hour.get("date_start")
                    hour_end = hour.get("hour_date_end") or hour.get("date_end")
                    hour_shift = hour.get("shift", {})
                    hour_shift_id = hour_shift.get("id") if hour_shift else None
                    
                    # Find matching shift(s) for this hour
                    matching_shifts = []
                    
                    # First try direct shift ID match if available
                    if hour_shift_id:
                        direct_matches = [s for s in shifts if s.get("id") == hour_shift_id]
                        if direct_matches:
                            matching_shifts.extend(direct_matches)
                            logger.debug(f"Found direct shift ID match for hour {hour.get('id')} and shift {hour_shift_id}")
                    
                    # If no direct match, try time-based matching
                    if not matching_shifts and hour_start and hour_end:
                        for shift in shifts:
                            shift_start = shift.get("start")
                            shift_end = shift.get("end")
                            
                            if not shift_start or not shift_end:
                                continue
                            
                            # Check for time overlap
                            # Case 1: Hours fall exactly within shift - exact match
                            exact_match = (hour_start == shift_start and hour_end == shift_end)
                            
                            # Case 2: Hours are a subset of shift - contained within
                            within_shift = False
                            if shift_start and shift_end:
                                within_shift = hour_start >= shift_start and hour_end <= shift_end
                                
                            # Case 3: Hours overlap with shift - partial overlap
                            overlaps_shift = False
                            if shift_start and shift_end:
                                # Hour starts during shift
                                starts_during = hour_start >= shift_start and hour_start < shift_end
                                # Hour ends during shift
                                ends_during = hour_end > shift_start and hour_end <= shift_end
                                # Hour spans the entire shift
                                contains_shift = hour_start <= shift_start and hour_end >= shift_end
                                
                                overlaps_shift = starts_during or ends_during or contains_shift
                            
                            # Case 4: Same date match when time precision is limited
                            same_date_match = False
                            if shift_start and hour_start:
                                # Convert to date objects for comparison if they're datetime objects
                                shift_date = shift_start.date() if hasattr(shift_start, 'date') else None
                                hour_date = hour_start.date() if hasattr(hour_start, 'date') else None
                                
                                if shift_date and hour_date and shift_date == hour_date:
                                    # If on the same day, consider it a match if the times are close (within 1 hour)
                                    if isinstance(shift_start, datetime.datetime) and isinstance(hour_start, datetime.datetime):
                                        time_diff = abs((hour_start - shift_start).total_seconds()) / 3600
                                        if time_diff <= 1:
                                            same_date_match = True
                                    else:
                                        # If we only have dates (not times), then same day = match
                                        same_date_match = True
                            
                            # If any type of match is found, add to matching shifts
                            if exact_match or within_shift or overlaps_shift or same_date_match:
                                matching_shifts.append(shift)
                    
                    # If we found matching shifts, update the user status
                    if matching_shifts:
                        for shift in matching_shifts:
                            # Find user in shift users list or add if not exists
                            user_entry = next((u for u in shift["users"] if u.get("id") == user_id), None)
                            
                            if not user_entry:
                                # User not found in shift, create entry from hour data
                                user_entry = {
                                    "id": user_id,
                                    "domain_id": user_obj.get("domain_id"),
                                    "user_fname": user_obj.get("user_fname"),
                                    "user_lname": user_obj.get("user_lname"),
                                    "user_email": user_obj.get("user_email"),
                                    "checkin_status": "absent"  # Will be updated below
                                }
                                shift["users"].append(user_entry)
                            
                            # Determine status based on hour
                            hour_status = hour.get("hour_status") or hour.get("status")
                            hour_created = hour.get("hour_date_created") or hour.get("created_at")
                            hour_updated = hour.get("hour_date_updated") or hour.get("updated_at")
                            hour_duration = hour.get("hour_duration") or hour.get("hour_hours") or hour.get("duration")
                            hour_source = hour.get("hour_source") or ""
                            # Determine the user's check-in status based on hour data
                            if hour_status and (hour_status.lower() == "denied" or 
                                              "denied" in hour_status.lower() or
                                              hour_status.lower() == "deny" or
                                              "reject" in hour_status.lower()):
                                checkin_status = "cancelled"
                            elif hour_status and (hour_status.lower() == "approved" or 
                                                hour_status.lower() == "a" or
                                                hour_status.lower() == "approve" or
                                                "approve" in hour_status.lower()):
                                checkin_status = "completed"
                            elif hour_duration and float(hour_duration or 0) > 0:
                                checkin_status = "completed"
                            elif hour_created and hour_updated and hour_created != hour_updated:
                                checkin_status = "completed"
                            else:
                                checkin_status = "active"
                            
                            # Analyze check-in/check-out patterns from hour_source
                            has_checkin = False
                            has_checkout = False
                            has_manager_approval = False
                            has_kiosk_activity = False
                            
                            if hour_source:
                                # Check for check-in activity
                                if "checkin" in hour_source.lower() or "storecheckin" in hour_source.lower():
                                    has_checkin = True
                                
                                # Check for check-out activity
                                if "checkout" in hour_source.lower() or "storecheckout" in hour_source.lower():
                                    has_checkout = True
                                
                                # Check for manager approval
                                manager_patterns = ["/manager/hours/", "/admin/", "manager", "admin", "approved", "approve"]
                                if any(pattern.lower() in hour_source.lower() for pattern in manager_patterns):
                                    has_manager_approval = True
                                
                                # Check for kiosk activity
                                if "/kiosk/" in hour_source.lower():
                                    has_kiosk_activity = True
                            
                            # Determine checkout status for pending hours
                            checkout_status = "unknown"
                            if hour_status and hour_status.lower() == "pending":
                                if has_checkin and has_checkout:
                                    checkout_status = "checked_in_and_out"
                                elif has_checkin and not has_checkout:
                                    checkout_status = "checked_in_only"
                                elif has_manager_approval:
                                    checkout_status = "manager_approved"
                                else:
                                    checkout_status = "no_checkin_activity"
                            
                            # Update user entry with hour information
                            user_entry.update({
                                "checkin_status": checkin_status,
                                "hour_id": hour.get("id"),
                                "hour_status": hour_status,
                                "hour_source": hour.get("hour_source") or hour.get("source") or "",
                                "hour_duration": hour_duration,
                                "hour_date_start": hour_start,
                                "hour_date_end": hour_end,
                                "hour_date_created": hour_created,
                                "hour_date_updated": hour_updated,
                                "hour_source": hour_source,
                                "checkout_status": checkout_status,
                                "has_checkin": has_checkin,
                                "has_checkout": has_checkout,
                                "has_manager_approval": has_manager_approval,
                                "has_kiosk_activity": has_kiosk_activity,
                                "checkout_analysis": {
                                    "checked_in": has_checkin,
                                    "checked_out": has_checkout,
                                    "manager_approval": has_manager_approval,
                                    "kiosk_activity": has_kiosk_activity,
                                    "status": checkout_status
                                }
                            })
                            
                            # Add duration as float for easier reporting
                            if hour_duration:
                                try:
                                    user_entry["duration"] = float(hour_duration)
                                except (ValueError, TypeError):
                                    # If conversion fails, try to calculate from start/end times
                                    if hour_start and hour_end and isinstance(hour_start, datetime.datetime) and isinstance(hour_end, datetime.datetime):
                                        duration_seconds = (hour_end - hour_start).total_seconds()
                                        user_entry["duration"] = duration_seconds / 3600.0  # Convert to hours
                            else:
                                # Try to calculate duration from start/end times
                                if hour_start and hour_end and isinstance(hour_start, datetime.datetime) and isinstance(hour_end, datetime.datetime):
                                    duration_seconds = (hour_end - hour_start).total_seconds()
                                    user_entry["duration"] = duration_seconds / 3600.0  # Convert to hours
                    else:
                        logger.debug(f"No matching shifts found for hour {hour.get('id')} for user {user_id}")
                
            except Exception as e:
                logger.error(f"Error correlating hours for need {need_id}: {str(e)}")
        
        # Calculate slots_filled for each shift
        for shift in shift_status_list:
            # Count non-cancelled slots
            shift["slots_filled"] = sum(1 for user in shift["users"] if user.get("checkin_status") != "cancelled")
            
            # Update slots if not set
            if not shift["slots"] or shift["slots"] == 0:
                shift["slots"] = len(shift["users"])
        
        logger.info(f"Updated user status in {len(shift_status_list)} shifts based on hours")

    def _save_shift_status_data(self, shift_status_list: list) -> None:
        """
        Save the shift status data to MongoDB.
        
        This function handles both inserting new shifts and updating existing ones,
        with proper error handling and statistics tracking.
        
        Args:
            shift_status_list: List of shift status records to save
        """
        logger.info(f"Saving {len(shift_status_list)} shift records to MongoDB...")
        
        if not shift_status_list:
            logger.warning("No data available for shift status collection")
            return
            
        # Check if we should clear existing data first
        fresh_data = self.config.get("fresh_shift_data", False)
        if fresh_data:
            logger.info("Performing a fresh shift status generation - clearing existing data first")
            self.db["shift_status"].delete_many({})
            
        # Process each shift individually for better error handling and duplication prevention
        processed_count = 0
        updated_count = 0
        inserted_count = 0
        error_count = 0
        
        for shift in shift_status_list:
            try:
                # Ensure each shift has a unique _id based on its id field
                shift_id = shift.get("id")
                if not shift_id:
                    logger.warning(f"Skipping shift without ID: {shift.get('title')}")
                    error_count += 1
                    continue
                    
                # Use the shift_id as MongoDB _id to avoid duplicates
                shift["_id"] = shift_id
                
                # Check if this shift already exists
                existing = self.db["shift_status"].find_one({"_id": shift_id})
                
                if existing:
                    # Update existing shift
                    update_data = {k: v for k, v in shift.items() if k != "_id"}
                    result = self.db["shift_status"].update_one(
                        {"_id": shift_id},
                        {"$set": update_data}
                    )
                    if result.modified_count > 0:
                        updated_count += 1
                else:
                    # Insert new shift
                    result = self.db["shift_status"].insert_one(shift)
                    if result.inserted_id:
                        inserted_count += 1
                        
                processed_count += 1
                
                # Log progress periodically
                if processed_count % 100 == 0:
                    logger.info(f"Processed {processed_count}/{len(shift_status_list)} shifts")
                    
            except Exception as e:
                error_count += 1
                logger.error(f"Error processing shift {shift.get('id')}: {str(e)}")
                
        logger.info(f"Shift status collection generated successfully: {processed_count} processed, {updated_count} updated, {inserted_count} inserted, {error_count} errors")
        
        # Process synthetic shifts for approved hours not linked to shifts
        self._process_synthetic_shifts_for_approved_hours()
    
    def _process_synthetic_shifts_for_approved_hours(self) -> None:
        """
        Create synthetic shifts for approved hours not linked to regular shifts.
        
        This function finds users with approved hours not properly linked to shifts
        and creates synthetic shifts for them to ensure all approved hours are tracked.
        """
        try:
            logger.info("Looking for users with approved hours not properly linked to shifts...")
            
            # Find users with approved hours
            hour_pipeline = [
                # Match only approved hours where both need.id and user.id exist
                {"$match": {
                    "hour_status": "approved", 
                    "need.id": {"$exists": True},
                    "user.id": {"$exists": True}
                }},
                {"$group": {
                    "_id": {"need_id": "$need.id", "user_id": "$user.id"},
                    "user_info": {"$first": "$user"},
                    "need_info": {"$first": "$need"},
                    "hours": {"$push": "$$ROOT"},
                    "min_start": {"$min": "$hour_date_start"},
                    "max_end": {"$max": "$hour_date_end"}
                }}
            ]
            
            need_user_hours = list(self.db["hours"].aggregate(hour_pipeline))
            logger.info(f"Found {len(need_user_hours)} need-user combinations with approved hours")
            
            # Track which ones need synthetic shifts
            synthetic_shifts = []
            
            for combo in need_user_hours:
                try:
                    # Safely get values with validation
                    if not combo or not isinstance(combo, dict):
                        logger.warning(f"Skipping invalid combo record: {combo}")
                        continue
                        
                    id_data = combo.get("_id", {})
                    if not id_data or not isinstance(id_data, dict):
                        logger.warning(f"Skipping combo with invalid _id: {combo}")
                        continue
                        
                    need_id = id_data.get("need_id")
                    user_id = id_data.get("user_id")
                    
                    if not need_id or not user_id:
                        logger.warning(f"Skipping combo with missing need_id or user_id: {id_data}")
                        continue
                        
                    logger.debug(f"Processing need_id={need_id}, user_id={user_id}")
                    
                    # Skip if already completed for this need
                    try:
                        query = {"need_id": need_id, "users.id": user_id, "users.checkin_status": "completed"}
                        if self.db["shift_status"].count_documents(query) > 0:
                            logger.debug(f"Skipping synthetic shift: user {user_id} already marked as completed for need {need_id}")
                            continue
                    except Exception as e:
                        logger.warning(f"Error checking if user {user_id} is completed for need {need_id}: {str(e)}")
                        continue
                        
                    # Build synthetic shift for this user
                    user_info = combo.get("user_info") or {}
                    need_info = combo.get("need_info") or {}
                    hours_list = combo.get("hours") or []
                    
                    if not hours_list or len(hours_list) == 0:
                        logger.debug(f"Skipping synthetic shift: no hours found for user {user_id} and need {need_id}")
                        continue
                        
                    # Get hour details
                    hour_id = hours_list[0].get("id")
                    start_time = combo.get("min_start")
                    end_time = combo.get("max_end")
                    
                    if not hour_id or not start_time or not end_time:
                        logger.debug(f"Skipping synthetic shift: missing hour_id, start_time, or end_time for user {user_id} and need {need_id}")
                        continue
                        
                    # Calculate total duration
                    total_duration = 0
                    for hour in hours_list:
                        try:
                            # Try different field names for duration
                            hour_duration = (
                                hour.get("hour_duration") or
                                hour.get("hour_hours") or
                                hour.get("duration") or
                                "0"
                            )
                            
                            # Convert to float and add to total
                            total_duration += float(hour_duration)
                        except (ValueError, TypeError) as e:
                            logger.debug(f"Error parsing duration '{hour.get('hour_duration')}' for user {user_id}: {str(e)}")
                    
                    # Use a default if total is 0
                    if total_duration == 0:
                        total_duration = 2.0
                        
                    # Create user entry with completed status
                    user_entry = {
                        "id": user_id,
                        "domain_id": user_info.get("domain_id", 0),
                        "user_fname": user_info.get("user_fname", "Unknown"),
                        "user_lname": user_info.get("user_lname", "User"),
                        "user_email": user_info.get("user_email", f"user_{user_id}@example.com"),
                        "checkin_status": "completed",
                        "hour_id": hour_id,
                        "hour_status": "approved",
                        "hour_duration": str(total_duration),
                        "hour_date_start": start_time,
                        "hour_date_end": end_time,
                        "checkout_status": "manager_approved",
                        "has_checkin": True,  # Assume approved hours had check-in
                        "has_checkout": True,  # Assume approved hours had check-out
                        "has_manager_approval": True,  # These are approved hours
                        "has_kiosk_activity": False,  # Unknown for synthetic
                        "checkout_analysis": {
                            "checked_in": True,
                            "checked_out": True,
                            "manager_approval": True,
                            "kiosk_activity": False,
                            "status": "manager_approved"
                        }
                    }
                    
                    # Create synthetic shift
                    shift_id = f"syn_{need_id}_{user_id}_{hour_id}"
                    shift_title = need_info.get("title") 
                    if not shift_title:
                        # Fallback to need title from the need collection
                        try:
                            need_doc = self.db["needs"].find_one({"id": need_id})
                            if need_doc:
                                shift_title = need_doc.get("need_title") or f"Need {need_id}"
                            else:
                                shift_title = f"Need {need_id}"
                        except Exception:
                            shift_title = f"Need {need_id}"
                    
                    # Create the shift object
                    shift = {
                        "_id": shift_id,
                        "id": shift_id,
                        "start": start_time,
                        "end": end_time,
                        "duration": str(total_duration),
                        "slots": 1,
                        "need_id": need_id,
                        "title": shift_title,
                        "users": [user_entry],
                        "slots_filled": 1,
                        "_synced_at": datetime.datetime.utcnow(),
                        "_sync_source": "synthetic"
                    }
                    
                    synthetic_shifts.append(shift)
                    
                except Exception as e:
                    logger.error(f"Error processing need-user combination: {str(e)}")
            
            # Upsert the synthetic shifts
            if synthetic_shifts:
                logger.info(f"Upserting {len(synthetic_shifts)} synthetic shifts for users with approved hours")
                
                # Track statistics
                updated_count = 0
                inserted_count = 0
                error_count = 0
                
                # Process each shift individually
                for shift in synthetic_shifts:
                    try:
                        # Check if this shift already exists
                        existing = self.db["shift_status"].find_one({"_id": shift["_id"]})
                        
                        if existing:
                            # Update existing shift
                            update_data = {k: v for k, v in shift.items() if k != "_id"}
                            result = self.db["shift_status"].update_one(
                                {"_id": shift["_id"]},
                                {"$set": update_data}
                            )
                            if result.modified_count > 0:
                                updated_count += 1
                        else:
                            # Insert new shift
                            result = self.db["shift_status"].insert_one(shift)
                            if result.inserted_id:
                                inserted_count += 1
                    except Exception as e:
                        error_count += 1
                        logger.error(f"Failed to upsert synthetic shift: {str(e)}")
                
                logger.info(f"Synthetic shifts processed: {len(synthetic_shifts)} total, {updated_count} updated, {inserted_count} inserted, {error_count} errors")
        
        except Exception as e:
            logger.error(f"Error adding synthetic shifts: {str(e)}")

    def _generate_checkin_checkout_analysis(self) -> None:
        """
        Generate a collection analyzing check-in/check-out patterns from hour_source field.
        
        This method analyzes the hour_source field to identify users who have:
        - Checked in (via kiosk or other means)
        - May or may not have checked out
        - Have NOT received manager approval yet
        - And currently have pending hour_status
        
        This helps identify:
        1. Users who checked in but forgot to check out (need follow-up)
        2. Users who checked in and out but need manager approval
        
        The analysis uses regex patterns to parse the hour_source field which contains
        information like: "Added at: /kiosk/storeCheckin/ by user 7533085" or
        "Added at: /kiosk/storeCheckin/ by user 7533085 Updated at: /kiosk/storeCheckout/ by user 7533085"
        """
        logger.info("Generating check-in/check-out analysis collection...")
        
        try:
            # Check if the hours collection exists and has data
            if "hours" not in self.db.list_collection_names() or self.db["hours"].count_documents({}) == 0:
                logger.warning("No data available in hours collection. Skipping check-in/check-out analysis.")
                return
            
            # Build the aggregation pipeline
            pipeline = [
                # Match only pending hours that have hour_source data
                {"$match": {
                    "hour_status": {"$regex": "pending", "$options": "i"},
                    "hour_source": {"$exists": True, "$ne": "", "$ne": None}
                }},
                
                # Add fields to analyze the hour_source
                {"$addFields": {
                    "has_checkin": {
                        "$regexMatch": {
                            "input": "$hour_source",
                            "regex": "(checkin|storeCheckin)",
                            "options": "i"
                        }
                    },
                    "has_checkout": {
                        "$regexMatch": {
                            "input": "$hour_source",
                            "regex": "(checkout|storeCheckout)",
                            "options": "i"
                        }
                    },
                    "has_manager_approval": {
                        "$regexMatch": {
                            "input": "$hour_source",
                            "regex": "(/manager/hours/|/admin/|manager|admin|approved|approve)",
                            "options": "i"
                        }
                    },
                    "has_kiosk_activity": {
                        "$regexMatch": {
                            "input": "$hour_source",
                            "regex": "/kiosk/",
                            "options": "i"
                        }
                    }
                }},
                
                # Filter for users who have checked in (regardless of checkout status) but no manager approval
                {"$match": {
                    "$and": [
                        {"has_checkin": True},
                        {"has_manager_approval": False}  # No manager approval yet
                    ]
                }},
                
                # Project the fields we want to keep
                {"$project": {
                    "_id": "$id",
                    "hour_id": "$id",
                    "user_info": {
                        "id": "$user.id",
                        "domain_id": "$user.domain_id",
                        "user_fname": "$user.user_fname",
                        "user_lname": "$user.user_lname",
                        "user_email": "$user.user_email"
                    },
                    "need_info": {
                        "id": "$need.id",
                        "title": "$need.need_title",

                    },
                    "hour_date_start": 1,
                    "hour_date_end": 1,
                    "hour_duration": 1,
                    "hour_status": 1,
                    "hour_source": 1,
                    "has_checkin": 1,
                    "has_checkout": 1,
                    "has_manager_approval": 1,
                    "has_kiosk_activity": 1,
                    "analysis_summary": {
                        "$concat": [
                            "Checked in: ", {"$toString": "$has_checkin"},
                            ", Checked out: ", {"$toString": "$has_checkout"},
                            ", Manager approval: ", {"$toString": "$has_manager_approval"},
                            ", Kiosk activity: ", {"$toString": "$has_kiosk_activity"}
                        ]
                    }
                }},
                
                # Sort by user and then by hour start date
                {"$sort": {
                    "user_info.user_lname": 1,
                    "user_info.user_fname": 1,
                    "hour_date_start": 1
                }},
                
                # Add metadata
                {"$addFields": {
                    "_synced_at": datetime.datetime.utcnow(),
                    "_sync_source": "aggregation"
                }}
            ]
            
            # Run the aggregation and store results
            result = list(self.db["hours"].aggregate(pipeline, allowDiskUse=True))
            
            # Clear the existing collection
            self.db["checkin_checkout_analysis"].delete_many({})
            
            if result:
                # Insert the aggregation results
                self.db["checkin_checkout_analysis"].insert_many(result)
                logger.info(f"Check-in/check-out analysis collection generated successfully with {len(result)} records")
                
                # Log some statistics
                checkin_only = sum(1 for r in result if r.get("has_checkin") and not r.get("has_checkout"))
                checkin_checkout = sum(1 for r in result if r.get("has_checkin") and r.get("has_checkout"))
                manager_approved = sum(1 for r in result if r.get("has_manager_approval"))
                
                logger.info(f"Analysis summary: {checkin_only} check-in only, {checkin_checkout} check-in + check-out, {manager_approved} with manager approval")
            else:
                logger.warning("No pending hours found with check-in/check-out patterns")
                
            # Create useful indexes for the collection
            self.db["checkin_checkout_analysis"].create_index([("user_info.id", 1)])
            self.db["checkin_checkout_analysis"].create_index([("need_info.id", 1)])
            self.db["checkin_checkout_analysis"].create_index([("hour_date_start", 1)])
            self.db["checkin_checkout_analysis"].create_index([("has_checkin", 1)])
            self.db["checkin_checkout_analysis"].create_index([("has_checkout", 1)])
            self.db["checkin_checkout_analysis"].create_index([("has_manager_approval", 1)])
            
            # Update sync metadata to track when this was last generated
            self._update_sync_metadata("checkin_checkout_analysis")
            
        except Exception as e:
            logger.error(f"Error generating check-in/check-out analysis collection: {str(e)}")
            raise

if __name__ == "__main__":
    # Create and run the sync tool
    try:
        sync_tool = GalaxyAPISync()
        
        # Check if we should validate and repair data types
        if os.getenv("VALIDATE_DATA_TYPES", "").lower() == "true":
            logger.info("Running data type validation and repair")
            sync_tool.validate_and_repair_data_types()
            sys.exit(0)
        
        # Check if we should run a specific report
        specific_report = os.getenv("GENERATE_SPECIFIC_REPORT", "")
        if specific_report:
            logger.info(f"Running specific report: {specific_report}")
            # Ensure database is synced before generating report
            logger.info("Syncing database before generating report...")
            sync_tool.sync_all_resources()
            sync_tool.generate_specific_report(specific_report)
            sys.exit(0)
        
        # Check if we should run aggregations only
        if os.getenv("GENERATE_REPORTS", "").lower() == "true":
            logger.info("Running aggregations only")
            # Ensure database is synced before generating reports
            logger.info("Syncing database before generating reports...")
            sync_tool.sync_all_resources()
            sync_tool.generate_activity_reports()
            sys.exit(0)
        
        # Check if we should run a one-time sync
        if os.getenv("SYNC_ONCE", "").lower() == "true":
            sync_tool.sync_all_resources()
            
            # Generate reports after sync if requested
            if os.getenv("INCLUDE_REPORTS", "").lower() == "true":
                sync_tool.generate_activity_reports()
        else:
            # Run scheduled sync every hour by default, or as specified in config
            interval = int(os.getenv("SYNC_INTERVAL_MINUTES", "60"))
            sync_tool.run_scheduled_sync(interval)
            
    except Exception as e:
        logger.critical(f"Failed to initialize or run sync tool: {str(e)}")
