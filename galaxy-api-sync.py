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
        
        for attempt in range(max_retries):
            try:
                logger.debug(f"Making API request to {url} (attempt {attempt+1}/{max_retries})")
                response = self.session.request('GET', url, headers=self.headers, params=params)
                
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
                    self.headers["Authorization"] = f"Bearer {self.token}"
                    continue
                
                # Check for server errors (5xx)
                if 500 <= response.status_code < 600:
                    if attempt < max_retries - 1:
                        wait_time = retry_delay * (attempt + 1)  # Exponential backoff
                        logger.warning(f"Server error {response.status_code}. Retrying in {wait_time} seconds...")
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
        
        # This should not be reached, but just in case
        raise requests.exceptions.RequestException(f"Failed to make API request to {url} after {max_retries} attempts")
    
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
        query_params = params.copy() if params else {}
        
        # Add a since parameter if specified and we have a last sync time
        if since_field:
            last_sync = self._get_last_sync_time(resource_name)
            if last_sync:
                query_params[since_field] = last_sync.strftime("%Y-%m-%d %H:%M")
                logger.info(f"Incremental sync using {since_field} since {last_sync}")
        
        # Add pagination parameters
        query_params.setdefault("per_page", 100)
        
        # Initialize counters for total items
        total_successful_items = 0
        total_failed_items = 0
        current_page = 1
        has_more_pages = True
        
        # Process all pages
        while has_more_pages:
            try:
                # Set the current page
                query_params["page"] = current_page
                
                logger.info(f"Fetching page {current_page} for {resource_name}")
                
                # Make the API request
                response = self._make_api_request(resource_name, query_params)
                
                # Check if we have data
                if "data" not in response:
                    logger.warning(f"No data found in response for {resource_name} on page {current_page}")
                    break
                
                # Get the items for this page
                items = response["data"]
                
                # If we got fewer items than per_page, this is the last page
                per_page = int(query_params.get("per_page", 100))
                if len(items) < per_page:
                    has_more_pages = False
                    logger.info(f"Reached last page ({current_page}) for {resource_name}")
                
                # Process each item with MongoDB error handling
                page_successful_items = 0
                page_failed_items = 0
                
                for item in items:
                    try:
                        self._update_document(collection, item)
                        page_successful_items += 1
                    except pymongo.errors.PyMongoError as e:
                        page_failed_items += 1
                        logger.error(f"MongoDB error while updating item {item.get('id', 'unknown')} for {resource_name}: {str(e)}")
                
                # Update totals
                total_successful_items += page_successful_items
                total_failed_items += page_failed_items
                
                logger.info(f"Synced {page_successful_items} items for {resource_name} on page {current_page} (failed: {page_failed_items})")
                
                # Check if we need to continue to the next page
                if len(items) == 0:
                    has_more_pages = False
                    logger.info(f"No more items for {resource_name}")
                else:
                    # Move to the next page
                    current_page += 1
                
            except requests.exceptions.RequestException as e:
                logger.error(f"API request error for {resource_name} on page {current_page}: {str(e)}")
                raise
            except pymongo.errors.PyMongoError as e:
                logger.error(f"MongoDB error for {resource_name} on page {current_page}: {str(e)}")
                raise
            except Exception as e:
                logger.error(f"Error syncing {resource_name} on page {current_page}: {str(e)}")
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
            
            logger.info("Successfully generated all activity reports")
            
        except Exception as e:
            logger.error(f"Error generating activity reports: {str(e)}")
            raise
    
    def generate_specific_report(self, report_type: str) -> None:
        """
        Generate a specific activity report.
        
        Args:
            report_type: Type of report to generate ('user', 'needs', 'opportunity', 'agency', 'time')
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
            else:
                logger.warning(f"Unknown report type: {report_type}")
                raise ValueError(f"Unknown report type: {report_type}. Valid options are: user, needs, opportunity, agency, time")
                
            logger.info(f"Successfully generated {report_type} report")
            
        except Exception as e:
            logger.error(f"Error generating {report_type} report: {str(e)}")
            raise
    
    def _generate_user_activity_summary(self) -> None:
        """
        Generate a summary of activity for each user.
        
        Creates a collection with user activity metrics including:
        - Total hours logged
        - Number of shifts attended
        - List of opportunities participated in
        - First and last activity dates
        - Average hours per shift
        """
        logger.info("Generating user activity summary...")
        
        try:
            # Check if the hours collection exists and has data
            if "hours" not in self.db.list_collection_names() or self.db["hours"].count_documents({}) == 0:
                logger.warning("No data available in hours collection. Skipping user activity summary generation.")
                return
                
            # Define the aggregation pipeline
            pipeline = [
                # Match only approved hours
                {"$match": {"hour_status": "Approved"}},
                
                # Group by user ID
                {"$group": {
                    "_id": "$user.id",
                    "user_info": {"$first": "$user"},
                    "total_hours": {"$sum": "$hour_duration"},
                    "shifts_attended": {"$sum": 1},
                    "opportunities": {"$addToSet": "$need.id"},
                    "first_activity": {"$min": "$hour_date_start"},
                    "last_activity": {"$max": "$hour_date_start"},
                    "all_hours": {"$push": {
                        "date": "$hour_date_start",
                        "duration": "$hour_duration",
                        "need_id": "$need.id",
                        "need_title": "$need.title"
                    }}
                }},
                
                # Add calculated fields
                {"$addFields": {
                    "avg_hours_per_shift": {"$divide": ["$total_hours", "$shifts_attended"]},
                    "days_since_last_activity": {
                        "$divide": [
                            {"$subtract": [datetime.datetime.utcnow(), "$last_activity"]},
                            1000 * 60 * 60 * 24  # Convert ms to days
                        ]
                    },
                    "opportunity_count": {"$size": "$opportunities"}
                }},
                
                # Sort by total hours (descending)
                {"$sort": {"total_hours": -1}},
                
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
            
            # Insert the aggregation results
            if result:
                self.db["user_activity_summary"].insert_many(result)
                logger.info(f"User activity summary generated successfully with {len(result)} records")
            else:
                logger.warning("No data available for user activity summary after aggregation")
                
        except Exception as e:
            logger.error(f"Error generating user activity summary: {str(e)}")
            raise
    
    def _generate_opportunity_activity(self) -> None:
        """
        Generate activity metrics for each opportunity.
        
        Creates a collection with opportunity metrics including:
        - Total hours logged
        - Number of unique volunteers
        - Average hours per volunteer
        - Most recent activity
        """
        logger.info("Generating opportunity activity metrics...")
        
        try:
            # Check if the hours collection exists and has data
            if "hours" not in self.db.list_collection_names() or self.db["hours"].count_documents({}) == 0:
                logger.warning("No data available in hours collection. Skipping opportunity activity metrics generation.")
                return
                
            # Define the aggregation pipeline
            pipeline = [
                # Match only approved hours
                {"$match": {"hour_status": "Approved"}},
                
                # Group by opportunity (need) ID
                {"$group": {
                    "_id": "$need.id",
                    "need_info": {"$first": "$need"},
                    "agency_id": {"$first": "$need.agency_id"},
                    "total_hours": {"$sum": "$hour_duration"},
                    "volunteer_count": {"$addToSet": "$user.id"},
                    "first_activity": {"$min": "$hour_date_start"},
                    "last_activity": {"$max": "$hour_date_start"},
                    "shifts_count": {"$sum": 1},
                    "hours_by_month": {
                        "$push": {
                            "month": {"$substr": ["$hour_date_start", 0, 7]},  # YYYY-MM format
                            "hours": "$hour_duration"
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
                            1000 * 60 * 60 * 24  # Convert ms to days
                        ]
                    }
                }},
                
                # Sort by total hours (descending)
                {"$sort": {"total_hours": -1}},
                
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
            
            # Insert the aggregation results
            if result:
                self.db["opportunity_activity"].insert_many(result)
                logger.info(f"Opportunity activity metrics generated successfully with {len(result)} records")
            else:
                logger.warning("No data available for opportunity activity metrics after aggregation")
                
        except Exception as e:
            logger.error(f"Error generating opportunity activity metrics: {str(e)}")
            raise
    
    def _generate_agency_activity(self) -> None:
        """
        Generate activity metrics for each agency.
        
        Creates a collection with agency metrics including:
        - Total volunteer hours
        - Number of unique volunteers
        - Number of opportunities
        - Most active opportunities
        """
        logger.info("Generating agency activity metrics...")
        
        try:
            # Check if the hours collection exists and has data
            if "hours" not in self.db.list_collection_names() or self.db["hours"].count_documents({}) == 0:
                logger.warning("No data available in hours collection. Skipping agency activity metrics generation.")
                return
                
            # Define the aggregation pipeline
            pipeline = [
                # Match only approved hours
                {"$match": {"hour_status": "Approved"}},
                
                # Lookup agency information
                {"$lookup": {
                    "from": "agencies",
                    "localField": "need.agency_id",
                    "foreignField": "id",
                    "as": "agency"
                }},
                
                # Unwind the agency array
                {"$unwind": {"path": "$agency", "preserveNullAndEmptyArrays": True}},
                
                # Group by agency ID
                {"$group": {
                    "_id": "$agency.id",
                    "agency_name": {"$first": "$agency.agency_name"},
                    "agency_info": {"$first": "$agency"},
                    "total_hours": {"$sum": "$hour_duration"},
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
                            1000 * 60 * 60 * 24  # Convert ms to days
                        ]
                    }
                }},
                
                # Sort by total hours (descending)
                {"$sort": {"total_hours": -1}},
                
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
            
            # Insert the aggregation results
            if result:
                self.db["agency_activity"].insert_many(result)
                logger.info(f"Agency activity metrics generated successfully with {len(result)} records")
            else:
                logger.warning("No data available for agency activity metrics after aggregation")
                
        except Exception as e:
            logger.error(f"Error generating agency activity metrics: {str(e)}")
            raise
    
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

if __name__ == "__main__":
    # Create and run the sync tool
    try:
        sync_tool = GalaxyAPISync()
        
        # Check if we should run a specific report
        specific_report = os.getenv("GENERATE_SPECIFIC_REPORT", "")
        if specific_report:
            logger.info(f"Running specific report: {specific_report}")
            sync_tool.generate_specific_report(specific_report)
            sys.exit(0)
        
        # Check if we should run aggregations only
        if os.getenv("GENERATE_REPORTS", "").lower() == "true":
            logger.info("Running aggregations only")
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
