#!/usr/bin/env python3
"""
A simple utility script to query and analyze data synced from Galaxy Digital API
to MongoDB.
"""

import sys
import argparse
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any
from pymongo import MongoClient
from pymongo.collection import Collection
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

def format_datetime(dt):
    """Format datetime objects for printing"""
    if isinstance(dt, datetime):
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    return dt

def pretty_print(obj):
    """Print an object with nice formatting"""
    if isinstance(obj, dict):
        for key, value in obj.items():
            if isinstance(value, (dict, list)):
                print(f"{key}:")
                pretty_print(value)
            else:
                print(f"{key}: {format_datetime(value)}")
    elif isinstance(obj, list):
        for i, item in enumerate(obj):
            print(f"[{i}]:")
            pretty_print(item)
    else:
        print(obj)

def get_db_connection():
    """Get a MongoDB database connection"""
    mongo_uri = os.getenv("MONGODB_URI", "mongodb://localhost:27017/")
    db_name = os.getenv("MONGODB_DATABASE", "galaxy_digital")
    client = MongoClient(mongo_uri)
    return client[db_name]

def list_collections(db):
    """List all collections in the database"""
    collections = db.list_collection_names()
    collections.sort()
    print(f"Available collections:")
    for coll in collections:
        count = db[coll].count_documents({})
        print(f"  - {coll} ({count} documents)")

def show_sync_status(db):
    """Show the sync status for each resource"""
    print("Sync Status:")
    print("-" * 70)
    print(f"{'Resource':<20} {'Last Sync':<20} {'Documents':<10}")
    print("-" * 70)
    
    for metadata in db["sync_metadata"].find().sort("resource"):
        resource = metadata.get("resource", "unknown")
        last_sync = metadata.get("last_sync", "never")
        if isinstance(last_sync, datetime):
            last_sync = last_sync.strftime("%Y-%m-%d %H:%M:%S")
        doc_count = db[resource].count_documents({})
        print(f"{resource:<20} {last_sync:<20} {doc_count:<10}")

def query_collection(db, collection_name, query=None, limit=10):
    """Query a collection and print results"""
    if query is None:
        query = {}
    else:
        # Try to parse as JSON if it's a string
        if isinstance(query, str):
            try:
                query = json.loads(query)
            except json.JSONDecodeError:
                print(f"Error: Invalid query format: {query}")
                return
    
    collection = db[collection_name]
    results = list(collection.find(query).limit(limit))
    
    if not results:
        print(f"No results found in {collection_name} for query: {query}")
        return
    
    print(f"Found {len(results)} results in {collection_name}:")
    for i, result in enumerate(results):
        print(f"\nResult {i+1}:")
        pretty_print(result)

def analyze_data(db, analysis_type):
    """Run different analysis on the synced data"""
    if analysis_type == "agencies":
        # Count agencies by status
        pipeline = [
            {"$group": {"_id": "$agency_status", "count": {"$sum": 1}}},
            {"$sort": {"_id": 1}}
        ]
        results = list(db["agencies"].aggregate(pipeline))
        print("Agency Count by Status:")
        for result in results:
            print(f"  {result['_id']}: {result['count']}")
            
    elif analysis_type == "needs":
        # Count needs by status
        pipeline = [
            {"$group": {"_id": "$need_status", "count": {"$sum": 1}}},
            {"$sort": {"_id": 1}}
        ]
        results = list(db["needs"].aggregate(pipeline))
        print("Needs Count by Status:")
        for result in results:
            print(f"  {result['_id']}: {result['count']}")
            
        # Count needs by agency
        pipeline = [
            {"$group": {"_id": "$agency.id", "agency_name": {"$first": "$agency.agency_name"}, "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 10}
        ]
        results = list(db["needs"].aggregate(pipeline))
        print("\nTop 10 Agencies by Need Count:")
        for result in results:
            print(f"  {result['agency_name']}: {result['count']}")
            
    elif analysis_type == "users":
        # Count users by status
        pipeline = [
            {"$group": {"_id": "$user_status", "count": {"$sum": 1}}},
            {"$sort": {"_id": 1}}
        ]
        results = list(db["users"].aggregate(pipeline))
        print("User Count by Status:")
        for result in results:
            print(f"  {result['_id']}: {result['count']}")
            
    elif analysis_type == "hours":
        # Sum hours by month
        pipeline = [
            {"$match": {"hour_date_start": {"$type": "date"}}},
            {"$project": {
                "year_month": {"$dateToString": {"format": "%Y-%m", "date": "$hour_date_start"}},
                "hours": {"$toDouble": "$hour_hours"}
            }},
            {"$group": {"_id": "$year_month", "total_hours": {"$sum": "$hours"}}},
            {"$sort": {"_id": 1}}
        ]
        results = list(db["hours"].aggregate(pipeline))
        print("Monthly Hours:")
        for result in results:
            print(f"  {result['_id']}: {result['total_hours']:.1f}")
            
    else:
        print(f"Unknown analysis type: {analysis_type}")
        print("Available analysis types: agencies, needs, users, hours")

def main():
    parser = argparse.ArgumentParser(description="Query Galaxy Digital data synced to MongoDB")
    parser.add_argument("--status", action="store_true", help="Show sync status")
    parser.add_argument("--list", action="store_true", help="List available collections")
    parser.add_argument("--collection", "-c", help="Collection to query")
    parser.add_argument("--query", "-q", help="Query to run (JSON format)")
    parser.add_argument("--limit", "-l", type=int, default=10, help="Limit the number of results")
    parser.add_argument("--analyze", "-a", help="Run analysis (options: agencies, needs, users, hours)")
    
    args = parser.parse_args()
    
    db = get_db_connection()
    
    if args.status:
        show_sync_status(db)
    elif args.list:
        list_collections(db)
    elif args.collection:
        query_collection(db, args.collection, args.query, args.limit)
    elif args.analyze:
        analyze_data(db, args.analyze)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
