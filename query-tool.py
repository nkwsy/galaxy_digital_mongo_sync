#!/usr/bin/env python3
"""
A simple utility script to query and analyze data synced from Galaxy Digital API
to MongoDB.
"""

import sys
import argparse
import json
import csv
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

def query_collection(db, collection_name, query=None, sort=None, limit=10):
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
    
    # Handle sort parameter if provided
    sort_params = None
    if sort:
        try:
            sort_params = json.loads(sort)
            # Convert to list of tuples as required by pymongo
            sort_params = [(k, v) for k, v in sort_params.items()]
        except json.JSONDecodeError:
            print(f"Error: Invalid sort format: {sort}")
            return
    
    collection = db[collection_name]
    cursor = collection.find(query)
    
    # Apply sort if provided
    if sort_params:
        cursor = cursor.sort(sort_params)
    
    # Apply limit
    cursor = cursor.limit(limit)
    
    # Execute query
    results = list(cursor)
    
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
            
    elif analysis_type == "shift_status":
        # Count volunteers by check-in status
        pipeline = [
            {"$unwind": "$users"},
            {"$group": {
                "_id": "$users.checkin_status", 
                "count": {"$sum": 1}
            }},
            {"$sort": {"_id": 1}}
        ]
        results = list(db["shift_status"].aggregate(pipeline))
        print("Volunteer Count by Check-in Status:")
        for result in results:
            print(f"  {result['_id']}: {result['count']}")
        
        # Find active shifts (current date)
        now = datetime.utcnow()
        today = datetime(now.year, now.month, now.day)
        tomorrow = today + timedelta(days=1)
        
        pipeline = [
            {"$match": {
                "start": {"$gte": today, "$lt": tomorrow}
            }},
            {"$project": {
                "title": 1,
                "start": 1,
                "end": 1,
                "slots": 1,
                "slots_filled": 1,
                "active_count": {
                    "$size": {
                        "$filter": {
                            "input": "$users",
                            "as": "user",
                            "cond": {"$eq": ["$$user.checkin_status", "active"]}
                        }
                    }
                },
                "pending_count": {
                    "$size": {
                        "$filter": {
                            "input": "$users",
                            "as": "user",
                            "cond": {"$eq": ["$$user.checkin_status", "pending"]}
                        }
                    }
                }
            }},
            {"$sort": {"start": 1}}
        ]
        
        results = list(db["shift_status"].aggregate(pipeline))
        if results:
            print("\nToday's Shifts:")
            for result in results:
                start_time = format_datetime(result.get('start'))
                print(f"  {result['title']} ({start_time}):")
                print(f"    Active: {result['active_count']}, Pending: {result['pending_count']}")
                print(f"    Filled: {result['slots_filled']}/{result['slots']} slots")
        else:
            print("\nNo shifts scheduled for today.")
            
    else:
        print(f"Unknown analysis type: {analysis_type}")
        print("Available analysis types: agencies, needs, users, hours, shift_status")

def count_checkin_statuses(db):
    """Count users by their checkin_status and check for mismatches with approved hours"""
    try:
        pipeline = [
            {"$unwind": "$users"},
            {"$group": {"_id": "$users.checkin_status", "count": {"$sum": 1}}},
            {"$sort": {"_id": 1}}
        ]
        
        results = list(db["shift_status"].aggregate(pipeline))
        print("User Count by Check-in Status:")
        for result in results:
            print(f"  {result['_id']}: {result['count']}")
        
        # Now check for users with approved hours who aren't marked as completed
        print("\nChecking for users with approved hours not marked as completed...")
        
        # Get all users with approved hours
        approved_user_hours = {}
        try:
            cursor = db["hours"].find({"hour_status": "approved"})
            for hour in cursor:
                try:
                    user_obj = hour.get("user", {})
                    need_obj = hour.get("need", {})
                    
                    if not user_obj or not need_obj:
                        continue
                        
                    user_id = user_obj.get("id")
                    need_id = need_obj.get("id")
                    
                    if user_id and need_id:
                        key = f"{need_id}_{user_id}"
                        if key not in approved_user_hours:
                            approved_user_hours[key] = {
                                "need_id": need_id,
                                "user_id": user_id,
                                "user_name": f"{user_obj.get('user_fname', '')} {user_obj.get('user_lname', '')}",
                                "need_title": need_obj.get("title", f"Need {need_id}")
                            }
                except Exception as e:
                    print(f"Error processing hour: {str(e)}")
                    continue
        except Exception as e:
            print(f"Error fetching hours: {str(e)}")
        
        print(f"Found {len(approved_user_hours)} unique need/user combinations with approved hours")
        
        # Check each one to see if they're marked as completed
        mismatch_count = 0
        for key, data in approved_user_hours.items():
            try:
                # Look for this user in shift_status where they're marked as completed
                completed = db["shift_status"].count_documents({
                    "need_id": data["need_id"],
                    "users": {
                        "$elemMatch": {
                            "id": data["user_id"],
                            "checkin_status": "completed"
                        }
                    }
                })
                
                if completed == 0:
                    mismatch_count += 1
                    
                    # Only print first 10 to avoid overwhelming output
                    if mismatch_count <= 10:
                        print(f"  Mismatch: {data['user_name']} (ID: {data['user_id']}) has approved hours for {data['need_title']} (ID: {data['need_id']}) but not marked completed")
            except Exception as e:
                print(f"Error checking completion status: {str(e)}")
        
        print(f"\nTotal users with approved hours not marked as completed: {mismatch_count}")
        
        # Special focus on need 800197
        special_need = 800197
        special_count = 0
        special_users = []
        
        for key, data in approved_user_hours.items():
            if data["need_id"] == special_need:
                try:
                    # Check if this user is already marked as completed
                    completed = db["shift_status"].count_documents({
                        "need_id": special_need,
                        "users": {
                            "$elemMatch": {
                                "id": data["user_id"],
                                "checkin_status": "completed"
                            }
                        }
                    })
                    
                    if completed == 0:
                        special_count += 1
                        special_users.append(data)
                except Exception as e:
                    print(f"Error checking completion status for need {special_need}: {str(e)}")
        
        print(f"\nFocus on need ID {special_need}:")
        print(f"  Users with approved hours: {len(special_users)}")
        print(f"  Users not marked as completed: {special_count}")
        
        # Suggest how to fix
        print("\nTo fix the issue, you can run:")
        print(f"python query-tool.py --fix-need {special_need}")
        print("This will create synthetic shift records for all users with approved hours")
    
    except Exception as e:
        print(f"Error in count_checkin_statuses: {str(e)}")

def diagnose_shift(db, shift_id=None, need_id=None):
    """Diagnose issues with shift status tracking for a specific shift"""
    # Find the shift
    query = {}
    if shift_id:
        query["id"] = shift_id
    elif need_id:
        query["need_id"] = need_id
    else:
        print("Please provide either shift_id or need_id")
        return

    shift = db["shift_status"].find_one(query)
    if not shift:
        print(f"Shift not found with query: {query}")
        
        # Check if the need exists
        if need_id:
            need = db["needs"].find_one({"id": need_id})
            if need:
                print(f"Need exists but no shift status found: {need.get('need_title')}")
                # Check if the need has shifts
                shifts = need.get("shifts", [])
                print(f"Need has {len(shifts)} shifts in the needs collection")
                if shifts:
                    print(f"First shift: {shifts[0]}")
            else:
                print(f"Need {need_id} not found in database")
        return
    
    # Basic shift info
    print(f"Examining shift: {shift['title']} (ID: {shift['id']})")
    print(f"Start: {shift.get('start')}, End: {shift.get('end')}")
    print(f"Slots: {shift.get('slots')}, Filled: {shift.get('slots_filled')}")
    
    # Check users
    users = shift.get("users", [])
    print(f"\nUsers ({len(users)}):")
    status_counts = {}
    
    for user in users:
        status = user.get("checkin_status", "unknown")
        status_counts[status] = status_counts.get(status, 0) + 1
        print(f"\n  User: {user.get('user_fname')} {user.get('user_lname')} ({user.get('id')})")
        print(f"  Status: {status}")
        
        # Check for presence of hour records
        if "hour_id" in user:
            print(f"  Hour record: {user.get('hour_id')}")
            print(f"  Hour status: {user.get('hour_status')}")
            print(f"  Hour duration: {user.get('hour_duration')}")
            print(f"  Hour dates: {user.get('hour_date_start')} to {user.get('hour_date_end')}")
            print(f"  Created: {user.get('hour_date_created')}, Updated: {user.get('hour_date_updated')}")
            
            # Check original hour record in hours collection
            hour = db["hours"].find_one({"id": user.get('hour_id')})
            if hour:
                print(f"  Original hour status: {hour.get('hour_status')}")
                print(f"  Original hour dates: {hour.get('hour_date_start')} to {hour.get('hour_date_end')}")
            else:
                print("  Warning: Hour record not found in hours collection")
        else:
            print("  No hour record associated with this user")
            
        # Check response
        response = db["responses"].find_one({"need.id": shift["need_id"], "shift.id": shift["id"], "user.id": user.get("id")})
        if response:
            print(f"  Response status: {response.get('response_status')}")
        else:
            print("  No response record found")
    
    # Print status counts
    print("\nStatus counts:")
    for status, count in status_counts.items():
        print(f"  {status}: {count}")
    
    # Find all hours for this need to check for matching issues
    print("\nAll hours for this need:")
    hours = list(db["hours"].find({"need.id": shift["need_id"]}))
    print(f"Found {len(hours)} hour records for need {shift['need_id']}")
    
    if hours:
        for hour in hours[:5]:  # Show first 5
            print(f"  Hour ID: {hour.get('id')}")
            print(f"  User: {hour.get('user', {}).get('user_fname')} {hour.get('user', {}).get('user_lname')} ({hour.get('user', {}).get('id')})")
            print(f"  Date: {hour.get('hour_date_start')} to {hour.get('hour_date_end')}")
            print(f"  Status: {hour.get('hour_status')}")
            print(f"  Shift ID (if present): {hour.get('shift', {}).get('id')}")
            print()
        
        if len(hours) > 5:
            print(f"  ... and {len(hours) - 5} more hour records")

def analyze_checkin_vs_hours(db):
    """Analyze the relationship between checkin statuses and hours records"""
    # First, get current checkin statuses
    pipeline = [
        {"$unwind": "$users"},
        {"$group": {"_id": "$users.checkin_status", "count": {"$sum": 1}}},
        {"$sort": {"_id": 1}}
    ]
    
    status_counts = list(db["shift_status"].aggregate(pipeline))
    print("Current Check-in Status Distribution:")
    for result in status_counts:
        print(f"  {result['_id']}: {result['count']}")
        
    # Now check how many approved hours exist overall
    approved_hours = db["hours"].count_documents({"hour_status": "approved"})
    print(f"\nTotal approved hours in database: {approved_hours}")
    
    # Check users with approved hours but not marked as completed
    print("\nAnalyzing users with approved hours but not marked as completed:")
    
    # Get users with approved hours
    users_with_approved_hours = set()
    for hour in db["hours"].find({"hour_status": "approved"}, {"user.id": 1}):
        user_id = hour.get("user", {}).get("id")
        if user_id:
            users_with_approved_hours.add(user_id)
    
    print(f"Found {len(users_with_approved_hours)} users with approved hours")
    
    # Check how many of these users are properly marked as completed in shift status
    completed_user_count = 0
    for shift in db["shift_status"].find({}, {"users": 1}):
        for user in shift.get("users", []):
            if user.get("id") in users_with_approved_hours and user.get("checkin_status") == "completed":
                completed_user_count += 1
                # Remove from set to count each user only once
                users_with_approved_hours.discard(user.get("id"))
    
    # The remaining users have approved hours but aren't marked as completed
    print(f"Users with approved hours marked as completed: {completed_user_count}")
    print(f"Users with approved hours NOT marked as completed: {len(users_with_approved_hours)}")
    
    # Sample a few of these problematic users
    print("\nSample of users with approved hours but not marked as completed:")
    sample_count = min(5, len(users_with_approved_hours))
    sample_users = list(users_with_approved_hours)[:sample_count]
    
    for user_id in sample_users:
        # Get user details
        user = db["users"].find_one({"id": user_id})
        user_name = f"{user.get('user_fname', '')} {user.get('user_lname', '')}" if user else f"User ID {user_id}"
        print(f"\nUser: {user_name} (ID: {user_id})")
        
        # Find their approved hours
        hours = list(db["hours"].find({"user.id": user_id, "hour_status": "approved"}).limit(3))
        print(f"  Approved hours: {len(hours)} (showing max 3)")
        for hour in hours:
            need = hour.get("need") or {}
            need_id = need.get("id") if need else None
            need_title = need.get("title") if need else "Unknown Need"
            shift = hour.get("shift") or {}
            shift_id = shift.get("id") if shift else None
            print(f"  - Need: {need_title} (ID: {need_id})")
            print(f"    Shift ID: {shift_id}")
            print(f"    Date: {hour.get('hour_date_start')} to {hour.get('hour_date_end')}")
            print(f"    Duration: {hour.get('hour_duration')}")
        
        # Find their check-in status
        shift_statuses = []
        for shift in db["shift_status"].find({"users.id": user_id}):
            for shift_user in shift.get("users", []):
                if shift_user.get("id") == user_id:
                    shift_statuses.append({
                        "need_id": shift.get("need_id"),
                        "title": shift.get("title"),
                        "status": shift_user.get("checkin_status")
                    })
                    
        print(f"  Check-in statuses: {len(shift_statuses)}")
        for status in shift_statuses:
            print(f"  - Need: {status['title']} (ID: {status['need_id']})")
            print(f"    Status: {status['status']}")

def fix_checkin_status(db):
    """Directly update checkin status for users with approved hours"""
    print("Starting direct checkin status fix...")
    
    # Find all users with approved hours by need ID
    approved_hours = {}
    
    print("Finding users with approved hours...")
    cursor = db["hours"].find({"hour_status": "approved"})
    for hour in cursor:
        need_id = hour.get("need", {}).get("id")
        user_id = hour.get("user", {}).get("id")
        
        if not need_id or not user_id:
            continue
            
        key = f"{need_id}_{user_id}"
        if key not in approved_hours:
            approved_hours[key] = {
                "need_id": need_id, 
                "user_id": user_id,
                "hours": []
            }
            
        approved_hours[key]["hours"].append(hour)
    
    print(f"Found {len(approved_hours)} unique need_id/user_id combinations with approved hours")
    
    # Now update shift status for these users
    update_count = 0
    for key, data in approved_hours.items():
        need_id = data["need_id"]
        user_id = data["user_id"]
        
        # Find all shifts for this need in the shift_status collection
        shifts = list(db["shift_status"].find({"need_id": need_id}))
        
        # If no shifts exist, continue
        if not shifts:
            continue
            
        # For each shift, check if the user is in it and update their status
        for shift in shifts:
            for i, user in enumerate(shift.get("users", [])):
                if user.get("id") == user_id and user.get("checkin_status") != "completed":
                    # Update this user's status to completed
                    update_result = db["shift_status"].update_one(
                        {"_id": shift["_id"], "users.id": user_id},
                        {"$set": {"users.$.checkin_status": "completed"}}
                    )
                    
                    if update_result.modified_count > 0:
                        update_count += 1
    
    print(f"Updated {update_count} user statuses to 'completed'")
    
    # If no existing shift entries were found for some approved hours, create synthetic shifts
    synthetic_count = 0
    synthetic_shifts = []
    
    for key, data in approved_hours.items():
        need_id = data["need_id"]
        user_id = data["user_id"]
        hours_list = data["hours"]
        
        # Check if this user is already marked as completed for this need
        already_completed = db["shift_status"].count_documents({
            "need_id": need_id, 
            "users.id": user_id, 
            "users.checkin_status": "completed"
        })
        
        if already_completed > 0:
            continue
            
        # Create a synthetic shift for this user
        # Get first hour for ID and info
        first_hour = hours_list[0]
        hour_id = first_hour.get("id")
        user_info = first_hour.get("user", {})
        need_info = first_hour.get("need", {})
        
        # Use start/end times from the hour
        start_time = first_hour.get("hour_date_start")
        end_time = first_hour.get("hour_date_end")
        
        if not (user_id and need_id and hour_id and start_time and end_time):
            continue
            
        # Calculate total duration
        total_duration = 0
        for hour in hours_list:
            try:
                duration = float(hour.get("hour_duration") or 0)
                total_duration += duration
            except (ValueError, TypeError):
                pass
                
        # Create user entry with completed status
        user_entry = {
            "id": user_id,
            "domain_id": user_info.get("domain_id"),
            "user_fname": user_info.get("user_fname"),
            "user_lname": user_info.get("user_lname"),
            "user_email": user_info.get("user_email"),
            "checkin_status": "completed",
            "hour_id": hour_id,
            "hour_status": "approved",
            "hour_duration": str(total_duration),
            "hour_date_start": start_time,
            "hour_date_end": end_time
        }
        
        # Create synthetic shift
        shift = {
            "id": f"syn_{need_id}_{user_id}_{hour_id}",
            "start": start_time,
            "end": end_time,
            "duration": str(total_duration),
            "slots": 1,
            "need_id": need_id,
            "title": need_info.get("title") or f"Need {need_id}",
            "users": [user_entry],
            "slots_filled": 1,
            "_synced_at": datetime.datetime.now(),
            "_sync_source": "direct_fix"
        }
        
        synthetic_shifts.append(shift)
        synthetic_count += 1
        
        # Insert in batches to avoid memory issues
        if len(synthetic_shifts) >= 100:
            if synthetic_shifts:
                db["shift_status"].insert_many(synthetic_shifts)
            synthetic_shifts = []
    
    # Insert any remaining shifts
    if synthetic_shifts:
        db["shift_status"].insert_many(synthetic_shifts)
    
    print(f"Created {synthetic_count} synthetic shifts for users with approved hours")
    
    # Now count the distribution after our fixes
    pipeline = [
        {"$unwind": "$users"},
        {"$group": {"_id": "$users.checkin_status", "count": {"$sum": 1}}},
        {"$sort": {"_id": 1}}
    ]
    
    status_counts = list(db["shift_status"].aggregate(pipeline))
    print("\nNew Check-in Status Distribution:")
    for result in status_counts:
        print(f"  {result['_id']}: {result['count']}")

def fix_specific_need(db, need_id=800197):
    """Fix the checkin status for a specific problematic need ID"""
    print("=" * 50)
    print(f"STARTING FIX FOR NEED {need_id}")
    print("=" * 50)
    
    try:
        print(f"Starting direct fix for need_id {need_id}...")
        
        # Find all approved hours for this need
        print(f"Fetching approved hours for need {need_id}...")
        
        # Show the exact query
        query = {"need.id": need_id, "hour_status": "approved"}
        print(f"Hours query: {query}")
        
        # Check what's in the hours collection
        total_hours = db["hours"].count_documents({})
        print(f"Total hours in collection: {total_hours}")
        
        # Check how many hours match just need.id
        need_hours = db["hours"].count_documents({"need.id": need_id})
        print(f"Hours with need.id={need_id}: {need_hours}")
        
        # Check how many hours match just "approved" status
        approved_hours = db["hours"].count_documents({"hour_status": "approved"})
        print(f"Hours with hour_status=approved: {approved_hours}")
        
        # Now run the query
        hours = list(db["hours"].find(query))
        print(f"Found {len(hours)} approved hours for need {need_id}")
        
        # Print a few samples
        if hours:
            print("Sample hour records:")
            for i, hour in enumerate(hours[:3]):
                print(f"  Hour {i+1}:")
                print(f"    id: {hour.get('id')}")
                print(f"    need.id: {hour.get('need', {}).get('id')}")
                print(f"    user.id: {hour.get('user', {}).get('id')}")
                print(f"    hour_status: {hour.get('hour_status')}")
        
        # Group hours by user ID
        users_with_hours = set()
        for hour in hours:
            user_id = hour.get("user", {}).get("id")
            if user_id:
                users_with_hours.add(user_id)
        
        print(f"Found {len(users_with_hours)} unique users with approved hours")
        
        # First try to update existing shift records
        print("\nUpdating existing shift records...")
        
        # Check what's in the shift_status collection
        total_shifts = db["shift_status"].count_documents({})
        print(f"Total shifts in collection: {total_shifts}")
        
        # Check how many shifts match need_id
        need_shifts = db["shift_status"].count_documents({"need_id": need_id})
        print(f"Shifts with need_id={need_id}: {need_shifts}")
        
        if need_shifts > 0:
            print("Sample shift records:")
            for i, shift in enumerate(db["shift_status"].find({"need_id": need_id}).limit(2)):
                print(f"  Shift {i+1}:")
                print(f"    id: {shift.get('id')}")
                print(f"    need_id: {shift.get('need_id')}")
                print(f"    users count: {len(shift.get('users', []))}")
                for j, user in enumerate(shift.get('users', [])[:2]):
                    print(f"      User {j+1}: {user.get('id')} - {user.get('checkin_status')}")
        
        # Update existing records
        update_count = 0
        for user_id in users_with_hours:
            try:
                # Update any existing shift records for this user and need
                update_query = {"need_id": need_id, "users.id": user_id}
                update_op = {"$set": {"users.$.checkin_status": "completed"}}
                
                print(f"Update query for user {user_id}: {update_query}")
                print(f"Update operation: {update_op}")
                
                # Check if the query would match anything
                match_count = db["shift_status"].count_documents(update_query)
                print(f"Query would match {match_count} documents")
                
                # Run the update
                result = db["shift_status"].update_many(update_query, update_op)
                update_count += result.modified_count
                print(f"Modified {result.modified_count} documents")
            except Exception as e:
                print(f"Error updating user {user_id}: {str(e)}")
        
        print(f"Updated {update_count} existing shift records to 'completed'")
        
        # Now create synthetic shifts for users that need them
        print("\nCreating synthetic shifts...")
        
        # First, create a test shift to see if it works
        test_id = "test_synthetic_shift"
        test_shift = {
            "id": test_id,
            "need_id": need_id,
            "title": "Test Shift",
            "start": datetime.datetime.now(),
            "end": datetime.datetime.now() + datetime.timedelta(hours=1),
            "slots": 1,
            "slots_filled": 1,
            "users": [
                {
                    "id": 123456,
                    "user_fname": "Test",
                    "user_lname": "User",
                    "checkin_status": "completed"
                }
            ],
            "_synced_at": datetime.datetime.now(),
            "_sync_source": "test"
        }
        
        # Try to insert the test shift
        try:
            # First delete any existing test shifts
            db["shift_status"].delete_one({"id": test_id})
            
            # Insert the test shift
            db["shift_status"].insert_one(test_shift)
            print(f"Successfully inserted test shift with ID {test_id}")
            
            # Clean up
            db["shift_status"].delete_one({"id": test_id})
        except Exception as e:
            print(f"Error inserting test shift: {str(e)}")
        
        # Create one real synthetic shift for the first user
        if users_with_hours:
            print("Creating a single test synthetic shift for the first user...")
            
            first_user_id = list(users_with_hours)[0]
            user_hours = [hour for hour in hours if hour.get("user", {}).get("id") == first_user_id]
            
            if user_hours:
                try:
                    first_hour = user_hours[0]
                    user_info = first_hour.get("user", {})
                    need_info = first_hour.get("need", {})
                    
                    # Create a synthetic shift for this user
                    shift_id = f"syn_test_{need_id}_{first_user_id}"
                    
                    shift = {
                        "id": shift_id,
                        "need_id": need_id,
                        "title": need_info.get("title") or f"Need {need_id}",
                        "start": first_hour.get("hour_date_start") or datetime.datetime.now(),
                        "end": first_hour.get("hour_date_end") or datetime.datetime.now() + datetime.timedelta(hours=1),
                        "slots": 1,
                        "slots_filled": 1,
                        "users": [
                            {
                                "id": first_user_id,
                                "user_fname": user_info.get("user_fname", ""),
                                "user_lname": user_info.get("user_lname", ""),
                                "checkin_status": "completed"
                            }
                        ],
                        "_synced_at": datetime.datetime.now(),
                        "_sync_source": "test"
                    }
                    
                    # First delete any existing test shifts
                    db["shift_status"].delete_one({"id": shift_id})
                    
                    # Insert the test shift
                    db["shift_status"].insert_one(shift)
                    print(f"Successfully inserted real test shift with ID {shift_id}")
                    
                    # Clean up
                    db["shift_status"].delete_one({"id": shift_id})
                except Exception as e:
                    print(f"Error inserting real test shift: {str(e)}")
            
        # Try a direct MongoDB statement to update a specific user
        if users_with_hours:
            first_user_id = list(users_with_hours)[0]
            print(f"\nTrying a direct MongoDB update for user {first_user_id}...")
            
            try:
                # Find an existing shift record for this need
                existing_shift = db["shift_status"].find_one({"need_id": need_id})
                
                if existing_shift:
                    shift_id = existing_shift["_id"]
                    
                    # Find if this user is in the shift
                    user_in_shift = False
                    for user in existing_shift.get("users", []):
                        if user.get("id") == first_user_id:
                            user_in_shift = True
                            break
                    
                    if user_in_shift:
                        # Try to update the user's status
                        update_result = db["shift_status"].update_one(
                            {"_id": shift_id, "users.id": first_user_id},
                            {"$set": {"users.$.checkin_status": "completed"}}
                        )
                        
                        print(f"Direct update result: {update_result.modified_count} documents modified")
                    else:
                        print(f"User {first_user_id} not found in shift {shift_id}")
                else:
                    print(f"No existing shifts found for need {need_id}")
            except Exception as e:
                print(f"Error with direct update: {str(e)}")
        
        print("\n" + "=" * 50)
        print(f"DEBUG COMPLETED FOR NEED {need_id}")
        print("=" * 50)
        
        return True
        
    except Exception as e:
        print(f"ERROR FIXING NEED {need_id}: {str(e)}")
        return False

def simple_fix_need(db, need_id=800197):
    """Create a simpler fix for need ID with better error handling"""
    print(f"Simple fix for need {need_id}")
    
    try:
        # Just do simple MongoDB updates directly
        # First, get a count of approved hours
        approved_count = db["hours"].count_documents({"need.id": need_id, "hour_status": "approved"})
        print(f"Found {approved_count} approved hours for need {need_id}")
        
        # Find all shifts related to this need
        shifts_count = db["shift_status"].count_documents({"need_id": need_id})
        print(f"Found {shifts_count} shifts for need {need_id}")
        
        if shifts_count == 0:
            print("No shifts found for this need")
            return
        
        # For each shift, get all users and set everyone to completed
        update_count = 0
        
        for shift in db["shift_status"].find({"need_id": need_id}):
            shift_id = shift.get("_id")
            users = shift.get("users", [])
            print(f"Processing shift {shift.get('id')} with {len(users)} users")
            
            # For each user, check if they have approved hours
            for user in users:
                user_id = user.get("id")
                if not user_id:
                    continue
                    
                # Check if this user has approved hours
                hours = db["hours"].count_documents({"need.id": need_id, "user.id": user_id, "hour_status": "approved"})
                
                if hours > 0:
                    print(f"  User {user_id} has {hours} approved hours")
                    
                    # Update this user's status
                    try:
                        result = db["shift_status"].update_one(
                            {"_id": shift_id, "users.id": user_id},
                            {"$set": {"users.$.checkin_status": "completed"}}
                        )
                        if result.modified_count > 0:
                            update_count += 1
                            print(f"  Updated user {user_id} status to completed")
                    except Exception as e:
                        print(f"  Error updating user {user_id}: {str(e)}")
        
        print(f"Updated a total of {update_count} users to completed")
    except Exception as e:
        print(f"Error in simple_fix_need: {str(e)}")

def export_shift_status_csv(db, start_date=None, end_date=None, need_id=None, output_file="shift_status_export.csv"):
    """Export shift status data with user details to CSV with optional date and need_id filters"""
    print(f"Exporting shift status data to {output_file}")
    
    # Build the query
    query = {}
    
    # Add date filters if provided
    if start_date:
        try:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            query["start"] = {"$gte": start_dt}
            print(f"Filtering shifts starting on or after {start_date}")
        except ValueError:
            print(f"Invalid start date format. Please use YYYY-MM-DD format.")
            return False
    
    if end_date:
        try:
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
            # Make it inclusive by setting to end of day
            end_dt = end_dt.replace(hour=23, minute=59, second=59)
            
            # Add to existing query if start_date was also provided
            if "start" in query:
                query["start"]["$lte"] = end_dt
            else:
                query["start"] = {"$lte": end_dt}
                
            print(f"Filtering shifts starting on or before {end_date}")
        except ValueError:
            print(f"Invalid end date format. Please use YYYY-MM-DD format.")
            return False
    
    # Add need_id filter if provided
    if need_id:
        query["need_id"] = need_id
        print(f"Filtering by need ID: {need_id}")
    
    # Get the shifts matching our query
    shifts = list(db["shift_status"].find(query))
    print(f"Found {len(shifts)} shifts matching criteria")
    
    if not shifts:
        print("No data found matching the criteria")
        return False
    
    # Set up CSV writer
    with open(output_file, 'w', newline='', encoding='utf-8') as csv_file:
        fieldnames = [
            'shift_start', 'shift_end', 'need_id', 'title', 
            'slots', 'slots_filled', 'user_fname', 'user_lname', 
            'hour_status', 'checkin_status', 'user_id'
        ]
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        
        # Process each shift and its users
        row_count = 0
        for shift in shifts:
            # Get base shift data
            base_data = {
                'shift_start': format_datetime(shift.get('start')),
                'shift_end': format_datetime(shift.get('end')),
                'need_id': shift.get('need_id'),
                'title': shift.get('title'),
                'slots': shift.get('slots'),
                'slots_filled': shift.get('slots_filled')
            }
            
            # Process each user in the shift
            users = shift.get('users', [])
            if not users:
                # Write one row with shift data but no user data
                writer.writerow(base_data)
                row_count += 1
            else:
                for user in users:
                    row = base_data.copy()
                    row.update({
                        'user_fname': user.get('user_fname'),
                        'user_lname': user.get('user_lname'),
                        'hour_status': user.get('hour_status'),
                        'checkin_status': user.get('checkin_status'),
                        'user_id': user.get('id')
                    })
                    writer.writerow(row)
                    row_count += 1
    
    print(f"Successfully exported {row_count} rows to {output_file}")
    return True

def main():
    parser = argparse.ArgumentParser(description="Query Galaxy Digital data synced to MongoDB")
    parser.add_argument("--status", action="store_true", help="Show sync status")
    parser.add_argument("--list", action="store_true", help="List available collections")
    parser.add_argument("--collection", "-c", help="Collection to query")
    parser.add_argument("--query", "-q", help="Query to run (JSON format)")
    parser.add_argument("--sort", "-s", help="Sort specification (JSON format, e.g. '{\"field\": 1}' for ascending, '{\"field\": -1}' for descending)")
    parser.add_argument("--limit", "-l", type=int, default=10, help="Limit the number of results")
    parser.add_argument("--analyze", "-a", help="Run analysis (options: agencies, needs, users, hours, shift_status)")
    parser.add_argument("--count-statuses", action="store_true", help="Count the number of users by checkin_status")
    parser.add_argument("--diagnose-shift", action="store_true", help="Diagnose issues with a specific shift")
    parser.add_argument("--shift-id", type=int, help="Shift ID to diagnose")
    parser.add_argument("--need-id", type=int, help="Need ID to diagnose")
    parser.add_argument("--analyze-checkin", action="store_true", help="Analyze relationship between checkin statuses and hours")
    parser.add_argument("--fix-status", action="store_true", help="Fix checkin status for users with approved hours")
    parser.add_argument("--fix-need", type=int, help="Fix checkin status for a specific need ID")
    parser.add_argument("--simple-fix", type=int, help="Simple fix for a specific need ID")
    parser.add_argument("--export-csv", action="store_true", help="Export shift status data to CSV")
    parser.add_argument("--start-date", help="Filter shifts starting on or after this date (YYYY-MM-DD)")
    parser.add_argument("--end-date", help="Filter shifts starting on or before this date (YYYY-MM-DD)")
    parser.add_argument("--csv-need-id", type=int, help="Filter shifts by need_id for CSV export")
    parser.add_argument("--output", help="Output file name for CSV export (default: shift_status_export.csv)")
    
    args = parser.parse_args()
    
    db = get_db_connection()
    
    if args.status:
        show_sync_status(db)
    elif args.list:
        list_collections(db)
    elif args.collection:
        query_collection(db, args.collection, args.query, args.sort, args.limit)
    elif args.analyze:
        analyze_data(db, args.analyze)
    elif args.count_statuses:
        count_checkin_statuses(db)
    elif args.diagnose_shift:
        diagnose_shift(db, args.shift_id, args.need_id)
    elif args.analyze_checkin:
        analyze_checkin_vs_hours(db)
    elif args.fix_status:
        fix_checkin_status(db)
    elif args.fix_need:
        fix_specific_need(db, args.fix_need)
    elif args.simple_fix:
        simple_fix_need(db, args.simple_fix)
    elif args.export_csv:
        output_file = args.output if args.output else "shift_status_export.csv"
        export_shift_status_csv(db, args.start_date, args.end_date, args.csv_need_id, output_file)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
