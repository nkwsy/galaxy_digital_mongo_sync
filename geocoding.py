import re
import time
import logging
import requests
from typing import Dict, List, Tuple, Optional, Any
import os
from dotenv import load_dotenv
import random
import concurrent.futures
import json
from pathlib import Path
import urllib.parse

# Load environment variables
load_dotenv()

# Create a geocoding cache directory if it doesn't exist
CACHE_DIR = Path("geocode_cache")
CACHE_DIR.mkdir(exist_ok=True)

# Global cache to avoid duplicate API calls within the same session
ADDRESS_CACHE = {}

def sanitize_address(address: str) -> str:
    """
    Clean up address string for geocoding.
    
    Args:
        address: Address string to sanitize
        
    Returns:
        Sanitized address string
    """
    # Remove unit/apartment numbers as they often cause geocoding failures
    replacements = [
        (r'\bUnit\s+\w+', ''),  # Remove "Unit XXX"
        (r'\bApt\.?\s+\w+', ''), # Remove "Apt XXX" or "Apt. XXX"
        (r'\b[A-Z]$', ''),      # Remove single letter unit numbers at end
        (r'\.', ''),            # Remove periods
        (r'\s+', ' '),          # Normalize whitespace
    ]
    
    result = address
    for pattern, replacement in replacements:
        result = re.sub(pattern, replacement, result, flags=re.IGNORECASE)
    
    return result.strip()

def is_zip_code_only(address: str) -> bool:
    """
    Check if the address is only a zip code.
    
    Args:
        address: Address string to check
        
    Returns:
        True if the address is only a zip code, False otherwise
    """
    # Remove all whitespace and check if the result is a 5-digit number
    clean = re.sub(r'\s+', '', address)
    return bool(re.match(r'^\d{5}(-\d{4})?$', clean))

def get_cache_key(address: str) -> str:
    """
    Generate a cache key for an address.
    
    Args:
        address: Address to generate a key for
        
    Returns:
        Cache key string
    """
    # Normalize the address for consistent cache keys
    normalized = re.sub(r'\s+', ' ', address.lower().strip())
    return normalized

def load_from_cache(address: str) -> Optional[Dict[str, float]]:
    """
    Try to load geocoding results from cache.
    
    Args:
        address: Address to look up
        
    Returns:
        Cached coordinates as dict with 'latitude' and 'longitude' keys or None if not in cache
    """
    # Check in-memory cache first
    cache_key = get_cache_key(address)
    if cache_key in ADDRESS_CACHE:
        logging.debug(f"Cache hit (memory): {address}")
        return ADDRESS_CACHE[cache_key]
    
    # Check file cache
    cache_file = CACHE_DIR / f"{hash(cache_key)}.json"
    if cache_file.exists():
        try:
            with open(cache_file, 'r') as f:
                data = json.load(f)
                # Handle both old and new format
                if 'latitude' in data and 'longitude' in data:
                    result = {
                        'latitude': data['latitude'],
                        'longitude': data['longitude']
                    }
                    ADDRESS_CACHE[cache_key] = result
                    return result
                elif 'lat' in data and 'lng' in data:
                    # Convert old format to new format
                    result = {
                        'latitude': data['lat'],
                        'longitude': data['lng']
                    }
                    ADDRESS_CACHE[cache_key] = result
                    return result
        except Exception as e:
            logging.warning(f"Error reading cache file for {address}: {str(e)}")
    
    return None

def save_to_cache(address: str, coordinates: Dict[str, float]) -> None:
    """
    Save geocoding results to cache.
    
    Args:
        address: Address that was geocoded
        coordinates: Coordinates dict with 'latitude' and 'longitude' keys
    """
    if not coordinates:
        return
        
    cache_key = get_cache_key(address)
    
    # Save to memory cache
    ADDRESS_CACHE[cache_key] = coordinates
    
    # Save to file cache
    cache_file = CACHE_DIR / f"{hash(cache_key)}.json"
    try:
        with open(cache_file, 'w') as f:
            json.dump({
                'address': address,
                'latitude': coordinates['latitude'],
                'longitude': coordinates['longitude']
            }, f)
    except Exception as e:
        logging.warning(f"Error writing cache file for {address}: {str(e)}")

def geocode_zip_code(zip_code: str, api_key: Optional[str] = None) -> Optional[Tuple[float, float]]:
    """
    Geocode a zip code to get coordinates, with a small random offset to distribute points.
    
    Args:
        zip_code: Zip code to geocode
        api_key: Google Maps API key (optional, will use GOOGLE_MAPS_API_KEY env var if not provided)
        
    Returns:
        Tuple of (latitude, longitude) or None if geocoding failed
    """
    if not api_key:
        api_key = os.getenv("GOOGLE_MAPS_API_KEY")
        
    if not api_key:
        logging.error("No Google Maps API key provided")
        return None
        
    # Check cache first
    cached_result = load_from_cache(zip_code)
    if cached_result:
        return cached_result
    
    # Clean the zip code
    zip_code = zip_code.strip()
    
    # Construct the URL for the API request
    url = f"https://maps.googleapis.com/maps/api/geocode/json?address={zip_code}&key={api_key}"
    
    try:
        response = requests.get(url)
        data = response.json()
        
        if data['status'] == 'OK' and data['results']:
            location = data['results'][0]['geometry']['location']
            result = {
                'latitude': location['lat'],
                'longitude': location['lng']
            }
            
            # Save to cache
            save_to_cache(zip_code, result)
            
            return result
        else:
            logging.warning(f"Failed to geocode zip code {zip_code}: {data.get('status', 'Unknown error')}")
            return None
            
    except Exception as e:
        logging.error(f"Error geocoding zip code {zip_code}: {str(e)}")
        return None

def geocode_address(address: str, api_key: Optional[str] = None) -> Optional[Tuple[float, float]]:
    """
    Geocode an address to get coordinates using Google's Geocoding API.
    
    Args:
        address: Address string to geocode
        api_key: Google Maps API key (optional, will use GOOGLE_MAPS_API_KEY env var if not provided)
        
    Returns:
        Tuple of (latitude, longitude) or None if geocoding failed
    """
    if not api_key:
        api_key = os.getenv("GOOGLE_MAPS_API_KEY")
        
    if not api_key:
        logging.error("No Google Maps API key provided")
        return None
    
    # Check cache first
    cached_result = load_from_cache(address)
    if cached_result:
        return cached_result
    
    # Sanitize the address
    address = sanitize_address(address)
    
    # Construct the URL for the API request
    url = f"https://maps.googleapis.com/maps/api/geocode/json?address={urllib.parse.quote(address)}&key={api_key}"
    
    try:
        response = requests.get(url)
        data = response.json()
        
        if data['status'] == 'OK' and data['results']:
            location = data['results'][0]['geometry']['location']
            result = {
                'latitude': location['lat'],
                'longitude': location['lng']
            }
            
            # Save to cache
            save_to_cache(address, result)
            
            return result
        else:
            logging.warning(f"Failed to geocode address {address}: {data.get('status', 'Unknown error')}")
            return None
            
    except Exception as e:
        logging.error(f"Error geocoding address {address}: {str(e)}")
        return None

def geocode_address_worker(args):
    """
    Worker function for geocoding a single address.
    
    Args:
        args (tuple): Tuple containing (address_dict, api_key, exclude_zip_only)
            address_dict should have 'id' and 'address' keys
    
    Returns:
        dict: Dictionary with 'id', 'latitude', 'longitude' keys or None if geocoding failed
    """
    address_dict, api_key, exclude_zip_only = args
    
    if not address_dict or 'address' not in address_dict or not address_dict['address']:
        logging.warning(f"Skipping geocoding for empty address: {address_dict}")
        return None
    
    address = address_dict['address']
    address_id = address_dict.get('id', 'unknown')
    
    # Check if this is a zip code only address
    is_zip_only = is_zip_code_only(address)
    
    # Skip zip code only addresses if requested
    if exclude_zip_only and is_zip_only:
        logging.info(f"Skipping zip code only address: {address}")
        return None
    
    # Try to load from cache first
    cached_result = load_from_cache(address)
    if cached_result:
        logging.info(f"Using cached geocoding result for: {address}")
        return {
            'id': address_id,
            'latitude': cached_result['latitude'],
            'longitude': cached_result['longitude'],
            'is_zip_only': is_zip_only
        }
    
    # Geocode the address
    try:
        if is_zip_only:
            result = geocode_zip_code(address, api_key)
        else:
            sanitized_address = sanitize_address(address)
            result = geocode_address(sanitized_address, api_key)
        
        if result and 'latitude' in result and 'longitude' in result:
            # Save to cache
            save_to_cache(address, result)
            
            # Return the result with the ID
            return {
                'id': address_id,
                'latitude': result['latitude'],
                'longitude': result['longitude'],
                'is_zip_only': is_zip_only
            }
        else:
            logging.warning(f"Failed to geocode address: {address}")
            return None
            
    except Exception as e:
        logging.error(f"Error geocoding address {address}: {str(e)}")
        return None

def batch_geocode(addresses, api_key, exclude_zip_only=False, max_workers=10, progress_callback=None):
    """
    Geocode a batch of addresses using parallel processing.
    
    Args:
        addresses (list): List of dictionaries with 'id' and 'address' keys
        api_key (str): Google Maps API key
        exclude_zip_only (bool): Whether to exclude zip code only addresses
        max_workers (int): Maximum number of parallel workers
        progress_callback (callable): Optional callback function to report progress
            The callback will receive (current_count, total_count, success_count)
        
    Returns:
        list: List of dictionaries with 'id', 'latitude', 'longitude' keys
    """
    if not addresses:
        return []
    
    # Create cache directory if it doesn't exist
    cache_dir = Path("geocode_cache")
    cache_dir.mkdir(exist_ok=True)
    
    # Determine the actual number of workers based on the number of addresses
    actual_workers = min(max_workers, len(addresses))
    
    # Initialize counters for progress reporting
    processed_count = 0
    success_count = 0
    total_count = len(addresses)
    
    # Create a list to store the results
    results = []
    
    logging.info(f"Geocoding {len(addresses)} addresses using {actual_workers} workers")
    
    # Use ThreadPoolExecutor for parallel processing
    with concurrent.futures.ThreadPoolExecutor(max_workers=actual_workers) as executor:
        # Submit all geocoding tasks
        future_to_address = {
            executor.submit(geocode_address_worker, (address, api_key, exclude_zip_only)): address
            for address in addresses
        }
        
        # Process results as they complete
        for future in concurrent.futures.as_completed(future_to_address):
            address = future_to_address[future]
            processed_count += 1
            
            try:
                result = future.result()
                if result:
                    results.append(result)
                    success_count += 1
                
                # Call progress callback if provided
                if progress_callback:
                    progress_callback(processed_count, total_count, success_count)
                
                # Log progress periodically
                if processed_count % 10 == 0 or processed_count == total_count:
                    logging.info(f"Geocoded {processed_count}/{total_count} addresses ({success_count} successful)")
                
            except Exception as e:
                logging.error(f"Error geocoding address {address['address']}: {str(e)}")
    
    logging.info(f"Completed geocoding {len(addresses)} addresses. Successfully geocoded: {success_count}")
    return results