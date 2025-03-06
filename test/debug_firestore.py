import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime, timedelta
import json
import pytz
from typing import Any, Dict, List, Set, Union
from dateutil import parser as date_parser

def stringify_for_json(obj: Any) -> Any:
    """Convert complex objects to JSON-serializable format"""
    if isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, set):
        return list(obj)
    return str(obj)

def debug_data_extraction(country: str = 'se', days: int = 7):
    """Debug the exact data extraction and processing for a country"""
    # Initialize Firebase
    if not firebase_admin._apps:
        try:
            cred = credentials.Certificate('keys/firestore_service_account.json')
            firebase_admin.initialize_app(cred)
            print("Initialized with service account")
        except Exception as e:
            firebase_admin.initialize_app()
            print(f"Using default credentials: {e}")
    
    db = firestore.client()
    
    # Define date range - include enough data for growth calculation
    # Make sure these are timezone-aware
    utc = pytz.UTC
    end_date = datetime.now(utc)
    start_date = end_date - timedelta(days=days)
    
    print(f"\n=== Data Extraction Debug for {country.upper()} ===")
    print(f"Date range: {start_date.date()} to {end_date.date()}")
    print(f"Using timezone-aware datetimes (UTC)")
    
    # 1. First get ALL documents to examine
    collection_name = f'pageViews_{country}'
    docs = list(db.collection(collection_name).get())
    print(f"\nTotal documents in {collection_name}: {len(docs)}")
    
    # 2. Examine timestamp types present in the collection
    timestamp_types = {}
    for doc in docs[:50]:  # Check more docs
        data = doc.to_dict()
        for field in ['visitedTimestamp', 'timestamp', 'endTimestamp', 'lastUpdate']:
            if field in data:
                value = data[field]
                type_name = type(value).__name__
                if field not in timestamp_types:
                    timestamp_types[field] = {}
                if type_name not in timestamp_types[field]:
                    timestamp_types[field][type_name] = 0
                timestamp_types[field][type_name] += 1
                
                # Show example values
                if timestamp_types[field][type_name] <= 3:  # Show multiple examples
                    print(f"Example {field} ({type_name}): {value}")
    
    print(f"\nTimestamp field types found: {json.dumps(timestamp_types, indent=2)}")
    
    # 3. Try to parse each timestamp format and check if it works
    print("\nTesting timestamp parsing:")
    for field, types in timestamp_types.items():
        for type_name in types:
            print(f"  Testing {field} ({type_name}):")
            # Find docs with this field and type
            for doc in docs:
                data = doc.to_dict()
                if field in data and type(data[field]).__name__ == type_name:
                    value = data[field]
                    try:
                        dt = None
                        if isinstance(value, datetime):
                            dt = value if value.tzinfo else utc.localize(value)
                            print(f"    Successfully used datetime object: {value} → {dt}")
                        elif isinstance(value, str):
                            if 'Z' in value:
                                # ISO format with Z timezone
                                dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
                                print(f"    Successfully parsed ISO with Z: {value} → {dt}")
                            elif '+' in value or 'T' in value:
                                # Try standard ISO format
                                dt = datetime.fromisoformat(value)
                                print(f"    Successfully parsed ISO: {value} → {dt}")
                            else:
                                # Try dateutil's parser for other formats
                                dt = date_parser.parse(value)
                                if not dt.tzinfo:
                                    dt = utc.localize(dt)
                                print(f"    Successfully parsed with dateutil: {value} → {dt}")
                    except Exception as e:
                        print(f"    Failed to parse {value}: {e}")
                        # Try alternative parsing as a last resort
                        try:
                            dt = date_parser.parse(value)
                            if not dt.tzinfo:
                                dt = utc.localize(dt)
                            print(f"    Alternate parsing succeeded: {value} → {dt}")
                        except Exception as e2:
                            print(f"    All parsing methods failed: {e2}")
                    break
    
    # 4. Now try to filter and process documents like the actual analytics processor
    print("\nFiltering documents by date range:")
    filtered_docs = []
    filter_errors = 0
    
    for doc in docs:
        data = doc.to_dict()
        doc_id = doc.id
        try:
            parsed_timestamp = None
            timestamp_field = None
            
            # Try visitedTimestamp first
            if 'visitedTimestamp' in data:
                timestamp_field = 'visitedTimestamp'
                value = data['visitedTimestamp']
                if isinstance(value, str):
                    try:
                        if 'Z' in value:
                            parsed_timestamp = datetime.fromisoformat(value.replace('Z', '+00:00'))
                        else:
                            parsed_timestamp = datetime.fromisoformat(value)
                    except:
                        pass
                elif isinstance(value, datetime):
                    parsed_timestamp = value
            
            # If visitedTimestamp fails, try timestamp
            if parsed_timestamp is None and 'timestamp' in data:
                timestamp_field = 'timestamp'
                value = data['timestamp']
                if isinstance(value, str):
                    try:
                        if 'Z' in value:
                            parsed_timestamp = datetime.fromisoformat(value.replace('Z', '+00:00'))
                        else:
                            # Try to parse various string formats
                            try:
                                parsed_timestamp = datetime.fromisoformat(value)
                            except:
                                # As a last resort, try a very permissive parser
                                try:
                                    from dateutil import parser
                                    parsed_timestamp = parser.parse(value)
                                except:
                                    pass
                    except:
                        pass
                elif isinstance(value, datetime):
                    parsed_timestamp = value
            
            if parsed_timestamp is None:
                filter_errors += 1
                if filter_errors <= 5:
                    print(f"  Could not parse timestamp for doc {doc_id}")
                continue
            
            # Make sure the timestamp is timezone-aware
            if parsed_timestamp.tzinfo is None:
                parsed_timestamp = utc.localize(parsed_timestamp)
            
            # Check if within date range
            if start_date <= parsed_timestamp <= end_date:
                data['_parsed_timestamp'] = parsed_timestamp
                data['_timestamp_field'] = timestamp_field
                filtered_docs.append(data)
        except Exception as e:
            filter_errors += 1
            if filter_errors <= 5:
                print(f"  Error filtering doc {doc_id}: {e}")
    
    print(f"Found {len(filtered_docs)} documents within date range")
    if filter_errors > 0:
        print(f"Had {filter_errors} errors while filtering")
    
    # 5. Process the filtered documents into daily metrics
    daily_metrics = {}
    processing_errors = 0
    
    for i, data in enumerate(filtered_docs):
        try:
            # Use the parsed timestamp
            parsed_timestamp = data['_parsed_timestamp']
            date_str = parsed_timestamp.date().isoformat()
            
            if date_str not in daily_metrics:
                daily_metrics[date_str] = {
                    'pageviews': 0,
                    'visitors': set(),
                    'total_time': 0
                }
            
            # Increment pageviews
            daily_metrics[date_str]['pageviews'] += 1
            
            # Process visitor ID
            visitor_id = 'unknown'
            if 'dailyId' in data:
                if data['dailyId'] != "null" and data['dailyId'] is not None:
                    visitor_id = str(data['dailyId'])
            
            daily_metrics[date_str]['visitors'].add(visitor_id)
            
            # Process time on page
            time_on_page = 0
            if 'timeOnPage' in data:
                try:
                    if isinstance(data['timeOnPage'], str):
                        time_on_page = float(data['timeOnPage'])
                    else:
                        time_on_page = float(data['timeOnPage'])
                except (ValueError, TypeError):
                    pass
            
            daily_metrics[date_str]['total_time'] += time_on_page
            
            # Print details of first 2 documents for verification
            if i < 2:
                # Create a simplified version for printing
                simple_data = {
                    'path': data.get('path', 'unknown'),
                    'timestamp_field': data['_timestamp_field'],
                    'timestamp_value': data.get(data['_timestamp_field']),
                    'parsed_timestamp': data['_parsed_timestamp'],
                    'date_str': date_str,
                    'dailyId': data.get('dailyId', 'not set'),
                    'timeOnPage': data.get('timeOnPage', 0)
                }
                print(f"\nSample document {i+1} processing:")
                print(json.dumps(simple_data, default=stringify_for_json, indent=2))
        except Exception as e:
            processing_errors += 1
            if processing_errors <= 5:
                print(f"Error processing document {i}: {e}")
    
    # 6. Prepare what would be stored in processed_analytics
    processed_data = {}
    for date_str, metrics in daily_metrics.items():
        processed_data[date_str] = {
            'pageviews': metrics['pageviews'],
            'unique_visitors': len(metrics['visitors']),
            'total_time': metrics['total_time']
        }
    
    # 7. Display final results
    print("\nFinal processed data (what would be stored in processed_analytics):")
    if processed_data:
        sorted_dates = sorted(processed_data.keys())
        for date in sorted_dates:
            metrics = processed_data[date]
            print(f"  {date}: pageviews={metrics['pageviews']}, "
                  f"visitors={metrics['unique_visitors']}, "
                  f"avg_time={metrics['total_time']/metrics['pageviews']:.2f}s")
        
        print("\nComplete processed data structure:")
        print(json.dumps(
            {'daily': processed_data}, 
            default=stringify_for_json, 
            indent=2
        ))
    else:
        print("  No data would be stored - all date processing failed")

    return processed_data

if __name__ == "__main__":
    # Test with Sweden data for a 7-day period
    debug_data_extraction('se', 7)