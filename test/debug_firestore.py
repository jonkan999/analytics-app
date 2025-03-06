import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime, timedelta
import json
import pytz

def debug_firestore_data():
    """Comprehensive debugging function for Firestore data"""
    # Initialize Firebase (if not already initialized)
    if not firebase_admin._apps:
        try:
            # Try with service account file first
            cred = credentials.Certificate('keys/firestore_service_account.json')
            firebase_admin.initialize_app(cred)
            print("Initialized Firebase with service account file")
        except Exception as e:
            print(f"Service account file error: {e}")
            # Fall back to default credentials
            firebase_admin.initialize_app()
            print("Initialized Firebase with default credentials")

    db = firestore.client()
    
    countries = ['se', 'no', 'dk', 'fi', 'ee', 'de', 'nl', 'be']
    
    print(f"\n=== Firestore Debug Report ({datetime.now()}) ===\n")
    
    # Check collections
    print("Available collections:")
    collections = [col.id for col in db.collections()]
    for col in collections:
        print(f"  - {col}")
    
    print("\n=== Pageview Collections ===")
    
    for country in countries:
        collection_name = f'pageViews_{country}'
        
        # Get sample documents without filtering
        try:
            docs = db.collection(collection_name).limit(5).get()
            doc_list = list(docs)
            
            print(f"\n{collection_name}: {len(doc_list)} sample documents retrieved")
            
            if len(doc_list) > 0:
                # Analyze first document to determine field structure
                first_doc = doc_list[0].to_dict()
                print(f"Document fields: {list(first_doc.keys())}")
                
                # Check for timestamp fields
                timestamp_fields = []
                for field in first_doc.keys():
                    if 'time' in field.lower() or 'date' in field.lower():
                        field_value = first_doc[field]
                        timestamp_fields.append(f"{field} ({type(field_value).__name__}): {field_value}")
                
                if timestamp_fields:
                    print("Timestamp fields:")
                    for tf in timestamp_fields:
                        print(f"  {tf}")
                
                # Check total document count
                total_count = len(list(db.collection(collection_name).limit(1000).get()))
                print(f"Total documents (up to 1000): {total_count}")
                
                # Get documents from last 24 hours
                yesterday = datetime.now() - timedelta(days=1)
                print(f"Checking documents from last 24 hours (since {yesterday})")
                
                # Try different timestamp fields
                for ts_field in ['timestamp', 'visitedTimestamp', 'endTimestamp', 'lastUpdate']:
                    try:
                        recent_docs = db.collection(collection_name)\
                                    .where(ts_field, '>=', yesterday)\
                                    .limit(10)\
                                    .get()
                        recent_list = list(recent_docs)
                        print(f"  Using '{ts_field}': Found {len(recent_list)} documents")
                        
                        if len(recent_list) > 0:
                            print("  Sample data:")
                            sample = recent_list[0].to_dict()
                            # Pretty print a subset of fields
                            important_fields = {k: sample.get(k) for k in 
                                              ['visitedTimestamp', 'path', 'dailyId', 'timeOnPage'] 
                                              if k in sample}
                            print(f"    {json.dumps(important_fields, default=str, indent=2)}")
                    except Exception as e:
                        print(f"  Error querying with '{ts_field}': {e}")
                
                # Show one full document with all fields
                print("\nComplete sample document:")
                try:
                    # Format the document for better readability
                    formatted_doc = {k: str(v) for k, v in first_doc.items()}
                    print(json.dumps(formatted_doc, indent=2))
                except Exception as e:
                    print(f"Error formatting document: {e}")
                    print(first_doc)
                    
            else:
                print("No documents found in this collection")
                
        except Exception as e:
            print(f"Error accessing collection: {e}")
    
    print("\n=== Debug Complete ===\n")

if __name__ == "__main__":
    debug_firestore_data()