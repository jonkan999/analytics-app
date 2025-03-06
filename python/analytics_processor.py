import logging
import pytz
from datetime import datetime, timedelta
from firebase_admin import firestore

class AnalyticsProcessor:
    """Processes raw pageview data into aggregated analytics metrics"""

    def __init__(self, db=None, log=None):
        """Initialize the analytics processor
        
        Args:
            db: Firestore database client (optional)
            log: Logger instance (optional)
        """
        # Set up logging
        self.log = log or logging.getLogger(__name__)
        
        # Set up database connection
        self.db = db or firestore.client()
        
        self.log.info("Analytics processor initialized")

    def process_analytics(self, countries=None, days=30):
        """Process analytics data for specified countries and time period
        
        Args:
            countries: List of country codes to process (default: all countries)
            days: Number of days to process (default: 30)
        """
        if not countries:
            # Default list of countries to process
            countries = ['se', 'no', 'dk', 'fi', 'nl', 'be', 'de']
        
        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        self.log.info(f"Processing analytics for {len(countries)} countries, last {days} days")
        
        # Process each country
        results = {}
        for country in countries:
            try:
                country_results = self._process_country(country, start_date, end_date)
                results[country] = country_results
                self.log.info(f"Successfully processed {country}")
            except Exception as e:
                self.log.error(f"Error processing {country}: {e}")
        
        # Store results
        self._store_results(results)
        
        return results
    
    def _store_results(self, results):
        """Store processed results in the database"""
        try:
            # Store the timestamp of when this data was processed
            timestamp = datetime.now()
            
            # Create document with processed data
            doc_ref = self.db.collection('processed_analytics').document(timestamp.strftime('%Y-%m-%d_%H%M%S'))
            doc_ref.set({
                'timestamp': timestamp,
                'data': results
            })
            
            self.log.info(f"Stored results in processed_analytics/{timestamp.strftime('%Y-%m-%d_%H%M%S')}")
        except Exception as e:
            self.log.error(f"Error storing results: {e}")

    def _process_country(self, country, start_date, end_date):
        """Process analytics data for a specific country within date range"""
        self.log.info(f"Processing {country} data from {start_date.date()} to {end_date.date()}")
        
        # Ensure timezone-aware datetimes
        start_date, end_date = self._ensure_timezone_aware([start_date, end_date])
        
        # Get and process documents
        collection_name = f'pageViews_{country}'
        docs = list(self.db.collection(collection_name).get())
        self.log.info(f"Retrieved {len(docs)} total documents for {country}")
        
        # Filter and parse documents
        filtered_docs, filter_errors = self._filter_documents_by_date(docs, start_date, end_date)
        
        self.log.info(f"Found {len(filtered_docs)} documents within date range")
        if filter_errors > 0:
            self.log.warning(f"Had {filter_errors} errors while filtering")
        
        # Aggregate metrics by day
        daily_metrics = self._aggregate_daily_metrics(filtered_docs)
        
        # Format result for storage
        result = self._format_metrics_for_storage(daily_metrics)
        
        self.log.info(f"Processed metrics for {len(daily_metrics)} days")
        return result

    def _ensure_timezone_aware(self, dates):
        """Ensure all dates are timezone-aware"""
        utc = pytz.UTC
        result = []
        for dt in dates:
            if dt.tzinfo is None:
                result.append(utc.localize(dt))
            else:
                result.append(dt)
        return result if len(result) > 1 else result[0]

    def _parse_timestamp(self, data):
        """Extract and parse timestamp from document data"""
        parsed_timestamp = None
        
        # Try visitedTimestamp first (most reliable)
        if 'visitedTimestamp' in data:
            value = data['visitedTimestamp']
            if isinstance(value, str):
                try:
                    if 'Z' in value:
                        parsed_timestamp = datetime.fromisoformat(value.replace('Z', '+00:00'))
                    else:
                        parsed_timestamp = datetime.fromisoformat(value)
                except Exception:
                    pass
            elif isinstance(value, datetime):
                parsed_timestamp = value
        
        # Fall back to timestamp field
        if parsed_timestamp is None and 'timestamp' in data:
            value = data['timestamp']
            if isinstance(value, datetime):
                parsed_timestamp = value
        
        # Ensure timezone awareness
        if parsed_timestamp and parsed_timestamp.tzinfo is None:
            parsed_timestamp = pytz.UTC.localize(parsed_timestamp)
            
        return parsed_timestamp

    def _filter_documents_by_date(self, docs, start_date, end_date):
        """Filter documents that fall within the specified date range"""
        filtered_docs = []
        filter_errors = 0
        
        for doc in docs:
            data = doc.to_dict()
            doc_id = doc.id
            try:
                parsed_timestamp = self._parse_timestamp(data)
                
                if parsed_timestamp is None:
                    filter_errors += 1
                    continue
                
                # Check if within date range
                if start_date <= parsed_timestamp <= end_date:
                    data['_parsed_timestamp'] = parsed_timestamp
                    filtered_docs.append(data)
            except Exception as e:
                filter_errors += 1
                if filter_errors <= 5:
                    self.log.warning(f"Error filtering doc {doc_id}: {e}")
        
        return filtered_docs, filter_errors

    def _aggregate_daily_metrics(self, filtered_docs):
        """Aggregate metrics by day from filtered documents"""
        daily_metrics = {}
        
        for data in filtered_docs:
            try:
                parsed_timestamp = data['_parsed_timestamp']
                date_str = parsed_timestamp.date().isoformat()
                
                # Initialize metrics for new date
                if date_str not in daily_metrics:
                    daily_metrics[date_str] = {
                        'pageviews': 0,
                        'visitors': set(),
                        'total_time': 0
                    }
                
                # Count pageview
                daily_metrics[date_str]['pageviews'] += 1
                
                # Add visitor ID (handling "null" values)
                visitor_id = self._get_visitor_id(data)
                daily_metrics[date_str]['visitors'].add(visitor_id)
                
                # Add time on page
                time_on_page = self._get_time_on_page(data)
                daily_metrics[date_str]['total_time'] += time_on_page
                
            except Exception as e:
                self.log.warning(f"Error processing document: {e}")
        
        return daily_metrics

    def _get_visitor_id(self, data):
        """Extract visitor ID from document data"""
        visitor_id = 'unknown'
        if 'dailyId' in data:
            if data['dailyId'] != "null" and data['dailyId'] is not None:
                visitor_id = str(data['dailyId'])
        return visitor_id

    def _get_time_on_page(self, data):
        """Extract time on page from document data"""
        time_on_page = 0
        if 'timeOnPage' in data:
            try:
                if isinstance(data['timeOnPage'], str):
                    time_on_page = float(data['timeOnPage'])
                else:
                    time_on_page = float(data['timeOnPage'])
            except (ValueError, TypeError):
                pass
        return time_on_page

    def _format_metrics_for_storage(self, daily_metrics):
        """Format daily metrics for storage"""
        result = {'daily': {}}
        for date_str, metrics in daily_metrics.items():
            result['daily'][date_str] = {
                'pageviews': metrics['pageviews'],
                'unique_visitors': len(metrics['visitors']),
                'total_time': metrics['total_time']
            }
        return result