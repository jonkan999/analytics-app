import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime, timedelta
import logging
import pytz
import sys

class AnalyticsProcessor:
    """Processes raw pageview data into aggregated analytics metrics"""

    def __init__(self):
        """Initialize the analytics processor with Firebase connection and logging"""
        # Set up logging
        self.log = logging.getLogger(__name__)
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        self.log.addHandler(handler)
        self.log.setLevel(logging.INFO)
        
        # Initialize Firebase if not already initialized
        if not firebase_admin._apps:
            try:
                # Try with service account first
                cred = credentials.Certificate('keys/firestore_service_account.json')
                firebase_admin.initialize_app(cred)
                self.log.info("Initialized Firebase with service account file")
            except Exception as e:
                # Fall back to default credentials (for Cloud Run)
                firebase_admin.initialize_app()
                self.log.info(f"Initialized Firebase with default credentials: {e}")
        
        # Get Firestore client
        self.db = firestore.client()
        self.log.info("Analytics processor initialized")

    def process_analytics(self, days=30):
        """Process analytics data for specified countries and time period
        
        Args:
            days: Number of days to process (default: 30)
        """
        # Focus on Nordic countries + Estonia
        countries = ['se', 'no', 'dk', 'fi', 'ee']
        
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
            timestamp_str = timestamp.strftime('%Y-%m-%d_%H%M%S')
            
            # Create an aggregated "all" result from all countries
            all_metrics = self._aggregate_all_countries(results)
            
            # Structure the data in the expected format
            formatted_results = {
                'all': all_metrics,
                'by_country': results
            }
            
            # Update the "latest" document to always point to most recent data
            latest_ref = self.db.collection('processed_analytics').document('latest')
            latest_ref.set({
                'timestamp': timestamp,
                'data': formatted_results
            })
            self.log.info(f"Updated 'latest' document with most recent data")
            
        except Exception as e:
            self.log.error(f"Error storing results: {e}")

    def _aggregate_all_countries(self, country_results):
        """Aggregate pageviews from all countries into a single result"""
        self.log.info("Aggregating pageviews from all countries")
        
        all_metrics = {'daily': {}}
        dates_seen = set()
        
        # First, collect all dates from all countries
        for country, result in country_results.items():
            if 'daily' in result:
                for date in result['daily'].keys():
                    dates_seen.add(date)
        
        # Initialize metrics for all dates
        for date in dates_seen:
            all_metrics['daily'][date] = {
                'pageviews': 0,
                'rolling_7': 0,
                'rolling_28': 0,
                'growth_7': 0,
                'growth_28': 0
            }
        
        # Log country counts for debugging
        for country, result in country_results.items():
            if 'daily' in result:
                daily_sum = sum(metrics.get('pageviews', 0) for metrics in result['daily'].values())
                self.log.info(f"Country {country}: {daily_sum} total pageviews")
        
        # Aggregate pageviews from each country
        for country, result in country_results.items():
            if 'daily' not in result:
                continue
            
            for date, metrics in result['daily'].items():
                all_metrics['daily'][date]['pageviews'] += metrics.get('pageviews', 0)
        
        # Re-calculate rolling metrics for the aggregated data
        all_metrics = self._calculate_rolling_metrics(all_metrics)
        
        # Log the aggregated results for verification
        total_pageviews = sum(metrics['pageviews'] for metrics in all_metrics['daily'].values())
        self.log.info(f"Total aggregated pageviews across all countries: {total_pageviews}")
        
        return all_metrics

    def _calculate_rolling_metrics(self, metrics):
        """Calculate rolling and growth metrics from daily pageviews"""
        # Convert daily metrics to array of (date, pageviews) for sorting
        date_metrics = [(datetime.fromisoformat(date_str), date_str, metrics['daily'][date_str]['pageviews']) 
                       for date_str in metrics['daily']]
        date_metrics.sort()  # Sort by actual date
        
        # Calculate metrics for each day
        for i, (date_obj, date_str, pageviews) in enumerate(date_metrics):
            # Calculate rolling 7-day pageviews
            if i >= 6:  # Need 7 days of data
                metrics['daily'][date_str]['rolling_7'] = sum(
                    item[2] for item in date_metrics[i-6:i+1]
                )
                
                # Calculate 7-day growth if we have data from 14 days ago
                if i >= 13:
                    previous_7 = sum(item[2] for item in date_metrics[i-13:i-6])
                    current_7 = metrics['daily'][date_str]['rolling_7']
                    
                    if previous_7 > 0:
                        growth = ((current_7 / previous_7) - 1) * 100
                        metrics['daily'][date_str]['growth_7'] = round(growth, 2)
            
            # Calculate rolling 28-day pageviews
            if i >= 27:  # Need 28 days of data
                metrics['daily'][date_str]['rolling_28'] = sum(
                    item[2] for item in date_metrics[i-27:i+1]
                )
                
                # Calculate 28-day growth if we have data from 56 days ago
                if i >= 55:
                    previous_28 = sum(item[2] for item in date_metrics[i-55:i-27])
                    current_28 = metrics['daily'][date_str]['rolling_28']
                    
                    if previous_28 > 0:
                        growth = ((current_28 / previous_28) - 1) * 100
                        metrics['daily'][date_str]['growth_28'] = round(growth, 2)
        
        return metrics

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
        """Aggregate pageviews by day from filtered documents"""
        daily_metrics = {}
        
        for data in filtered_docs:
            try:
                parsed_timestamp = data['_parsed_timestamp']
                date_str = parsed_timestamp.date().isoformat()
                
                # Initialize metrics for new date - simplified to just count pageviews
                if date_str not in daily_metrics:
                    daily_metrics[date_str] = {
                        'pageviews': 0
                    }
                
                # Count pageview - every document is one pageview
                daily_metrics[date_str]['pageviews'] += 1
                
            except Exception as e:
                self.log.warning(f"Error processing document: {e}")
        
        return daily_metrics

    def _format_metrics_for_storage(self, daily_metrics):
        """Format daily metrics for storage with rolling periods and growth"""
        result = {'daily': {}}
        
        # Convert daily metrics to array of (date, pageviews) for sorting
        date_metrics = [(datetime.fromisoformat(date_str), date_str, metrics['pageviews']) 
                       for date_str, metrics in daily_metrics.items()]
        date_metrics.sort()  # Sort by actual date
        
        # Calculate metrics for each day
        for i, (date_obj, date_str, pageviews) in enumerate(date_metrics):
            result['daily'][date_str] = {
                'pageviews': pageviews,
                'rolling_7': 0,
                'rolling_28': 0,
                'growth_7': 0,
                'growth_28': 0
            }
            
            # Calculate rolling 7-day pageviews
            if i >= 6:  # Need 7 days of data
                result['daily'][date_str]['rolling_7'] = sum(
                    item[2] for item in date_metrics[i-6:i+1]
                )
                
                # Calculate 7-day growth if we have data from 14 days ago
                if i >= 13:
                    previous_7 = sum(item[2] for item in date_metrics[i-13:i-6])
                    current_7 = result['daily'][date_str]['rolling_7']
                    
                    if previous_7 > 0:
                        growth = ((current_7 / previous_7) - 1) * 100
                        result['daily'][date_str]['growth_7'] = round(growth, 2)
            
            # Calculate rolling 28-day pageviews
            if i >= 27:  # Need 28 days of data
                result['daily'][date_str]['rolling_28'] = sum(
                    item[2] for item in date_metrics[i-27:i+1]
                )
                
                # Calculate 28-day growth if we have data from 56 days ago
                if i >= 55:
                    previous_28 = sum(item[2] for item in date_metrics[i-55:i-27])
                    current_28 = result['daily'][date_str]['rolling_28']
                    
                    if previous_28 > 0:
                        growth = ((current_28 / previous_28) - 1) * 100
                        result['daily'][date_str]['growth_28'] = round(growth, 2)
        
        return result

def main():
    try:
        processor = AnalyticsProcessor()
        processor.process_analytics()
        print("Analytics processing completed successfully")
    except Exception as e:
        print(f"Error processing analytics: {str(e)}")
        raise e

if __name__ == "__main__":
    main()