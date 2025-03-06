def _process_country(self, country, start_date, end_date):
    """Process analytics data for a specific country within date range"""
    self.log.info(f"Processing {country} data from {start_date.date()} to {end_date.date()}")
    
    # Ensure we have timezone-aware datetime objects
    if start_date.tzinfo is None:
        utc = pytz.UTC
        start_date = utc.localize(start_date)
        end_date = utc.localize(end_date)
    
    # Get all documents from the collection
    collection_name = f'pageViews_{country}'
    docs = list(self.db.collection(collection_name).get())
    self.log.info(f"Retrieved {len(docs)} total documents for {country}")
    
    # Filter documents by date in memory (more reliable than Firestore query)
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
                if isinstance(value, datetime):
                    parsed_timestamp = value
            
            if parsed_timestamp is None:
                filter_errors += 1
                continue
            
            # Make sure the timestamp is timezone-aware
            if parsed_timestamp.tzinfo is None:
                parsed_timestamp = pytz.UTC.localize(parsed_timestamp)
            
            # Check if within date range
            if start_date <= parsed_timestamp <= end_date:
                # Store the parsed timestamp to avoid reparsing later
                data['_parsed_timestamp'] = parsed_timestamp
                filtered_docs.append(data)
        except Exception as e:
            filter_errors += 1
            if filter_errors <= 5:
                self.log.warning(f"Error filtering doc {doc_id}: {e}")
    
    self.log.info(f"Found {len(filtered_docs)} documents within date range")
    if filter_errors > 0:
        self.log.warning(f"Had {filter_errors} errors while filtering")
    
    # Process the filtered documents into daily metrics
    daily_metrics = {}
    
    for data in filtered_docs:
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
            
        except Exception as e:
            self.log.warning(f"Error processing document: {e}")
    
    # Convert sets to counts for serialization
    result = {'daily': {}}
    for date_str, metrics in daily_metrics.items():
        result['daily'][date_str] = {
            'pageviews': metrics['pageviews'],
            'unique_visitors': len(metrics['visitors']),
            'total_time': metrics['total_time']
        }
    
    self.log.info(f"Processed metrics for {len(daily_metrics)} days")
    return result