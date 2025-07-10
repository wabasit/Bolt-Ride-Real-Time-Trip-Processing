#!/usr/bin/env python3
"""
NSP Bolt Ride - Local Trip Processing System
Simulates the full production pipeline locally for development/testing
"""

import json
import sqlite3
import time
import asyncio
import threading
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Any
from flask import Flask, request, jsonify
from collections import defaultdict, deque
import pandas as pd
import uuid
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class TripEvent:
    """Standardized trip event structure"""
    event_type: str
    trip_id: str
    timestamp: str
    schema_version: str = "2.0"
    metadata: Optional[Dict] = None
    
    def to_dict(self) -> Dict:
        return asdict(self)

@dataclass 
class TripStartEvent(TripEvent):
    """Trip start event with specific fields"""
    user_id: str
    driver_id: str
    pickup_location: Dict[str, float]
    pickup_location_id: int
    dropoff_location_id: int
    vendor_id: int
    pickup_datetime: str
    estimated_dropoff_datetime: str
    estimated_fare: float

@dataclass
class TripEndEvent(TripEvent):
    """Trip end event with specific fields"""
    dropoff_location: Dict[str, float]
    dropoff_datetime: str
    final_fare: float
    tip_amount: float
    distance_km: float
    duration_minutes: int
    payment_type: int
    rate_code: int
    passenger_count: int
    trip_type: int

class LocalDatabase:
    """SQLite database for local development"""
    
    def __init__(self, db_path: str = "trip_data.db"):
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        """Initialize SQLite tables"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Trip state table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS trip_state (
                trip_id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                trip_start_data TEXT,
                trip_end_data TEXT,
                created_at TEXT,
                updated_at TEXT,
                completion_date TEXT
            )
        """)
        
        # Events table (for lineage)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS trip_events (
                event_id TEXT PRIMARY KEY,
                trip_id TEXT,
                event_type TEXT,
                event_data TEXT,
                processed_at TEXT,
                processing_status TEXT
            )
        """)
        
        # Daily metrics table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS daily_metrics (
                date TEXT PRIMARY KEY,
                total_fare REAL,
                trip_count INTEGER,
                average_fare REAL,
                max_fare REAL,
                min_fare REAL,
                generated_at TEXT
            )
        """)
        
        conn.commit()
        conn.close()
        logger.info("Database initialized")
    
    def get_connection(self):
        return sqlite3.connect(self.db_path)
    
    def insert_event(self, event: TripEvent):
        """Insert event into events table"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        event_id = str(uuid.uuid4())
        cursor.execute("""
            INSERT INTO trip_events 
            (event_id, trip_id, event_type, event_data, processed_at, processing_status)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            event_id,
            event.trip_id,
            event.event_type,
            json.dumps(event.to_dict()),
            datetime.utcnow().isoformat(),
            'processed'
        ))
        
        conn.commit()
        conn.close()
        return event_id
    
    def get_trip_state(self, trip_id: str) -> Optional[Dict]:
        """Get current trip state"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM trip_state WHERE trip_id = ?", (trip_id,))
        row = cursor.fetchone()
        conn.close()
        
        if row:
            return {
                'trip_id': row[0],
                'status': row[1], 
                'trip_start_data': json.loads(row[2]) if row[2] else None,
                'trip_end_data': json.loads(row[3]) if row[3] else None,
                'created_at': row[4],
                'updated_at': row[5],
                'completion_date': row[6]
            }
        return None
    
    def update_trip_state(self, trip_data: Dict):
        """Update trip state"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT OR REPLACE INTO trip_state 
            (trip_id, status, trip_start_data, trip_end_data, created_at, updated_at, completion_date)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            trip_data['trip_id'],
            trip_data['status'],
            json.dumps(trip_data.get('trip_start_data')) if trip_data.get('trip_start_data') else None,
            json.dumps(trip_data.get('trip_end_data')) if trip_data.get('trip_end_data') else None,
            trip_data.get('created_at'),
            datetime.utcnow().isoformat(),
            trip_data.get('completion_date')
        ))
        
        conn.commit()
        conn.close()
    
    def get_completed_trips_by_date(self, date: str) -> List[Dict]:
        """Get completed trips for a specific date"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT trip_start_data, trip_end_data 
            FROM trip_state 
            WHERE status = 'completed' AND completion_date = ?
        """, (date,))
        
        rows = cursor.fetchall()
        conn.close()
        
        trips = []
        for row in rows:
            trips.append({
                'trip_start_data': json.loads(row[0]) if row[0] else {},
                'trip_end_data': json.loads(row[1]) if row[1] else {}
            })
        return trips

class TripProcessor:
    """Core trip processing logic"""
    
    def __init__(self, db: LocalDatabase):
        self.db = db
        self.processing_queue = deque()
        self.metrics_cache = {}
        
    def process_event(self, event_data: Dict) -> Dict:
        """Process incoming trip event"""
        try:
            # Validate event structure
            if not self._validate_event(event_data):
                return {"status": "error", "message": "Invalid event structure"}
            
            # Create event object
            event = self._create_event_object(event_data)
            
            # Store event for lineage
            event_id = self.db.insert_event(event)
            
            # Process based on event type
            if event.event_type == "trip_start":
                result = self._process_trip_start(event)
            elif event.event_type == "trip_end":
                result = self._process_trip_end(event)
            else:
                return {"status": "error", "message": f"Unknown event type: {event.event_type}"}
            
            logger.info(f"Processed {event.event_type} for trip {event.trip_id}")
            return {"status": "success", "event_id": event_id, "result": result}
            
        except Exception as e:
            logger.error(f"Error processing event: {str(e)}")
            return {"status": "error", "message": str(e)}
    
    def _validate_event(self, event_data: Dict) -> bool:
        """Validate event structure"""
        required_fields = ['event_type', 'trip_id', 'timestamp']
        return all(field in event_data for field in required_fields)
    
    def _create_event_object(self, event_data: Dict) -> TripEvent:
        """Create appropriate event object"""
        if event_data['event_type'] == 'trip_start':
            return TripStartEvent(**event_data)
        elif event_data['event_type'] == 'trip_end':
            return TripEndEvent(**event_data)
        else:
            return TripEvent(**event_data)
    
    def _process_trip_start(self, event: TripStartEvent) -> Dict:
        """Process trip start event"""
        trip_data = {
            'trip_id': event.trip_id,
            'status': 'in_progress',
            'trip_start_data': event.to_dict(),
            'created_at': datetime.utcnow().isoformat()
        }
        
        self.db.update_trip_state(trip_data)
        return {"action": "trip_started", "trip_id": event.trip_id}
    
    def _process_trip_end(self, event: TripEndEvent) -> Dict:
        """Process trip end event"""
        # Get existing trip state
        trip_state = self.db.get_trip_state(event.trip_id)
        
        if not trip_state:
            # Orphaned trip end - create minimal state
            trip_state = {
                'trip_id': event.trip_id,
                'status': 'completed',
                'trip_start_data': None,
                'created_at': datetime.utcnow().isoformat()
            }
        
        # Update with trip end data
        completion_date = datetime.fromisoformat(event.timestamp.replace('Z', '+00:00')).date().isoformat()
        trip_state.update({
            'status': 'completed',
            'trip_end_data': event.to_dict(),
            'completion_date': completion_date
        })
        
        self.db.update_trip_state(trip_state)
        
        # Trigger daily aggregation update
        self._update_daily_metrics(completion_date)
        
        return {"action": "trip_completed", "trip_id": event.trip_id, "completion_date": completion_date}
    
    def _update_daily_metrics(self, date: str):
        """Update daily metrics for completed trips"""
        trips = self.db.get_completed_trips_by_date(date)
        
        if not trips:
            return
        
        # Extract fares from completed trips
        fares = []
        for trip in trips:
            if trip['trip_end_data']:
                fare = trip['trip_end_data'].get('final_fare', 0)
                if fare > 0:
                    fares.append(fare)
        
        if not fares:
            return
        
        # Calculate metrics
        metrics = {
            'date': date,
            'total_fare': sum(fares),
            'trip_count': len(fares),
            'average_fare': sum(fares) / len(fares),
            'max_fare': max(fares),
            'min_fare': min(fares),
            'generated_at': datetime.utcnow().isoformat()
        }
        
        # Store in database
        conn = self.db.get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO daily_metrics 
            (date, total_fare, trip_count, average_fare, max_fare, min_fare, generated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            metrics['date'],
            metrics['total_fare'],
            metrics['trip_count'],
            metrics['average_fare'],
            metrics['max_fare'],
            metrics['min_fare'],
            metrics['generated_at']
        ))
        conn.commit()
        conn.close()
        
        # Save to JSON file (simulating S3 output)
        output_file = f"analytics_output/daily_metrics_{date}.json"
        import os
        os.makedirs("analytics_output", exist_ok=True)
        
        with open(output_file, 'w') as f:
            json.dump(metrics, f, indent=2)
        
        logger.info(f"Updated daily metrics for {date}: {metrics['trip_count']} trips, ${metrics['total_fare']:.2f} total")

class MonitoringDashboard:
    """Simple monitoring for local development"""
    
    def __init__(self, db: LocalDatabase):
        self.db = db
        self.start_time = datetime.utcnow()
    
    def get_system_metrics(self) -> Dict:
        """Get current system metrics"""
        conn = self.db.get_connection()
        cursor = conn.cursor()
        
        # Get event counts
        cursor.execute("SELECT COUNT(*) FROM trip_events")
        total_events = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM trip_events WHERE event_type = 'trip_start'")
        trip_starts = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM trip_events WHERE event_type = 'trip_end'")
        trip_ends = cursor.fetchone()[0]
        
        # Get trip states
        cursor.execute("SELECT COUNT(*) FROM trip_state WHERE status = 'in_progress'")
        active_trips = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM trip_state WHERE status = 'completed'")
        completed_trips = cursor.fetchone()[0]
        
        # Get recent metrics
        cursor.execute("SELECT * FROM daily_metrics ORDER BY date DESC LIMIT 1")
        latest_metrics = cursor.fetchone()
        
        conn.close()
        
        uptime = datetime.utcnow() - self.start_time
        
        return {
            'system': {
                'uptime_seconds': int(uptime.total_seconds()),
                'uptime_formatted': str(uptime).split('.')[0]
            },
            'events': {
                'total_events': total_events,
                'trip_starts': trip_starts,
                'trip_ends': trip_ends
            },
            'trips': {
                'active_trips': active_trips,
                'completed_trips': completed_trips
            },
            'latest_daily_metrics': {
                'date': latest_metrics[0] if latest_metrics else None,
                'total_fare': latest_metrics[1] if latest_metrics else 0,
                'trip_count': latest_metrics[2] if latest_metrics else 0,
                'average_fare': latest_metrics[3] if latest_metrics else 0
            } if latest_metrics else None
        }

# Flask API for event ingestion
app = Flask(__name__)
app.json.sort_keys = False

# Initialize components
db = LocalDatabase()
processor = TripProcessor(db)
monitoring = MonitoringDashboard(db)

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({"status": "healthy", "timestamp": datetime.utcnow().isoformat()})

@app.route('/events', methods=['POST'])
def ingest_event():
    """Ingest trip event"""
    try:
        event_data = request.get_json()
        if not event_data:
            return jsonify({"error": "No JSON data provided"}), 400
        
        result = processor.process_event(event_data)
        
        if result['status'] == 'success':
            return jsonify(result), 200
        else:
            return jsonify(result), 400
            
    except Exception as e:
        logger.error(f"API error: {str(e)}")
        return jsonify({"error": "Internal server error"}), 500

@app.route('/metrics', methods=['GET'])
def get_metrics():
    """Get system metrics"""
    try:
        metrics = monitoring.get_system_metrics()
        return jsonify(metrics), 200
    except Exception as e:
        logger.error(f"Metrics error: {str(e)}")
        return jsonify({"error": "Could not retrieve metrics"}), 500

@app.route('/trip/<trip_id>', methods=['GET'])
def get_trip_status(trip_id):
    """Get trip status"""
    try:
        trip_state = db.get_trip_state(trip_id)
        if trip_state:
            return jsonify(trip_state), 200
        else:
            return jsonify({"error": "Trip not found"}), 404
    except Exception as e:
        logger.error(f"Trip lookup error: {str(e)}")
        return jsonify({"error": "Could not retrieve trip"}), 500

@app.route('/analytics/<date>', methods=['GET'])
def get_daily_analytics(date):
    """Get daily analytics for a specific date"""
    try:
        conn = db.get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM daily_metrics WHERE date = ?", (date,))
        row = cursor.fetchone()
        conn.close()
        
        if row:
            metrics = {
                'date': row[0],
                'total_fare': row[1],
                'trip_count': row[2],
                'average_fare': row[3],
                'max_fare': row[4],
                'min_fare': row[5],
                'generated_at': row[6]
            }
            return jsonify(metrics), 200
        else:
            return jsonify({"error": "No data for this date"}), 404
    except Exception as e:
        logger.error(f"Analytics error: {str(e)}")
        return jsonify({"error": "Could not retrieve analytics"}), 500

if __name__ == '__main__':
    logger.info("Starting NSP Bolt Ride Local Processing System")
    logger.info("API endpoints:")
    logger.info("  POST /events - Ingest trip events")
    logger.info("  GET /metrics - System metrics")
    logger.info("  GET /trip/<trip_id> - Trip status")
    logger.info("  GET /analytics/<date> - Daily analytics")
    logger.info("  GET /health - Health check")
    
    app.run(host='0.0.0.0', port=5000, debug=True)