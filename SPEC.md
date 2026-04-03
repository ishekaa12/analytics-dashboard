# Privacy-First Analytics Engine - Specification

## Project Overview
- **Project Name**: Privacy-First Analytics Engine
- **Type**: FastAPI Python Backend
- **Core Functionality**: Privacy-respecting web analytics with bot filtering, event collection, and real-time streaming
- **Target Users**: Website owners who want privacy-focused analytics

## Functionality Specification

### Core Features

#### 1. Event Collection (`POST /collect`)
- Receives analytics events with fields: `page_url`, `referrer`, `user_agent`, `timestamp`, `screen_width`, `screen_height`, `click_x`, `click_y`
- **Bot Filtering**:
  - User-agent pattern matching (bot/crawler/spider patterns)
  - Timing check: reject if timestamp is in future or too old (>24h)
  - Request frequency check: reject if >100 requests/minute from same IP
- Writes events to SQLite via SQLAlchemy
- Returns 202 Accepted on success

#### 2. Hourly Stats (`GET /stats`)
- Returns pageviews aggregated by hour for last 24 hours
- Uses pre-aggregated hourly summary table (never queries raw events)
- Returns JSON: `{"hour": "2026-04-03T14:00", "pageviews": 123, "unique_visitors": 45}`

#### 3. Referrers (`GET /referrers`)
- Returns top 10 referrer sources
- Uses pre-aggregated hourly table
- Returns JSON: `[{"referrer": "google.com", "count": 500}, ...]`

#### 4. Funnel Analysis (`GET /funnel?steps=url1,url2,url3`)
- Takes comma-separated URL paths as steps
- Calculates drop-off counts per step
- Returns JSON: `[{"step": "/pricing", "visitors": 100}, {"step": "/signup", "visitors": 50}, {"step": "/checkout", "visitors": 20}]`

#### 5. Click Heatmap (`GET /heatmap?page=url`)
- Returns all click x/y coordinates for a given page
- Uses pre-aggregated hourly table
- Returns JSON: `[{"x": 100, "y": 200}, {"x": 150, "y": 250}, ...]`

#### 6. Live Events (`GET /live`)
- Server-Sent Events (SSE) streaming
- Streams new events in real-time using asyncio
- Uses asyncio.Queue for event broadcasting

#### 7. Geo Location (`GET /geo`)
- On event ingestion, performs IP lookup via ip-api.com
- Stores lat/lng in database
- Returns JSON: `[{"lat": 40.7128, "lng": -74.0060, "count": 50}, ...]`

### Database Schema

#### Raw Events Table (`events`)
- id: Integer, Primary Key
- page_url: String
- referrer: String
- user_agent: String
- ip_address: String
- screen_width: Integer
- screen_height: Integer
- click_x: Integer
- click_y: Integer
- timestamp: DateTime
- is_bot: Boolean
- country: String (nullable)
- lat: Float (nullable)
- lng: Float (nullable)

#### Hourly Summary Table (`hourly_summary`)
- id: Integer, Primary Key
- hour: DateTime
- page_url: String
- pageviews: Integer
- unique_visitors: Integer
- clicks: Integer

#### Referrer Summary Table (`referrer_summary`)
- id: Integer, Primary Key
- hour: DateTime
- referrer: String
- count: Integer

#### Click Data Table (`clicks_hourly`)
- id: Integer, Primary Key
- hour: DateTime
- page_url: String
- x: Integer
- y: Integer
- count: Integer

#### Geo Summary Table (`geo_summary`)
- id: Integer, Primary Key
- hour: DateTime
- lat: Float
- lng: Float
- count: Integer

### Acceptance Criteria
1. POST /collect filters bots and stores valid events
2. GET /stats returns hourly data from summary table
3. GET /referrers returns top 10 referrers
4. GET /funnel calculates drop-offs correctly
5. GET /heatmap returns click coordinates
6. GET /live streams events via SSE
7. GET /geo returns lat/lng of recent visitors
8. All dashboard queries use pre-aggregated tables only
