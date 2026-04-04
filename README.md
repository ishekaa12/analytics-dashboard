<div align="center">

# 📊 Privacy-First Analytics Engine
  
<p align="center">
  <img src="https://img.shields.io/badge/Python-3.10%2B-blue.svg" alt="Python Version">
  <img src="https://img.shields.io/badge/FastAPI-0.100%2B-00a393.svg" alt="FastAPI">
  <img src="https://img.shields.io/badge/SQLite-Native-003B57.svg" alt="SQLite">
  <img src="https://img.shields.io/badge/License-MIT-green.svg" alt="License">
</p>

**A lightweight, high-performance, and privacy-focused web analytics platform built with Python (FastAPI) and vanilla JavaScript.** <br>
Track pageviews, unique visitors, click heatmaps, and funnel drop-offs without relying on cookies.

</div>

---

## Features

- ** Privacy-First By Design**: No cookies. Zero reliance on invasive tracking techniques. Unique visitors are aggregated securely using cryptographically hashed IPs and hourly tumbling windows.
- ** Multi-Tenant Architecture**: Out-of-the-box support for distinct namespaces, making it trivial to track multiple websites (e.g., Luma, Substack) from a single unified backend.
- ** Aggressive Bot Filtering**: Intelligently screens and discards bot traffic based on HTTP user-agents, request velocities, and behavioral heuristics.
- **⚡ Real-Time SSE Feed**: See your visitors navigate your site live via Server-Sent Events (SSE). 
- ** Comprehensive Visuals**:
  - Interactive Leaflet.js Geographic world maps.
  - Heatmap visualizers layered dynamically onto your site's screenshots.
  - Step-by-step conversion tracking funnels.
- ** Aesthetic Pixel-Art Hub**: A retro gaming-inspired dashboard hub that organizes all of your tracked sites in one highly aesthetic view.

## 📂 Project Structure

```text
analytics-dashboard/
├─ main.py                 # Core FastAPI Backend application. Handles routing & aggregation.
├─ tracker.js              # Lightweight vanilla JS tracking snippet for client websites.
├─ analytics.db            # Local SQLite database (Auto-generated).
├─ requirements.txt        # Python backend dependencies.
├─ populate_all.py         # Mock data generator for testing UI components.
├─ frontend/
│  ├─ index.html           # Unified Homepage Hub (Pixel Art styled).
│  └─ dashboard/           # Specific dashboard client views.
│     ├─ luma/dashboard.html
│     └─ substack/dashboard.html
└─ static/                 # Static assets for the dashboard (scripts, maps, css).
   ├─ dashboard.js         # Vanilla JS powering the charts & SSE live feeds.
   ├─ dashboard.css        # Dashboard styling.
   └─ ...
```

##  Getting Started

### Prerequisites

Ensure you have **Python 3.10+** installed on your machine.

### Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/yourusername/analytics-dashboard.git
   cd analytics-dashboard
   ```

2. **Install Python dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Generate Mock Data (Optional):**
   If you want to preview the visualizations with dummy data prior to a real deployment:
   ```bash
   python populate_all.py
   ```

4. **Boot up the Server:**
   ```bash
   uvicorn main:app --reload --port 8000
   ```

5. **Explore:** 
   Open up `http://localhost:8000/` in your browser.

##  Integrating the Tracker

Adding the engine to your website is trivial. Embed `tracker.js` into your HTML `<head>` and modify the internal `endpoint` variable within the script to point to your live analytics domain.

```html
<!-- Example injection -->
<script src="https://your-analytics-domain.com/tracker.js"></script>
```

The vanilla tracker operates completely silently in the background, pinging out payloads of page navigations, viewport metadata, and `x/y` click coordinates utilizing the non-blocking `Navigator.sendBeacon` API.

##  Built With

* [FastAPI](https://fastapi.tiangolo.com/) &mdash; High performance asynchronous web framework.
* [SQLAlchemy](https://www.sqlalchemy.org/) &mdash; ORM mapped to local AIOSQLite instances.
* [Chart.js](https://www.chartjs.org/) &mdash; Responsive frontend data visualizations.
* [Leaflet.js](https://leafletjs.com/) &mdash; Geographic map rendering.

##  License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

<div align="center">
  <i>Built with privacy in mind. No cookies. Open Source.</i>
</div>
