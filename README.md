# SHAHED - Live Map Insights 🌍🇱🇧

SHAHED is a real-time geospatial mapping platform designed to transform unstructured reports from Telegram channels into structured geographic insights about events across Lebanon.
The project combines **Telegram data collection, AI-powered information extraction, geographic processing, and a web-based interactive map** to turn large amounts of unstructured Arabic reports into searchable and visualized location-based data.
check it at **http://https://shahedlebanon.com/**
**🟢 Deployed: March 7, 2026**
## 🚀 How SHAHED Works

The core idea behind SHAHED is to automate the process of going from an unstructured message to a geographic point on a map.

```text
Telegram Channels
       │
       ▼
  Telethon / Python
       │
       ▼
 Arabic / Unstructured Reports
       │
       ▼
      Llama 3
       │
       │  Extract location / village information
       ▼
 Data Processing & Normalization
       │
       ▼
 Geographic Coordinates
       │
       ▼
   FastAPI Backend
       │
       ▼
 Interactive Map
```

### 1. Collecting Telegram Data

SHAHED uses **Telethon** to interact with Telegram channels and collect relevant messages.

The collected messages are primarily unstructured Arabic reports. A single message may contain information about an event without following a consistent format, which makes traditional rule-based extraction difficult.

The backend therefore processes the messages before they can be represented geographically.

### 2. AI-Powered Location Extraction

One of the main components of SHAHED is the use of **Llama 3** for extracting location information from the collected messages.

Instead of relying only on fixed keywords or manually defined rules, the model is used to interpret the text and identify Lebanese villages or locations mentioned in reports.

```text
Raw Telegram Message
        │
        ▼
     Llama 3
        │
        ▼
Extracted Location
        │
        ▼
Normalized Village Name
```

This allows SHAHED to work with variations in Arabic wording and unstructured reports.

### 3. Geographic Processing

After identifying a location, the application associates it with geographic information such as:

* Arabic village name
* English village name
* Latitude
* Longitude
* Number of reported events
* Original source message

Locations can then be aggregated so that multiple reports referring to the same village are represented together.

For example:

```text
Village A
├── Report 1
├── Report 2
├── Report 3
└── Report 4

        ↓

Village A
Attack/Event Count: 4
Coordinates: latitude / longitude
```

### 4. FastAPI Data Layer

The processed information is exposed through a **FastAPI** application.

The API provides the data required by the SHAHED map, including geographic events, statistics, and user-submitted testimonies.

The frontend can therefore request structured JSON instead of processing the original Telegram messages itself.

## 🗺️ Map Visualization

The frontend consumes the processed geographic data and displays it on an interactive map.

Each location is represented using its geographic coordinates.

The visualization also aggregates events by location, allowing areas with a higher number of reported events to be visually distinguished.

Users can:

* Explore locations on the map
* Search for villages
* View the number of reported events
* Open detailed information for a location
* View the latest source message
* View submitted testimonies
* Submit a testimony associated with a location

## 🧠 AI + Geospatial Pipeline

The main technical concept of SHAHED is the combination of **Natural Language Processing and Geographic Information Systems**.

```text
Unstructured Arabic Text
          │
          ▼
      Llama 3
          │
          ▼
   Location Extraction
          │
          ▼
 Name Normalization
          │
          ▼
 Geographic Coordinates
          │
          ▼
 Event Aggregation
          │
          ▼
      FastAPI
          │
          ▼
 Interactive Map
```

This transforms information that is difficult to analyze as raw text into a spatial representation that can be explored visually.

## 🛠️ Technology Stack

### Data Collection

* Python
* Telethon
* Telegram API

### AI / NLP

* Llama 3
* Arabic text processing
* Location/entity extraction

### Backend

* Python
* FastAPI
* Uvicorn
* Pydantic

### Geospatial Visualization

* Interactive web map
* Geographic coordinates
* Location-based aggregation

### Deployment
Docker
Procfile-based deployment

### Main Components

**`main.py`**

The main FastAPI application and API entry point. It exposes the processed SHAHED data to the frontend.

**`models.py`**

Contains data models used by the application for structuring and validating data.

**`live.py`**

Handles the live data processing workflow and supports the collection/processing of incoming reports.

**`history.py`**

Handles historical data and previously processed reports.

**`historybackup.py`**

Contains supporting/backup functionality related to historical processing.

**`reprocess.py`**

Used for reprocessing existing data when extraction or processing needs to be performed again.

**`south.py`**

Contains processing related to the southern Lebanon data workflow.

**`testfound.py`**

Testing and experimentation related to identifying/extracting locations from collected data.

**`Dockerfile`**

Defines the containerized environment used to run the application.

**`Procfile`**

Provides the process definition used by the deployment environment.

## 🔄 Data Processing Philosophy

A major challenge in SHAHED is that the source data is not designed as structured geographic data.

Telegram messages can contain:

* Different Arabic spellings for the same village
* Informal descriptions
* Multiple locations in a single message
* Inconsistent formatting
* Context-dependent location references

SHAHED therefore uses an AI-assisted extraction stage before geographic visualization.

This makes the system more flexible than relying entirely on manually maintained lists of keywords and locations.

## 📊 From Reports to Insights

The transformation performed by SHAHED can be summarized as:

```text
Raw Data
   ↓
Telegram Messages
   ↓
Python / Telethon
   ↓
Llama 3 Extraction
   ↓
Structured Location Data
   ↓
Geographic Processing
   ↓
Event Aggregation
   ↓
FastAPI
   ↓
Interactive Geospatial Visualization
```

The result is a system where users can move from **individual reports** to a **geographic overview of reported events across Lebanon**.

## 🎯 Project Purpose

SHAHED was developed as a prototype to explore how AI and geospatial technologies can be combined to make large amounts of unstructured information easier to analyze.

The project demonstrates a practical pipeline for:

* Collecting real-world data
* Processing Arabic natural language
* Extracting geographic entities using an LLM
* Converting extracted information into geographic data
* Aggregating events by location
* Serving processed information through an API
* Visualizing the results on an interactive map

## 👤 Author

Built by **Mariam Sleiman** as a project exploring AI-powered data processing, backend systems, natural language processing, and geospatial visualization.
