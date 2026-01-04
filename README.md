# HPC Cluster Observability and Analytics System

## Overview
This project implements an end-to-end observability and analytics platform for HPC clusters by processing raw PBS scheduler logs.  
Designed and developed collaboratively by our team for the **SCC 2025 Nationals**, the system transforms unstructured log data into actionable insights through a robust ETL pipeline and interactive dashboard.

---

## Problem Statement
High-Performance Computing (HPC) clusters generate extensive scheduler logs that are difficult to analyze manually.  
Our system extracts, cleans, and aggregates these logs to provide meaningful metrics about cluster utilization, job scheduling efficiency, and resource bottlenecks over time.

---

## Features
- **ETL Pipeline:** Ingests and processes raw PBS scheduler logs spanning multiple years (~2017-2025), handling over 137,000 records.
- **Data Cleaning and Aggregation:** Normalizes log entries, calculates cluster-level metrics such as CPU and node utilization, job states, and queue dynamics.
- **Interactive Dashboard:** Built with Streamlit, enabling time-range filtering, dynamic visualizations, and detailed insights into cluster performance.
- **Production Deployment:** The dashboard is deployed both behind an Nginx reverse proxy for production-like environments and on **Streamlit Cloud** for easy remote access.
- **System and Application Log Integration:** Incorporates system logs alongside application logs for comprehensive observability.

---

## Tech Stack
- Python (Pandas, NumPy, Streamlit)
- Linux (Ubuntu)
- PBS Scheduler logs
- Nginx (web server / reverse proxy)
- Streamlit Cloud (cloud deployment)
- Docker (optional for deployment)
- Git (version control)

---

## Architecture
Raw PBS Logs → ETL Pipeline → Analytics Metrics → Streamlit Dashboard → Nginx Reverse Proxy / Streamlit Cloud → User Interface

---

## Dataset
- PBS scheduler logs collected from Lengau Cluster
- Data range: 2017 to 2025
- Approximately 137,000 log entries

---

## Installation & Setup

### Prerequisites
- Python 3.8+
- Streamlit
- Pandas, NumPy
- Nginx (for production deployment, optional)
- Linux environment recommended

### Run locally (development)
pip install -r requirements.txt
streamlit run app.py

### Production setup (simplified)
Configure Nginx reverse proxy to forward requests to Streamlit app
Alternatively, deploy the app on Streamlit Cloud for cloud hosting
Secure server environment

### Usage
Access the dashboard via http://localhost:8501 (development), the configured Nginx server address, or the Streamlit Cloud URL.
Alternatively on streamlit cloud it can be acced through 
## Link
https://9qhq9u9tsybkptfuu4qikw.streamlit.app/

Use the interactive controls to filter by date.
Visualize cluster performance trends over time and identify scheduling bottlenecks.
Key Metrics & Visualizations
CPU and node utilization heatmaps
Job lifecycle state distributions (running, queued, held, etc.)
Queue wait times and congestion analysis
Longitudinal resource usage trends
### Contributions
This was a collaborative project developed by a team of four(special thanks to 2 mentors assigned mentors and one team mentor), combining expertise in data engineering, HPC systems, and frontend visualization.

### Future Work
Real-time log ingestion and alerting
Integration with SLURM and other schedulers
Anomaly detection using ML techniques
Multi-cluster support and scalability improvements
Credits
Team Members: Senzokuhle Mokoena, Phumlani Mokoena, Emmanuel Sivhuwana and Unathi Nkosi
University of Zululand HPC group
Author
Senzokuhle Mokoena
Team Lead, HPC Analytics
