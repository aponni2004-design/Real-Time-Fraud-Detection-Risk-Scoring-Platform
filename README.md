# Real-Time Fraud Detection & Risk Scoring Platform

## Project Overview

A real-time data engineering platform designed to ingest, process, analyze, and visualize simulated banking/UPI transactions.

The platform uses streaming data processing, fraud detection rules, Medallion architecture, a data warehouse, and business intelligence dashboards.

## Architecture

Python Transaction Producer
        ↓
      Kafka
        ↓
PySpark Structured Streaming
        ↓
Bronze → Silver → Gold
        ↓
PostgreSQL Data Warehouse
        ↓
Power BI

MongoDB is used for device and metadata storage.

## Technologies

- Python
- Apache Kafka
- Docker
- PySpark
- PostgreSQL
- MongoDB
- Power BI
- Git