# Distributed Search Engine

## Overview

This project implements a full-stack distributed search engine for the Simple English Wikipedia. It features a microservices architecture that decouples offline data processing from online query serving.

Key components include:
1.  **Distributed Indexing**: A MapReduce-style pipeline to build an inverted index using PostgreSQL JSONB for storage.
2.  **Distributed PageRank**: A Bulk Synchronous Parallel (BSP) graph processing engine using Redis for coordination.
3.  **Hybrid Ranking**: A retrieval model combining BM25 relevance scores with PageRank authority scores.
4.  **Search Interface**: A RESTful API backend and a web-based frontend.

## Prerequisites

Ensure the following are installed and running on your system:
* **Docker Desktop** (Required for container orchestration)
* **Python 3.12** (Required to run automation scripts; no external pip packages needed for the scripts themselves)

## How to Run

You can run the system in two modes: **Quick Demo** (recommended for evaluation) or **Full Pipeline** (re-computation from scratch).

### Option 1: Quick Verification (Toy Dataset)
Approximate time: ~3 minutes
This mode executes the **full distributed pipeline** (Ingestion -> Indexing -> PageRank -> Serving) using a small subset of data (5000 pages). It allows you to verify the system logic in seconds without waiting for the full dataset.

1.  Open a terminal in the project root and run:
    ```bash
    python run_full_pipeline.py --file data/raw/simplewiki-toy.xml
    ```
2.  **Note:** To see the list of articles included in this toy dataset (useful for testing search queries), please check:
    > `data/raw/simplewiki-toy-titles.txt`

3.  The script will automatically clean the environment, compute everything from scratch, and launch web services.
4.  Access the application:
    * **Frontend UI**: http://localhost:8501
    * **Backend API Docs**: http://localhost:8000/docs

### Option 2: Full Pipeline Execution
Approximate time: ~40 minutes depending on your machine.
This mode performs the entire ETL process from scratch: downloading raw XML, cleaning data, building the index, calculating PageRank, and deploying.

1.  Open a terminal in the project root and run:
    ```bash
    python run_full_pipeline.py
    ```
2.  The script will execute the following stages sequentially:
    * Download and extract raw Wikipedia XML.
    * **Ingestion**: Clean XML and convert to JSONL format.
    * **Indexing**: Run Mapper and Reducer containers to build the inverted index.
    * **PageRank**: Run Controller and Worker containers to compute authority scores.
    * **Export**: Persist metadata and scores to PostgreSQL.
    * **Deploy**: Start backend and frontend services.

## Project Structure

* **compute/**: Contains core logic for distributed tasks (Indexing Mappers/Reducers, PageRank Controller/Workers, and DB utilities).
* **serving/**: Contains the FastAPI backend and Search Engine ranking logic.
* **frontend/**: Contains the Streamlit web interface.
* **ingestion/**: Scripts for parsing and cleaning raw XML data.
* **data/**: Directory for storing raw data, intermediate files, and database volumes.
* **evaluation/**: evaluation models and scripts.
* **logs/**: Directory for storing log files.
* **tests/**: random tests scripts
* **docker-compose.yml**: Defines the multi-container architecture (Redis, Postgres, Compute Nodes, Web Services).
* **run_demo.py**: Script to run the Quick Demo mode.
* **run_full_pipeline.py**: Script to run the Full Pipeline mode.


## Architecture

* **Storage**: PostgreSQL (Persistent storage for Index, Metadata, PageRank), Redis (Task queues and graph state).
* **Communication**: Docker Network for internal service communication; Redis for task scheduling.
* **Text Processing**: NLTK for tokenization and stemming; mwparserfromhell for Wiki markup parsing.