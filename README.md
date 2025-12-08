# Real-Time Stock Data Pipeline with Spark ML and Angular

This project demonstrates a full-stack, real-time data pipeline for processing, analyzing, and visualizing stock data. It uses Kafka for message queuing, Spark Streaming for processing and ML model training, PostgreSQL for data storage, a Flask backend API, and an Angular frontend for visualization.

---

## Architecture

The application follows a decoupled, microservice-based architecture orchestrated by Docker Compose:

- **Producer (producer/):** A Python script that generates simulated stock price data for a predefined list of symbols and sends it to Kafka.
- **Kafka (docker-compose.yml):** Acts as the central message bus, decoupling the producer from the Spark processor. Uses the official Confluent Kafka images, including Zookeeper.
- **Spark Streaming (spark/):** A PySpark application that:
  - Reads raw data from Kafka in micro-batches (every 5 seconds).
  - Calculates the average stock price over a 5-second window.
  - Saves the aggregated data to PostgreSQL.
  - Conditionally (every 5 minutes): Fetches the full history for active stocks from PostgreSQL, retrains a simple Linear Regression model using Spark MLlib, and saves the updated model.
- **PostgreSQL (docker-compose.yml):** Stores the aggregated time-series data processed by Spark. Also used by Spark to fetch historical data for model training.
- **Backend API (backend/):** A Flask application that:
  - Provides a REST API endpoint (`/api/stock_data`) to serve the latest aggregated data (last 24 hours) from PostgreSQL to the frontend.
  - Provides a REST API endpoint (`/api/future_trends`) that loads the latest Spark MLlib model for a requested stock and generates future price predictions. It includes an embedded SparkSession for this purpose.
- **Angular Frontend (bigdata-dashboard/):** A standalone Angular application (run separately) that:
  - Connects to the Backend API to fetch and display real-time data using Chart.js.
  - Allows users to select specific stocks or view all active stocks.
  - Provides functionality to request and display future price predictions from the backend.

---

<img width="1200" height="750" alt="Picture1" src="https://github.com/user-attachments/assets/7d8bd258-9ddb-4db2-8e22-f07e7246aeea" />

## Prerequisites

Before you begin, ensure you have the following installed on your system:

- **Docker Desktop:** To build and run the containerized backend services. [Download Docker](https://www.docker.com/products/docker-desktop)  
- **Git:** To clone the repository.  
- **Node.js and npm:** (Recommended LTS version) To run the Angular frontend. [Download Node.js](https://nodejs.org/)  
- **Angular CLI:** Install globally if you don't have it:  
```bash
npm install -g @angular/cli
```

---

## Setup

1. **Clone the Repository:**
```bash
git clone -b temp-work https://github.com/Itz-mehanth/LiveStock.git
cd LiveStock
```

2. **Ensure .gitignore Files:**  
Make sure you have appropriate `.gitignore` files in the root directory, `backend/`, `producer/`, `spark/`, and especially `bigdata-dashboard/` (to ignore `node_modules`).

---

## Running the Application

The application consists of **backend services** (run via Docker Compose) and the **frontend application** (run via Angular CLI).

### 1. Running the Backend (Docker Compose)

This single command builds Docker images and starts all backend services in the correct order:

```bash
cd LiveStock
docker-compose up
```

- First run may take several minutes to download base images and build custom service images.  
- Wait until logs indicate Spark has connected to Kafka and the Backend API has started (e.g., `Running on http://0.0.0.0:5000`).  

- View logs:  
```bash
docker-compose logs -f
```

#### Stopping the Backend:
```bash
docker-compose down
```

#### Resetting Data:
```bash
docker-compose down -v
```
This clears all historical data and resets Postgres and Kafka volumes.

---

### 2. Running the Frontend (Angular CLI)

1. Open a new terminal window.
2. Navigate to the Angular project directory:
```bash
cd LiveStock/bigdata-dashboard-bu
```
3. Install dependencies:
```bash
npm install
```
4. Start the Angular development server:
```bash
ng serve
```
5. Open your browser at [http://localhost:4200](http://localhost:4200)  

> The dashboard will update in near real-time as Spark processes batches.

---

## Configuration

- **Stock Symbols:** Modify `STOCK_SYMBOLS` list in `producer/producer.py`.  
  - Remember to reset data: `docker-compose down -v && docker-compose up`.
- **Spark Processing Interval:** Adjust window duration and trigger interval in `spark/spark_streaming.py`.
- **Model Training Interval:** Modify `TRAINING_INTERVAL_SECONDS` in `spark/spark_streaming.py`.
---

##Screenshot
<img width="1668" height="906" alt="Screenshot 2025-12-05 184746" src="https://github.com/user-attachments/assets/3d50ba89-f63b-4d0c-b3f5-ea843f58eb0f" />

