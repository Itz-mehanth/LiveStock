# Use an official Python runtime as a parent image
FROM python:3.9-slim

# --- Install Java Development Kit (JDK) for PySpark ---
# Update package lists and install default JDK without interactive prompts
RUN apt-get update && \
    apt-get install -y default-jdk && \
    apt-get clean

# --- Set the JAVA_HOME environment variable for PySpark to find Java ---
ENV JAVA_HOME /usr/lib/jvm/default-java

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the backend application code
COPY . .

# Expose the port the app runs on
EXPOSE 5000

# The command to run when the container starts
CMD ["python", "app.py"]