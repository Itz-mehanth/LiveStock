# Use the official Apache Spark image as the base
FROM apache/spark:3.5.1

# Switch to the root user to have permissions to install packages
USER root

# Copy the requirements file into a temporary location
COPY requirements.txt /tmp/

# Install the Python dependencies system-wide
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Switch back to the default non-root spark user for security
USER 185

