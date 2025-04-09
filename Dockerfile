# Use an official Python runtime as a parent image
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container at /app
COPY requirements.txt .

# Install build dependencies required for some packages (like lz4)
RUN apt-get update && apt-get install -y --no-install-recommends build-essential && rm -rf /var/lib/apt/lists/*

# Install any needed packages specified in requirements.txt
# Use --no-cache-dir to reduce image size
RUN pip install --no-cache-dir -r requirements.txt

# Optional: Clean up build dependencies to reduce image size
# RUN apt-get purge -y --auto-remove build-essential

# Copy the rest of the application code into the container at /app
COPY . .

# Make port 8000 available to the world outside this container
# (MCP servers typically run on port 8000 by default)
EXPOSE 8000

# Define environment variables (these will be overridden by docker run -e flags)
ENV DATABRICKS_HOST=""
ENV DATABRICKS_TOKEN=""
ENV DATABRICKS_HTTP_PATH=""

# Run main.py when the container launches
CMD ["python", "main.py"]