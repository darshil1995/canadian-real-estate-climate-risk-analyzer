# 1. Use an official Python base image
FROM python:3.11-slim

# 2. Install System Dependencies
# 'default-jre-headless' provides the latest stable Java for Spark.
# 'procps' is added because Spark requires it for process management.
RUN apt-get update && apt-get install -y \
    default-jre-headless \
    build-essential \
    procps \
    curl \
    && apt-get clean

# 3. Set Environment Variables
# We use the generic 'default-java' path so it works even if the version updates.
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$PATH:$JAVA_HOME/bin
ENV PYTHONPATH=/app
ENV IS_DOCKER=true

# 4. Set Working Directory
WORKDIR /app

# 5. Install Python Dependencies
# Copy requirements first to utilize Docker layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 6. Copy the entire project
COPY . .

# 7. Expose the Streamlit port
EXPOSE 8501

# 8. Command to run the dashboard
CMD ["streamlit", "run", "src/ui/dashboard.py", "--server.port=8501", "--server.address=0.0.0.0"]