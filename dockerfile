# Use an official PySpark image
FROM jupyter/pyspark-notebook:spark-3.3.0

# Set the working directory inside the container
WORKDIR /app

# Copy your project files (if any) into the container
COPY . /app

# Install additional dependencies (if needed)
RUN pip install --no-cache-dir pandas numpy

# Set PySpark environment variables
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Default command: Start a Jupyter Notebook (Optional)
CMD ["start-notebook.sh", "--NotebookApp.token=''"]