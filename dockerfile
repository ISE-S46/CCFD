# Use an official PySpark image
FROM jupyter/pyspark-notebook:spark-3.3.0

# Set the working directory inside the container
WORKDIR /app

# Copy your project files into the container
COPY . /app

# Install additional dependencies
RUN pip install --no-cache-dir pandas numpy scikit-learn kafka-python

# Set PySpark environment variables
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Expose ports for Jupyter Notebook and Spark UI
EXPOSE 8888 4040

# Default command: Start a Jupyter Notebook
CMD ["start-notebook.sh", "--NotebookApp.token=''"]