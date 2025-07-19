FROM quay.io/jupyter/pyspark-notebook:python-3.12

WORKDIR /app

COPY . /app

# Expose ports for Jupyter Notebook and Spark UI
EXPOSE 8888 4040

# Default command: Start a Jupyter Notebook
CMD ["start-notebook.sh", "--NotebookApp.token=''"]