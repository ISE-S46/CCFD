Docker Container set up(Window):
docker build -t pyspark_env .
docker run -it -p 8888:8888 -v ${pwd}:/app --name pyspark_env pyspark_env
