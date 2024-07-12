# Pull the latest Selenium Chrome image
docker pull selenium/standalone-chrome

# Run the Selenium Chrome container
docker run -d --name chrome -p 4444:4444 -p 7900:7900 --shm-size="2g" -e SE_NODE_MAX_SESSIONS=12 selenium/standalone-chrome

# Pull the latest Puckel Docker Airflow image
docker pull puckel/docker-airflow

# Run the Airflow container
docker run -d --name workflow -v "${PWD}/requirements.txt:/requirements.txt" -p 8080:8080 puckel/docker-airflow webserver

# Copy the dags directory to the Airflow container
docker cp dags/ workflow:/usr/local/airflow/dags/
