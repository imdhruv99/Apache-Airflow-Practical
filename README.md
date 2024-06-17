# Apache Airflow

- Since I am using windows, I am running apache airflow as a docker compose.
- [Official Docker Compose File](https://airflow.apache.org/docs/apache-airflow/2.9.2/docker-compose.yaml). You will need Docker Compose version > 2.14.0
- [Official Documentation for Docker Compose](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)


## Instruction to Run Airflow
- Standard docker compose command will pull all the images and run the containers. This will take 5 mins, depending upon the computer.
    ```
    docker-compose up
    ```
- Additionally, You can run monitoring app named as `flower`.
    ```
    docker compose --profile flower up
    ```
- <p style="color:red;"><b>Note:</b> Respect your computer as Airflow containers will consume around or more than 8GB of your computer's RAM. So If you don't have 16GB ram, please don't setup Airflow. For me, all the containers are consuming around 10GB of RAM.</p>

- Default Username & Password is `airflow`
- You can create your own password with the help of below command
    ```
    airflow users  create --role Admin --username admin --email admin --firstname admin --lastname admin --password admin
    ```

- Once all the containers are up and running, you can access the Airflow dashboard at `localhost:8080`

- One all the containers are running, It will create four directories at your docker-compose file location. Directory names are `config`, `dags`, `logs` and `plugins`.

## Instruction to Run DAGS

- Create dags under `dags` directory and compose will automatically get refreshed.
- Go to UI and run the dag by pressing run button present on right side.