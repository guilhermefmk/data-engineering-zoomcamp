* Comandos

** DOCKER

    docker run -it ubuntu bash
    docker run -it python:3.9
    docker run -it --entrypoint=bash python:3.9
    docker build -t test:pandas .
    docker run -it test:pandas   
    docker ps
    docker rm $(docker ps -a -q)
    docker exec -it nomedocontainer /bin/bash 

    docker run -it \
    -e POSTGRES_USER="root" \
    -e POSTGRES_PASSWORD="root" \
    -e POSTGRES_DB="ny_taxi" \
    -v /home/guilherme.cunha/Área\ de\ Trabalho/WORKSPACE/data-engineering-zoomcamp/data/ny_taxi_postgres_data:/var/lib/postgresql/data \
    -p 5432:5432 \
    --network=pg-network \
    --name pg-database \
    postgres:13

    docker run -it \
    -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
    -e PGADMIN_DEFAULT_PASSWORD="root" \
    -p 8080:80 \
    --network=pg-network \
    --name pgadmin \
    dpage/pgadmin4

    docker network create pg-network

    pgcli -h localhost -p 5432 -u root -d ny_taxi

    url="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"
    url="https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv"
    url="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz"

    python3 ingest_data.py \
        --user=root \
        --passwd=root \
        --host=localhost \
        --port=5432 \
        --db=ny_taxi \
        --table_name=yellow_taxi_trips \
        --url=${url}


    docker build -t taxi_ingest:v001 .

    docker run -it --network=data-engineering-zoomcamp_default taxi_ingest:v001 --user=root --passwd=root --host=pg-database --port=5432 --db=ny_taxi --table_name=yellow_taxi_trips --url=${url}

https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
https://www1.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf