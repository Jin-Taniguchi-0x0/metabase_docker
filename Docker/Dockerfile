FROM postgres:10.3

COPY dvdrental.tar /tmp/
COPY create_db.sh /docker-entrypoint-initdb.d/

# docker exec -it postgres psql -U metabase -d postgres -c "CREATE DATABASE dvdrental;"
# docker cp ./dvdrental/restore.sql postgres:/restore.sql
# docker exec -it postgres psql -U metabase -d dvdrental -f /restore.sql
# docker exec -it postgres psql -U metabase -d dvdrental

#tarをvolumeにアップロード
#docker run --rm -v test_docker_compose_test_db:/data busybox sh -c "mkdir -p /data"
#docker run --rm -v test_docker_compose_test_db:/data -v ./dvdrental.tar:/tmp/my-archive.tar busybox sh -c "tar -xf /tmp/my-archive.tar -C /data"
