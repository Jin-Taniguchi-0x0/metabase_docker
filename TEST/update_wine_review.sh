#!/bin/bash
# Update running database
# Use the CONTAINER_NAME from .env or default 'data_db'
# We'll use 'metabase-data_db-1' or just rely on 'docker-compose exec' which handles service names

echo "Updating wine_review table in the running database..."

# Add column
docker-compose exec -T data_db psql -U data_user -d data_db -c "ALTER TABLE wine_review ADD COLUMN IF NOT EXISTS points_per_price FLOAT;"

# Update values
docker-compose exec -T data_db psql -U data_user -d data_db -c "UPDATE wine_review SET points_per_price = ROUND(points::numeric / NULLIF(price::numeric, 0), 2);"

echo "Update complete. Verifying..."
docker-compose exec -T data_db psql -U data_user -d data_db -c "SELECT points, price, points_per_price FROM wine_review LIMIT 5;"
