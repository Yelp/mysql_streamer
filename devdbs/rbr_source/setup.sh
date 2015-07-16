# This script comes from the base image. It starts mysqld in the background
bash /opt/startup.sh

# Create our database and users
cat setup.sql | mysql

# Create tables
cat tables/create_business_table_add_rows.sql | mysql yelp
cat tables/heartbeat.sql | mysql yelp_heartbeat

mysqladmin shutdown
