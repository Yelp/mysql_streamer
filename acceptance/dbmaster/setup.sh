# This script comes from the base image. It starts mysqld in the background
bash /opt/startup.sh

# Create our database and users
echo "CREATE DATABASE yelp DEFAULT CHARACTER SET utf8;" | mysql
echo "GRANT ALL ON *.* TO 'yelpdev'@'%';" | mysql

mysqladmin shutdown
