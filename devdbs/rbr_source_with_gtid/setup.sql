
-- Setup database and users
-- Reference http://y/runbook-add-new-db


CREATE DATABASE yelp DEFAULT CHARACTER SET utf8;

GRANT ALL ON *.* TO 'yelpdev'@'%';
