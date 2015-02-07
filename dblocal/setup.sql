
-- Setup database and users
-- Reference http://y/runbook-add-new-db


CREATE DATABASE yelp DEFAULT CHARACTER SET utf8;

GRANT ALL ON *.* TO 'yelpdev'@'%';
use yelp;

CREATE TABLE `business` (
      `id` int(11) NOT NULL auto_increment,
      `acxiom_id` int(11) default NULL,
      `name` varchar(64) collate utf8_unicode_ci default NULL,
      `address1` varchar(128) collate utf8_unicode_ci default NULL,
      `address2` varchar(128) collate utf8_unicode_ci default NULL,
      `address3` varchar(128) collate utf8_unicode_ci default NULL,
      `city` varchar(64) collate utf8_unicode_ci default NULL,
      `county` varchar(64) collate utf8_unicode_ci default NULL,
      `state` varchar(3) collate utf8_unicode_ci default NULL,
      `country` varchar(2) collate utf8_unicode_ci default NULL,
      `zip` varchar(12) collate utf8_unicode_ci default NULL,
      `phone` varchar(32) collate utf8_unicode_ci default NULL,
      `fax` varchar(32) collate utf8_unicode_ci default NULL,
      `url` varchar(255) collate utf8_unicode_ci default NULL,
      `email` varchar(64) collate utf8_unicode_ci default NULL,
      `flags` int(11) NOT NULL default '0',
      `latitude` double default NULL,
      `longitude` double default NULL,
      `accuracy` double default NULL,
      `time_created` int(11) NOT NULL default '0',
      `score` double default NULL,
      `rating` double default NULL,
      `review_count` int(11) NOT NULL default '0',
      `photo_id` int(11) default NULL,
      `alias` varchar(96) collate utf8_unicode_ci default NULL,
      `geoquad` int(10) unsigned default NULL,
      `data_source_type` tinyint(3) unsigned default NULL,
      PRIMARY KEY  (`id`),
      KEY `zip` (`zip`,`phone`),
      KEY `longitude` (`longitude`,`latitude`),
      KEY `phone` (`phone`),
      KEY `review_count` (`review_count`),
      KEY `geoquad` (`geoquad`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci

