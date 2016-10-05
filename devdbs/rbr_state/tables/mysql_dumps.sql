CREATE TABLE `mysql_dumps` (
  `cluster_name` varchar(255) NOT NULL,
  `database_dump` longtext NOT NULL,
  PRIMARY KEY (`cluster_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;