CREATE TABLE `mysql_dumps` (
  `id` int(11) NOT NULL,
  `database_dump` longtext NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;