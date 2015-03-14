CREATE TABLE `event_state` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `gtid` text NOT NULL,
  `status` ENUM('Pending', 'Completed'),
  `query` text NOT NULL,
  `create_table_statement` text NOT NULL,
  `is_clean_shutdown` tinyint(1) NOT NULL,
  `time_created` int(11) NOT NULL,
  `time_updated` int(11) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
