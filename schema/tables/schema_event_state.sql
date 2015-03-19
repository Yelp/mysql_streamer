CREATE TABLE `schema_event_state` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `gtid` varchar(255) NOT NULL,
  `status` varchar(20) NOT NULL DEFAULT 'Pending',
  `query` text NOT NULL,
  `table_name` varchar(255) NOT NULL,
  `create_table_statement` text NOT NULL,
  `time_created` int(11) NOT NULL,
  `time_updated` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `gtid` (`gtid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
