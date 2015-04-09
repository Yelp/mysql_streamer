CREATE TABLE `data_event_checkpoint` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `gtid` varchar(255) NOT NULL,
  `offset` int(11) NOT NULL,
  `table_name` varchar(255) NOT NULL,
  `time_created` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `gtid` (`gtid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
