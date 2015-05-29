CREATE TABLE `global_event_state` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `position` text NOT NULL,
  `is_clean_shutdown` tinyint(1) DEFAULT 0 NOT NULL,
  `event_type` varchar(20) NOT NULL,
  `cluster_name` varchar(255) NOT NULL,
  `database_name` varchar(255) NOT NULL,
  `time_updated` int(11) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
