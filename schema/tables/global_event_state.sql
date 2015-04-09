CREATE TABLE `global_event_state` (
  `gtid` varchar(255) NOT NULL,
  `is_clean_shutdown` tinyint(1) DEFAULT 0 NOT NULL,
  `event_type` varchar(20) NOT NULL,
  `time_updated` int(11) NOT NULL,
  PRIMARY KEY (`gtid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
