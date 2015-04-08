CREATE TABLE `global_state` (
  `gtid` varchar(255) NOT NULL,
  `is_clean_shutdown` tinyint(1) NOT NULL,
  PRIMARY KEY (`gtid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
