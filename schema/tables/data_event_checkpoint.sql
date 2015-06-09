CREATE TABLE `data_event_checkpoint` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `kafka_topic` varchar(255) NOT NULL,
  `kafka_offset` int(11) NOT NULL,
  `cluster_name` varchar(255) NOT NULL,
  `time_created` int(11) NOT NULL,
  `time_updated` int(11) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
