CREATE TABLE `replication_heartbeat` (
    `serial` bigint(20) NOT NULL,
    `timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

INSERT INTO `replication_heartbeat` (`serial`) VALUES (0);
