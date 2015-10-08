Feature: Save States

  Scenario: Execute create table query
    Given a create table statement for table biz
        """
        CREATE TABLE `biz` (
          `id` int(11) DEFAULT NULL,
          `name` varchar(64) DEFAULT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8
        """
    When we execute the statement in rbrsource database
    Then schematracker should have correct schema information
    And rbrstate should have correct state information
