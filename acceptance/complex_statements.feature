Feature: Complext Statements

  Scenario: Apply a series of schema changing queries
    Given a query to execute for table employee
      """
      CREATE TABLE `employee` (
        `id` int(11) DEFAULT NULL,
        `name` varchar(64) DEFAULT NULL,
        `is_active` tinyint(1) NOT NULL DEFAULT 0,
        `salary` float(10, 2) NOT NULL DEFAULT 0.00,
        `job_description` text DEFAULT NULL,
        `created_at` int(11) NOT NULL,
        `update_at` int(11) NOT NULL
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8
      """
    When we execute the statement in rbrsource database
    Given a query to execute for table employee
      """
      ALTER TABLE `employee` ADD `office` varchar(64) DEFAULT NULL AFTER `job_description`
      """
    When we execute the statement in rbrsource database
    Given a query to execute for table employee_backup
      """
      RENAME TABLE `employee` TO `employee_backup`
      """
    When we execute the statement in rbrsource database
    Given a query to execute for table employee_new
      """
      ALTER TABLE `employee_backup` RENAME TO `employee_new`
      """
    When we execute the statement in rbrsource database
    Given a query to execute for table employee_new
      """
      ALTER TABLE `employee_new` MODIFY office varchar(128)
      """
    When we execute the statement in rbrsource database
    Given an expected create table statement for table employee_new
      """
      CREATE TABLE `employee_new` (
        `id` int(11) DEFAULT NULL,
        `name` varchar(64) DEFAULT NULL,
        `is_active` tinyint(1) NOT NULL DEFAULT '0',
        `salary` float(10,2) NOT NULL DEFAULT '0.00',
        `job_description` text,
        `office` varchar(128) DEFAULT NULL,
        `created_at` int(11) NOT NULL,
        `update_at` int(11) NOT NULL
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8
      """
    Given an expected avro schema for table employee_new
      """
      {"fields": [{"default": null, "type": ["null", "int"], "name": "id"}, {"default": null, "maxlen": "64", "type": ["null", "string"], "name": "name"}, {"default": 0, "type": "int", "name": "is_active"}, {"default": 0.0, "scale": "2", "type": "float", "name": "salary", "precision": "10"}, {"default": null, "type": ["null", "string"], "name": "job_description"}, {"default": null, "maxlen": "128", "type": ["null", "string"], "name": "office"}, {"type": "int", "name": "created_at"}, {"type": "int", "name": "update_at"}], "namespace": "", "name": "employee_new", "type": "record"}
    """
    Then schematracker should have correct schema information
    And schematizer should have correct info
