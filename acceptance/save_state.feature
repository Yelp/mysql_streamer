Feature: Save States

  Scenario: Execute create table query
    Given a create table statement
    When we execute the statement in rbrsource database
    Then schematracker should have correct schema information
    And rbrstate should have correct state information
