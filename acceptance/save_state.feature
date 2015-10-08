Feature: Save States

  Scenario: Execute create table query
    Given a create table statement
    When we execute the statement in rbr source database
    Then schema tracker should have correct information
    And rbr state should have correct information
