 MongoDB Exception hierarchy
=============================

MongoDB\Driver\Exception\Exception interface.

  MongoDB\Driver\Exception\LogicException (extends LogicException)
  - InvalidDataAccessApiUsageException:

  MongoDB\Driver\Exception\InvalidArgumentException (extends InvalidArgumentException)
  - InvalidDataAccessApiUsageException:

  MongoDB\Driver\Exception\UnexpectedValueException (extends UnexpectedValueException)
  - InvalidDataAccessApiUsageException:

  MongoDB\Driver\Exception\RuntimeException (extends RuntimeException)
  - UncategorizedDataAccessException
    MongoDB\Driver\Exception\ConnectionException
    - DataAccessResourceFailureException
      MongoDB\Driver\Exception\AuthenticationException
      - DataAccessResourceFailureException
      MongoDB\Driver\Exception\ConnectionTimeoutException
      - DataAccessResourceFailureException
      MongoDB\Driver\Exception\SSLConnectionException (deprecated)
      - DataAccessResourceFailureException
    MongoDB\Driver\Exception\ServerException
    - UncategorizedDataAccessException
      MongoDB\Driver\Exception\CommandException
      - InvalidDataAccessResourceUsageException
      MongoDB\Driver\Exception\ExecutionTimeoutException
      - QueryTimeoutException
      MongoDB\Driver\Exception\WriteException
      - DataIntegrityViolationException
      - DuplicateKeyException(E11000)
        MongoDB\Driver\Exception\BulkWriteException
        - DataIntegrityViolationException
        - DuplicateKeyException(E11000)

 Dao Exception hierarchy
===========================

DataAccessException:
    Root of the hierarchy of data access exceptions discussed in Expert One-On-One J2EE Design and Development.

- NonTransientDataAccessException:
    Root of the hierarchy of data access exceptions that are considered non-transient - where a retry of the same operation would fail unless the cause of the Exception is corrected.

  - CleanupFailureDataAccessException:
      Exception thrown when we couldn't cleanup after a data access operation, but the actual operation went OK.

  - DataIntegrityViolationException:
      Exception thrown when an attempt to insert or update data results in violation of an integrity constraint.
      MongoDB\Driver\Exception\WriteException
        MongoDB\Driver\Exception\BulkWriteException

    - DuplicateKeyException:
        Exception thrown when an attempt to insert or update data results in violation of an primary key or unique constraint.
      MongoDB\Driver\Exception\WriteException
        MongoDB\Driver\Exception\BulkWriteException

  - DataRetrievalFailureException:
      Exception thrown if certain expected data could not be retrieved, e.g.

    - IncorrectResultSizeDataAccessException:
        Data access exception thrown when a result was not of the expected size, for example when expecting a single row but getting 0 or more than 1 rows.

      - EmptyResultDataAccessException:
          Data access exception thrown when a result was expected to have at least one row (or element) but zero rows (or elements) were actually returned.

  - InvalidDataAccessApiUsageException:
      Exception thrown on incorrect usage of the API, such as failing to "compile" a query object that needed compilation before execution.
  MongoDB\Driver\Exception\LogicException
  MongoDB\Driver\Exception\InvalidArgumentException (extends InvalidArgumentException)

  - InvalidDataAccessResourceUsageException:
      Root for exceptions thrown when we use a data access resource incorrectly.
      MongoDB\Driver\Exception\CommandException

    - IncorrectUpdateSemanticsDataAccessException:
        Data access exception thrown when something unintended appears to have happened with an update, but the transaction hasn't already been rolled back.

    - TypeMismatchDataAccessException:
        Exception thrown on mismatch between Java type and database type: for example on an attempt to set an object of the wrong type in an RDBMS column.
  MongoDB\Driver\Exception\UnexpectedValueException (extends UnexpectedValueException)

  - NonTransientDataAccessResourceException:
      Data access exception thrown when a resource fails completely and the failure is permanent.

    - DataAccessResourceFailureException:
        Data access exception thrown when a resource fails completely: for example, if we can't connect to a database using JDBC.
    MongoDB\Driver\Exception\ConnectionException
      MongoDB\Driver\Exception\AuthenticationException
      MongoDB\Driver\Exception\ConnectionTimeoutException
      MongoDB\Driver\Exception\SSLConnectionException (deprecated)

  - PermissionDeniedDataAccessException:
      Exception thrown when the underlying resource denied a permission to access a specific element, such as a specific database table.

  - UncategorizedDataAccessException:
      Normal superclass when we can't distinguish anything more specific than "something went wrong with the underlying resource": for example, a SQLException from JDBC we can't pinpoint more precisely.
  MongoDB\Driver\Exception\RuntimeException (extends RuntimeException)

- RecoverableDataAccessException:
    Data access exception thrown when a previously failed operation might be able to succeed if the application performs some recovery steps and retries the entire transaction or in the case of a distributed transaction, the transaction branch.

- TransientDataAccessException:
    Root of the hierarchy of data access exceptions that are considered transient - where a previously failed operation might be able to succeed when the operation is retried without any intervention by application-level functionality.

  - ConcurrencyFailureException:
      Exception thrown on concurrency failure.

    - OptimisticLockingFailureException:
        Exception thrown on an optimistic locking violation.

    - PessimisticLockingFailureException:
        Exception thrown on a pessimistic locking violation.

      - CannotAcquireLockException:
          Exception thrown on failure to aquire a lock during an update, for example during a "select for update" statement.

      - CannotSerializeTransactionException:
          Exception thrown on failure to complete a transaction in serialized mode due to update conflicts.

      - DeadlockLoserDataAccessException:
          Generic exception thrown when the current process was a deadlock loser, and its transaction rolled back.

  - QueryTimeoutException:
      Exception to be thrown on a query timeout.
      MongoDB\Driver\Exception\ExecutionTimeoutException

  - TransientDataAccessResourceException:
      Data access exception thrown when a resource fails temporarily and the operation can be retried.
