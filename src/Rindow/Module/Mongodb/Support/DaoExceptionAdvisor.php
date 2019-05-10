<?php
namespace Rindow\Module\Mongodb\Support;

use MongoDB\Driver\Exception\Exception as MongoDBException;
use Interop\Lenient\Dao\Exception\DataAccessException as DaoException;

class DaoExceptionAdvisor
{
    static protected $errorCodes = array(
        11000 => 'Rindow\\Database\\Dao\\Exception\\DuplicateKeyException',
    );

    static protected $mongodbExceptions = array(
        'MongoDB\\Driver\\Exception\\LogicException' =>
            'Rindow\\Database\\Dao\\Exception\\InvalidDataAccessApiUsageException',
        'MongoDB\\Driver\\Exception\\InvalidArgumentException' =>
            'Rindow\\Database\\Dao\\Exception\\InvalidDataAccessApiUsageException',
        'MongoDB\\Driver\\Exception\\UnexpectedValueException' =>
            'Rindow\\Database\\Dao\\Exception\\InvalidDataAccessApiUsageException',
        'MongoDB\\Driver\\Exception\\RuntimeException' =>
            'Rindow\\Database\\Dao\\Exception\\UncategorizedDataAccessException',
        'MongoDB\\Driver\\Exception\\ConnectionException' =>
            'Rindow\\Database\\Dao\\Exception\\DataAccessResourceFailureException',
        'MongoDB\\Driver\\Exception\\AuthenticationException' =>
            'Rindow\\Database\\Dao\\Exception\\DataAccessResourceFailureException',
        'MongoDB\\Driver\\Exception\\ConnectionTimeoutException' =>
            'Rindow\\Database\\Dao\\Exception\\DataAccessResourceFailureException',
        'MongoDB\\Driver\\Exception\\SSLConnectionException' =>
            'Rindow\\Database\\Dao\\Exception\\DataAccessResourceFailureException',
        'MongoDB\\Driver\\Exception\\ServerException' =>
            'Rindow\\Database\\Dao\\Exception\\UncategorizedDataAccessException',
        'MongoDB\\Driver\\Exception\\CommandException' =>
            'Rindow\\Database\\Dao\\Exception\\InvalidDataAccessResourceUsageException',
        'MongoDB\\Driver\\Exception\\ExecutionTimeoutException' =>
            'Rindow\\Database\\Dao\\Exception\\QueryTimeoutException',
        'MongoDB\\Driver\\Exception\\WriteException' =>
            'Rindow\\Database\\Dao\\Exception\\DataIntegrityViolationException',
        'MongoDB\\Driver\\Exception\\BulkWriteException' =>
            'Rindow\\Database\\Dao\\Exception\\DataIntegrityViolationException',
    );

    public function translateMongodbException($mongodbException)
    {
        if(!($mongodbException instanceof MongoDBException))
            return $mongodbException;
        if($mongodbException instanceof DaoException)
            return $mongodbException;
        $originalClassName = get_class($mongodbException);
        if(!isset(self::$mongodbExceptions[$originalClassName]))
            return $mongodbException;
        $className = self::$mongodbExceptions[$originalClassName];
        if($mongodbException instanceof \MongoDB\Driver\Exception\WriteException) {
            $msg = $mongodbException->getMessage();
            if(preg_match('/^E([0-9]+) /', $msg, $matches)) {
                $code = intval($matches[1]);
                if(isset(self::$errorCodes[$code])) {
                    $className = self::$errorCodes[$code];
                }
            }
        }
        $daoException = new $className($mongodbException->getMessage(),$mongodbException->getCode(),$mongodbException);
        return $daoException;
    }

    public function afterThrowingAdvice(/*JoinPointInterface*/ $joinPoint)
    {
        $e = $joinPoint->getThrowing();
        if($e) {
            $e = $this->translateMongodbException($e);
            $joinPoint->setThrowing($e);
        }
    }
}

