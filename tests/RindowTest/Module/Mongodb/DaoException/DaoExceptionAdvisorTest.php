<?php
namespace RindowTest\Module\Mongodb\Dao\DaoExceptionAdvisorTest;

use PHPUnit\Framework\TestCase;
use Rindow\Module\Mongodb\Support\DaoExceptionAdvisor;
use Rindow\Container\ModuleManager;

class Test extends TestCase
{
	const MONGODB_DRIVER_EXCEPTION = 'MongoDB\\Driver\\Exception';
	const DAO_EXCEPTION = 'Rindow\\Database\\Dao\\Exception';

    public function dropCollection($collection)
    {
        $client = new \MongoDB\Driver\Manager();
        $cmd = new \MongoDB\Driver\Command(array('drop'=>$collection));
        try {
            $client->executeCommand('test',$cmd);
        } catch(\Exception $e) {
            ;
        }
    }

    public function createCollection($collection)
    {
        $client = new \MongoDB\Driver\Manager();
        $cmd = new \MongoDB\Driver\Command(array(
            'createIndexes'=> $collection,
            'indexes' => array(
                array('name'=>'ser_idx','key'=>array('ser'=>1),'unique'=>true),
            ),
        ));
        $client->executeCommand('test',$cmd);
    }

    public function getConfig()
    {
    	$config = array(
            'module_manager' => array(
                'modules' => array(
                    'Rindow\\Aop\\Module'=>true,
                    'Rindow\\Transaction\\Local\\Module'=>true,
                    'Rindow\\Module\\Mongodb\\LocalTxModule'=>true,
                ),
                'enableCache'=>false,
            ),
            'database' => array(
                'connections'=>array(
                    'mongodb' => array(
                        'database'=>'test',
                    ),
                ),
            ),
            'container' => array(
                'components' => array(
                    __NAMESPACE__.'\TestRepositoryWithArray'=>array(
                        'parent'=>'Rindow\\Module\\Mongodb\\Repository\\AbstractRepository',
                        'properties' => array(
                            'collection' => array('value'=>'test'),
                        ),
                    ),
                ),
            ),
        );
		return $config;
    }

	public function testTranslateMongodbException()
	{
		if(version_compare(MONGODB_VERSION, '1.5.0')<0) {
			$this->markTestSkipped('mongodb driver version < 1.5.0');
			return;
		}
		$exceptions = array(
			array('LogicException',             'E00000 test','InvalidDataAccessApiUsageException'),
			array('InvalidArgumentException',   'E00000 test','InvalidDataAccessApiUsageException'),
			array('UnexpectedValueException',   'E00000 test','InvalidDataAccessApiUsageException'),
			array('RuntimeException',           'E00000 test','UncategorizedDataAccessException'),
			array('ConnectionException',        'E00000 test','DataAccessResourceFailureException'),
			array('AuthenticationException',    'E00000 test','DataAccessResourceFailureException'),
			array('ConnectionTimeoutException', 'E00000 test','DataAccessResourceFailureException'),
			array('SSLConnectionException',     'E00000 test','DataAccessResourceFailureException'),
			array('ServerException',            'E00000 test','UncategorizedDataAccessException'),
			array('CommandException',           'E00000 test','InvalidDataAccessResourceUsageException'),
			array('ExecutionTimeoutException',  'E00000 test','QueryTimeoutException'),
			// Error: Cannot instantiate abstract class MongoDB\Driver\Exception\WriteException
			//array('WriteException',             'E00000 test','DataIntegrityViolationException'),
			//array('WriteException',             'E11000 test','DuplicateKeyException'),
			array('BulkWriteException',         'E00000 test','DataIntegrityViolationException'),
			array('BulkWriteException',         'E11000 test','DuplicateKeyException'),
		);

		$advisor = new DaoExceptionAdvisor();
		foreach ($exceptions as $test) {
			list($mongoExceptionName,$msg,$daoExceptionName) = $test;
			$className = self::MONGODB_DRIVER_EXCEPTION.'\\'.$mongoExceptionName;
			$mongoException = new $className($msg);
			$daoException = $advisor->translateMongodbException($mongoException);
			$this->assertInstanceof(
				self::DAO_EXCEPTION.'\\'.$daoExceptionName,$daoException);
			$this->assertEquals($mongoException,$daoException->getPrevious());
		}
	}

    /**
     * @expectedException        Rindow\Database\Dao\Exception\DuplicateKeyException
     * @expectedExceptionMessage E11000 duplicate key error
     */
	public function testTranslateDuplicateException()
	{
		$collection = 'test';
		$this->dropCollection($collection);
		$this->createCollection($collection);
		$mm = new ModuleManager($this->getConfig());
		$repository = $mm->getServiceLocator()->get(__NAMESPACE__.'\TestRepositoryWithArray');
		$entity = array('name'=>'foo','ser'=>1);
		$repository->save($entity);
		$entity = array('name'=>'foo','ser'=>1);
		$repository->save($entity);
	}
}
