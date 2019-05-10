<?php
namespace RindowTest\Module\Mongodb\Repository\RepositoryOnModuleTest;

use PHPUnit\Framework\TestCase;
use Rindow\Container\ModuleManager;
use Interop\Lenient\Dao\Repository\DataMapper;
use Rindow\Stdlib\Entity\AbstractEntity;
use MongoDB\BSON\Persistable;
use Rindow\Module\Mongodb\Repository\MongodbRepository;

class TestEntity extends AbstractEntity
{
    protected $id;
    protected $test;
    protected $group;
    protected $ser;

    public function __construct(array $data = null)
    {
        if($data==null)
            return;
        foreach ($data as $key => $value) {
            $this->$key = $value;
        }
    }
}

class TestDataMapper implements DataMapper
{
    public function map($data)
    {
        $entity = new TestEntity();
        $entity->setId($data['_id']);
        if(isset($data['test']))
            $entity->setTest($data['test']);
        if(isset($data['group']))
            $entity->setGroup($data['group']);
        if(isset($data['ser']))
            $entity->setSer($data['ser']);
        return $entity;
    }

    public function demap($entity)
    {
        //var_dump($entity);
        $data = array();
        if($entity->getId()!==null)
            $data['_id'] = $entity->getId();
        if($entity->getTest()!==null)
            $data['test'] = $entity->getTest();
        if($entity->getGroup()!==null)
            $data['group'] = $entity->getGroup();
        if($entity->getSer()!==null)
            $data['ser'] = $entity->getSer();
        return $data;
    }

    public function fillId($entity,$id)
    {
        $entity->setId($id);
        return $entity;
    }
    public function getFetchClass()
    {
        return null;
    }
}

class TestEntityForBSON extends AbstractEntity implements Persistable
{
    protected $id;
    protected $test;
    protected $group;
    protected $ser;

    public function __construct(array $data = null)
    {
        if($data==null)
            return;
        foreach ($data as $key => $value) {
            $this->$key = $value;
        }
    }

    public function bsonUnserialize(array $data)
    {
        foreach ($data as $key => $value) {
            if($key=='_id')
                $key = 'id';
            if(property_exists($this, $key))
                $this->$key = $value;
        }
    }

    public function bsonSerialize()
    {
        $data = array();
        if($this->id!==null)
            $data['_id'] = $this->id;
        $data['test'] = $this->test;
        $data['group'] = $this->group;
        $data['ser'] = $this->ser;
        return $data;
    }
}

class TestDataMapperForBSON implements DataMapper
{
    public function map($entity)
    {
        return $entity;
    }

    public function demap($entity)
    {
        return $entity;
    }

    public function fillId($entity,$id)
    {
        $entity->setId($id);
        return $entity;
    }
    public function getId($entity)
    {
        return $entity->getId();
    }

    public function getFetchClass()
    {
        return __NAMESPACE__.'\TestEntityForBSON';
    }
}

class TestExtendedRepository extends MongodbRepository
{
    public function map($data)
    {
        $entity = new TestEntity();
        $entity->setId($data['_id']);
        if(isset($data['test']))
            $entity->setTest($data['test']);
        if(isset($data['group']))
            $entity->setGroup($data['group']);
        if(isset($data['ser']))
            $entity->setSer($data['ser']);
        return $entity;
    }

    public function demap($entity)
    {
        //var_dump($entity);
        $data = array();
        if($entity->getId()!==null)
            $data['_id'] = $entity->getId();
        if($entity->getTest()!==null)
            $data['test'] = $entity->getTest();
        if($entity->getGroup()!==null)
            $data['group'] = $entity->getGroup();
        if($entity->getSer()!==null)
            $data['ser'] = $entity->getSer();
        return $data;
    }

    public function fillId($entity,$id)
    {
        $entity->setId($id);
        return $entity;
    }
    public function getFetchClass()
    {
        return null;
    }
}

class Test extends TestCase
{
    public static $skip = false;
    public static function setUpBeforeClass()
    {
        if (!extension_loaded('mongodb')) {
            self::$skip = true;
            return;
        }
        try {
            $client = new \MongoDB\Driver\Manager();
            $query = new \MongoDB\Driver\Query(array());
            $cursor = $client->executeQuery('test.testdb',$query);
        } catch(\Exception $e) {
            self::$skip = true;
            return;
        }
    }

    public static function tearDownAfterClass()
    {
        if(self::$skip)
            return;
    }

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

    public function deleteFromCollection($collection,$id)
    {
        $client = new \MongoDB\Driver\Manager();
        $bulkWrite = new \MongoDB\Driver\BulkWrite();
        $bulkWrite->delete(array('_id'=>$id));
        return $client->executeBulkWrite('test.'.$collection,$bulkWrite);
    }

    public function insertToCollection($collection,$data)
    {
        $client = new \MongoDB\Driver\Manager();
        $bulkWrite = new \MongoDB\Driver\BulkWrite();
        $bulkWrite->insert($data);
        return $client->executeBulkWrite('test.'.$collection,$bulkWrite);
    }

    public function getData($collection,$filter=array())
    {
        $client = new \MongoDB\Driver\Manager();
        $query = new \MongoDB\Driver\Query($filter);
        $cursor = $client->executeQuery('test.'.$collection,$query);
        $cursor->setTypeMap(array('root'=>'array','document'=>'array','array'=>'array'));
        return $cursor;
    }

    public function setUp()
    {
        if(self::$skip) {
            $this->markTestSkipped();
            return;
        }
        $this->dropCollection('testdb');
        $this->createCollection('testdb');
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
            /*
            'testrepositories' => array(
            	'test' => array(
            		'factory' => 'Rindow\\Module\\Mongodb\\Repository\\DefaultRepositoryFactory',
            		'reference' => 'testdb',
            	),
            ),
            */
            'container' => array(
                'components' => array(
                    __NAMESPACE__.'\TestRepositoryWithArray'=>array(
                        'parent'=>'Rindow\\Module\\Mongodb\\Repository\\AbstractRepository',
                        'properties' => array(
                            'collection' => array('value'=>'testdb'),
                            //'dataMapper' => array('ref' =>__NAMESPACE__.'\TestMapper'),
                        ),
                    ),
                    // ************** RepositoryWithDataMapper *******
                    __NAMESPACE__.'\TestRepositoryWithDataMapper'=>array(
                        'parent'=>'Rindow\\Module\\Mongodb\\Repository\\AbstractRepository',
                        'properties' => array(
                            'collection' => array('value'=>'testdb'),
                            'dataMapper' => array('ref' =>__NAMESPACE__.'\TestDataMapper'),
                        ),
                    ),
                    __NAMESPACE__.'\TestDataMapper'=>array(
                        'class' => __NAMESPACE__.'\TestDataMapper',
                    ),

                    // ************** RepositoryWithDataMapperForBSON *******
                    __NAMESPACE__.'\TestRepositoryWithDataMapperForBSON'=>array(
                        'parent'=>'Rindow\\Module\\Mongodb\\Repository\\AbstractRepository',
                        'properties' => array(
                            'collection' => array('value'=>'testdb'),
                            'dataMapper' => array('ref' =>__NAMESPACE__.'\TestDataMapperForBSON'),
                        ),
                    ),
                    __NAMESPACE__.'\TestDataMapperForBSON'=>array(
                        'class' => __NAMESPACE__.'\TestDataMapperForBSON',
                    ),

                    // ************** ExtendedRepository *******
                    __NAMESPACE__.'\TestExtendedRepository'=>array(
                        'parent'=>'Rindow\\Module\\Mongodb\\Repository\\AbstractRepository',
                        'class' => __NAMESPACE__.'\TestExtendedRepository',
                        'properties' => array(
                            'collection' => array('value'=>'testdb'),
                        ),
                    ),
                ),
            ),
            'aop' => array(
                'intercept_to' => array(
                    __NAMESPACE__.'\TestExtendedRepository' => true,
                ),
                'pointcuts' => array(
                    __NAMESPACE__.'\TestExtendedRepository'=> 
                        'execution('.__NAMESPACE__.'\TestExtendedRepository::save()) ||'.
                        'execution('.__NAMESPACE__.'\TestExtendedRepository::findById()) ||'.
                        'execution('.__NAMESPACE__.'\TestExtendedRepository::findAll()) ||'.
                        'execution('.__NAMESPACE__.'\TestExtendedRepository::findOne()) ||'.
                        'execution('.__NAMESPACE__.'\TestExtendedRepository::delete()) ||'.
                        'execution('.__NAMESPACE__.'\TestExtendedRepository::deleteById()) ||'.
                        'execution('.__NAMESPACE__.'\TestExtendedRepository::existsById()) ||'.
                        'execution('.__NAMESPACE__.'\TestExtendedRepository::count())',
                ),
                'aspectOptions' => array(
                    'Rindow\\Module\\Mongodb\\DefaultDaoExceptionAdvisor' => array(
                        'advices' => array(
                            'afterThrowingAdvice' => array(
                                'type' => 'after-throwing',
                                'pointcut_ref' => array(
                                    __NAMESPACE__.'\TestExtendedRepository'=>true,
                                ),
                            ),
                        ),
                    ),
                    'Rindow\\Transaction\\DefaultTransactionAdvisor' => array(
                        'advices' => array(
                            'required' => array(
                                'pointcut_ref' => array(
                                    __NAMESPACE__.'\TestExtendedRepository'=>true,
                                ),
                            ),
                        ),
                    ),
                ),
            ),
    	);
		return $config;
    }
/*
    public function testRepositoryFromFactory()
    {
    	$mm = new ModuleManager($this->getConfig());
    	$sl = $mm->getServiceLocator();
    	$factory = $sl->get('Rindow\\Module\\Mongodb\\Repository\\DefaultRepositoryFactory');

    	$repository = $factory->getRepository('testdb');
        // save
	    $doc = $repository->save(array('test'=>'foo','group'=>'red','ser'=>1));
        $doc2 = $repository->save(array('test'=>'foo','group'=>'blue','ser'=>2));
        $doc3 = $repository->save(array('test'=>'boo','group'=>'red','ser'=>3));
    	$data = $this->getData('testdb')->toArray();
    	$this->assertCount(3,$data);

        // find
        $b = $repository->getQueryBuilder();
        $filter = array();
        $filter[] = $b->createExpression('test',$b->eq(),'foo');
        $filter[] = $b->createExpression('group',$b->eq(),'red');
        $results = array();
        foreach ($repository->findAll($filter) as $entity) {
            $results[] = $entity;
        }
        $this->assertCount(1,$results);

        // delete
        $repository->delete($doc);
        $this->deleteFromCollection('testdb',$doc['id']);
        $data = $this->getData('testdb')->toArray();
        $this->assertCount(2,$data);
    }
*/
    public function testRepositoryWithArray()
    {
        $mm = new ModuleManager($this->getConfig());
        $sl = $mm->getServiceLocator();
        $repository = $sl->get(__NAMESPACE__.'\TestRepositoryWithArray');

        // save
        $doc = $repository->save(array('test'=>'foo','group'=>'red','ser'=>1));
        $doc2 = $repository->save(array('test'=>'foo','group'=>'blue','ser'=>2));
        $doc3 = $repository->save(array('test'=>'boo','group'=>'red','ser'=>3));
        $data = $this->getData('testdb')->toArray();
        $this->assertCount(3,$data);

        // find
        $b = $repository->getQueryBuilder();
        $filter = array();
        $filter[] = $b->createExpression('test',$b->eq(),'foo');
        $filter[] = $b->createExpression('group',$b->eq(),'red');
        $results = array();
        foreach ($repository->findAll($filter) as $entity) {
            $results[] = $entity;
        }
        $this->assertCount(1,$results);

        // delete
        $repository->delete($doc);
        $this->deleteFromCollection('testdb',$doc['id']);
        $data = $this->getData('testdb')->toArray();
        $this->assertCount(2,$data);
    }

    public function testRepositoryWithDataMapperNormal()
    {
        $mm = new ModuleManager($this->getConfig());
        $sl = $mm->getServiceLocator();
        $repository = $sl->get(__NAMESPACE__.'\TestRepositoryWithDataMapper');

        // save
        $doc = $repository->save(new TestEntity(array('test'=>'foo','group'=>'red','ser'=>1)));
        $doc2 = $repository->save(new TestEntity(array('test'=>'foo','group'=>'blue','ser'=>2)));
        $doc3 = $repository->save(new TestEntity(array('test'=>'boo','group'=>'red','ser'=>3)));
        $data = $this->getData('testdb')->toArray();
        $this->assertCount(3,$data);

        // find
        $b = $repository->getQueryBuilder();
        $filter = array();
        $filter[] = $b->createExpression('test',$b->eq(),'foo');
        $filter[] = $b->createExpression('group',$b->eq(),'red');
        $results = array();
        foreach ($repository->findAll($filter) as $entity) {
            $results[] = $entity;
            $this->assertInstanceof(__NAMESPACE__.'\TestEntity',$entity);
        }
        $this->assertCount(1,$results);

        // delete
        $repository->delete($doc);
        $this->deleteFromCollection('testdb',$doc->getId());
        $data = $this->getData('testdb')->toArray();
        $this->assertCount(2,$data);
    }

    public function testRepositoryWithDataMapperForBSON()
    {
        $mm = new ModuleManager($this->getConfig());
        $sl = $mm->getServiceLocator();
        $repository = $sl->get(__NAMESPACE__.'\TestRepositoryWithDataMapperForBSON');

        // save
        $doc = $repository->save(new TestEntityForBSON(array('test'=>'foo','group'=>'red','ser'=>1)));
        $doc2 = $repository->save(new TestEntityForBSON(array('test'=>'foo','group'=>'blue','ser'=>2)));
        $doc3 = $repository->save(new TestEntityForBSON(array('test'=>'boo','group'=>'red','ser'=>3)));
        $data = $this->getData('testdb')->toArray();
        $this->assertCount(3,$data);

        // find
        $b = $repository->getQueryBuilder();
        $filter = array();
        $filter[] = $b->createExpression('test',$b->eq(),'foo');
        $filter[] = $b->createExpression('group',$b->eq(),'red');
        $results = array();
        foreach ($repository->findAll($filter) as $entity) {
            $results[] = $entity;
            $this->assertInstanceof(__NAMESPACE__.'\TestEntityForBSON',$entity);
        }
        $this->assertCount(1,$results);

        // delete
        $repository->delete($doc);
        $this->deleteFromCollection('testdb',$doc->getId());
        $data = $this->getData('testdb')->toArray();
        $this->assertCount(2,$data);
    }

    public function testExtendedRepository()
    {
        $mm = new ModuleManager($this->getConfig());
        $sl = $mm->getServiceLocator();
        $repository = $sl->get(__NAMESPACE__.'\TestExtendedRepository');

        // save
        $doc = $repository->save(new TestEntity(array('test'=>'foo','group'=>'red','ser'=>1)));
        $doc2 = $repository->save(new TestEntity(array('test'=>'foo','group'=>'blue','ser'=>2)));
        $doc3 = $repository->save(new TestEntity(array('test'=>'boo','group'=>'red','ser'=>3)));
        $data = $this->getData('testdb')->toArray();
        $this->assertCount(3,$data);

        // find
        $b = $repository->getQueryBuilder();
        $filter = array();
        $filter[] = $b->createExpression('test',$b->eq(),'foo');
        $filter[] = $b->createExpression('group',$b->eq(),'red');
        $results = array();
        foreach ($repository->findAll($filter) as $entity) {
            $results[] = $entity;
        }
        $this->assertCount(1,$results);

        // delete
        $repository->delete($doc);
        $this->deleteFromCollection('testdb',$doc->getId());
        $data = $this->getData('testdb')->toArray();
        $this->assertCount(2,$data);
    }

    /**
     * @expectedException        Interop\Lenient\Dao\Exception\DuplicateKeyException
     *
     * Code changes depending on the version of extension
     * expectedExceptionCode    11000
     */
    public function testDuplicateWithArray()
    {
        $mm = new ModuleManager($this->getConfig());
        $sl = $mm->getServiceLocator();
        $repository = $sl->get(__NAMESPACE__.'\TestRepositoryWithArray');

        // save
        $doc = $repository->save(array('test'=>'foo','group'=>'red','ser'=>1));
        $doc2 = $repository->save(array('test'=>'foo','group'=>'blue','ser'=>1));
    }
}
