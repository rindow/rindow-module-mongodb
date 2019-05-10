<?php
namespace RindowTest\Module\Mongodb\Orm\CriteriaMapperTest;

use PHPUnit\Framework\TestCase;
use Rindow\Stdlib\Entity\AbstractEntity;
use Rindow\Container\ModuleManager;
use Rindow\Persistence\Orm\Criteria\CriteriaBuilder;
use Rindow\Persistence\OrmShell\DataMapper;
use Rindow\Module\Mongodb\Orm\CriteriaMapper;
use Rindow\Module\Mongodb\Orm\AbstractMapper;

class TestEntity extends AbstractEntity
{
    public $id;
    public $name;
    public $field1;
    public $field2;
}

class TestEntityMapper extends AbstractMapper implements DataMapper
{
    //const CLASS_NAME = 'RindowTest\Module\Mongodb\Orm\CriteriaMapperTest\TestEntityMapper';
    const TABLE_NAME = 'category';
    const PRIMARYKEY = 'id';

    protected function namedQueryFactories()
    {}

    public function className()
    {
        return __NAMESPACE__.'\TestEntity';
    }

    public function tableName()
    {
        return self::TABLE_NAME;
    }

    public function primaryKey()
    {
        return self::PRIMARYKEY;
    }

    public function fieldNames()
    {
        return array('id','name','field1','field2');
    }

    public function hash($entityManager,$entity)
    {
        $hash = md5(strval($this->getId($entity))) .
            md5(strval($this->getField($entity,'name'))).
            md5(strval($this->getField($entity,'field1'))).
            md5(strval($this->getField($entity,'field2')));
        return md5($hash);
    }

    public function supplementEntity($entityManager,$entity)
    {
        return $entity;
    }

    public function subsidiaryPersist($entityManager,$entity)
    {
    }

    public function subsidiaryRemove($entityManager,$entity)
    {
    }
    protected function bulidInsertDocument($entity)
    {
        return $this->extractEntity($entity);
    }
    protected function bulidUpdateDocument($entity)
    {
        return $this->extractEntity($entity);
    }
    protected function buildEntity($document,$entity=null)
    {
        return $this->hydrateEntity($document,$entity);
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
            $cursor = $client->executeQuery('test.color',$query);
        } catch(\Exception $e) {
            self::$skip = true;
            return;
        }
    }

    public static function tearDownAfterClass()
    {
        if(self::$skip)
            return;

        $client = new \MongoDB\Driver\Manager();
        $cmd = new \MongoDB\Driver\Command(array('drop'=>'testcol'));
        try {
            //$client->executeCommand('test',$cmd);
        } catch(\Exception $e) {
            ;
        }
    }

    public function setUp()
    {
        if(self::$skip) {
            $this->markTestSkipped();
            return;
        }
        $client = new \MongoDB\Driver\Manager();
        $cmd = new \MongoDB\Driver\Command(array('drop'=>'color'));
        try {
            $client->executeCommand('test',$cmd);
        } catch(\Exception $e) {
            ;
        }
        $cmd = new \MongoDB\Driver\Command(array('drop'=>'category'));
        try {
            $client->executeCommand('test',$cmd);
        } catch(\Exception $e) {
            ;
        }
        $cmd = new \MongoDB\Driver\Command(array('drop'=>'product'));
        try {
            $client->executeCommand('test',$cmd);
        } catch(\Exception $e) {
            ;
        }
    }

    public static function getMongoClientStatic()
    {
        $client = new \MongoDB\Driver\Manager();
        return $client;
    }
    public function getMongoClient()
    {
        return self::getMongoClientStatic();
    }
    public function getData($collection,$filter=array())
    {
        $mongo = $this->getMongoClient();
        $query = new \MongoDB\Driver\Query($filter);
        $cursor = $mongo->executeQuery('test.'.$collection,$query);
        $cursor->setTypeMap(array('root'=>'array','document'=>'array','array'=>'array'));
        return $cursor;
    }
    public function setData($collection,$data)
    {
        $manager = $this->getMongoClient();
        $bulkWrite = new \MongoDB\Driver\BulkWrite();
        foreach ($data as $doc) {
            $ids[] = $bulkWrite->insert($doc);
        }
        $manager->executeBulkWrite('test.'.$collection,$bulkWrite);
        return $ids;
    }

    public function getConfig()
    {
        $config = array(
            'module_manager' => array(
                'modules' => array(
                    'Rindow\Persistence\OrmShell\Module' => true,
                    'Rindow\Module\Mongodb\StandaloneModule' => true,
                ),
                'enableCache'=>false,
            ),
            'container' => array(
                'aliases' => array(
                    'Rindow\Persistence\OrmShell\Transaction\DefaultTransactionSynchronizationRegistry'=>'dummy',
                    'EntityManager' => 'Rindow\\Persistence\\OrmShell\\DefaultEntityManager',
                    'CriteriaBuilder' => 'Rindow\\Persistence\\OrmShell\\DefaultCriteriaBuilder',
                ),
                'components' => array(
                    __NAMESPACE__.'\TestEntityMapper'=>array(
                        'parent' => 'Rindow\\Module\\Mongodb\\Orm\\DefaultAbstractMapper',
                        'properties' => array(
                            'hydrator'=>array('ref'=>__NAMESPACE__.'\EntityHydrator'),
                        ),
                    ),
                    __NAMESPACE__.'\EntityHydrator'=>array(
                        'class' => 'Rindow\Stdlib\Entity\EntityHydrator',
                    ),
                ),
            ),
            'database' => array(
                'connections' => array(
                    'mongodb' => array(
                        'database' => 'test',
                    ),
                ),
            ),
            'persistence' => array(
                'mappers' => array(
                    // O/R Mapping for Mongodb
                    __NAMESPACE__.'\TestEntity' => __NAMESPACE__.'\TestEntityMapper',
                ),
            ),
        );
        return $config;
    }

	public function testPartOfBuilder1()
	{
		$cb = new CriteriaBuilder();
        $cm = new CriteriaMapper();

        // build QueryCriteria
        $q = $cb->createQuery('FooResult');
        $r = $q->from('FooEntity')->alias('p');
        $p1 = $cb->parameter('integer','p1');
        $q->select($r)
            //->distinct(true)
            ->where($cb->and_(
                $cb->gt($r->get('field1'),$p1),
                $cb->le($p1,$r->get('field1_2'))))
            ->orderBy(
                $cb->desc($r->get('field2')),
                $r->get('field2_2'));
            //->groupBy(
            //    $r->get('field3'),
            //    $r->get('field3_2'));
            //->having($cb->gt($r->get('field4'),100));

        $parameters = array(
        	'p1' => 1,
        );

        $pc = $cm->prepare($q);
        $this->assertInstanceOf('Rindow\Module\Mongodb\Orm\PreparedCriteria',$pc);
        $this->assertInstanceOf('Rindow\Persistence\Orm\Criteria\CriteriaQuery',$pc->getCriteria());
        $this->assertNull($pc->getResultClass());

        $options = array('limit'=>10);
        list($filter,$options,$command,$resultFormatter,$mapped) = $pc->buildQuery($parameters,$options,'collection');

        $this->assertEquals(
            array('$and'=>array(
                array('field1'=>array('$gt'=>1)),
                array('field1_2'=>array('$gte'=>1)),
            )),
            $filter
        );
        $this->assertEquals(
            array(
                'limit'=>10,
                'sort'=>array('field2'=>-1,'field2_2'=>1)
            ),
            $options
        );
        $this->assertNull($command);
        $this->assertNull($resultFormatter);
        $this->assertTrue($mapped);
	}

    public function testPartOfBuilder2()
    {
        $cb = new CriteriaBuilder();
        $cm = new CriteriaMapper();

        $q = $cb->createQuery('FooResult');

        $r = $q->from('FooEntity')->alias('p');
        $p1 = $cb->parameter('integer','p1');
        //$q->distinct(true)
        $q->multiselect(
                $r->get('field1'),
                $r->get('field2'),
                $cb->count($r->get('field1'))->alias('count'),
                $cb->sum($r->get('field3'))->alias('sum'),
                $cb->max($r->get('field4'))->alias('max'),
                $cb->min($r->get('field5'))->alias('min')
            )
            //->where($cb->and_(
            //    $cb->gt($r->get('field1'),$p1),
            //    $cb->le($p1,$r->get('field1_2'))))
            //->orderBy(
            //    $cb->desc($r->get('field2')),
            //    $r->get('field2_2'));
            ->groupBy(
                $r->get('field1')
            )
            ->having($cb->gt($r->get('field1'),$p1));

        $parameters = array(
            'p1' => 1,
        );

        $pc = $cm->prepare($q);
        $options = array('limit'=>10);
        list($filter,$options,$command,$resultFormatter,$mapped) = $pc->buildQuery($parameters,$options,'collection');
        // $filter ===> N/A
        // $options ===> N/A
        $this->assertEquals(
            array(
                'group'=>array(
                    'ns' => 'collection',
                    'key' => array('field1'=>1),
                    '$reduce' => 
                        'function (curr, result) {'.
                            'result.field2 = curr.field2;'.
                            'result.count++;'.
                            'result.sum += curr.field3;'.
                            'result.max = Math.max(result.max,curr.field4);'.
                            'result.min = Math.min(result.min,curr.field5);'.
                        '}',
                    'initial' => array(
                        'field2' => 0,
                        'count' => 0,
                        'sum' => 0,
                        'max' => ~PHP_INT_MAX,
                        'min' => PHP_INT_MAX,
                    ),
                    'cond' => array(
                        'field1' => array( '$gt' => 1),
                    ),
                ),
            ),
            $command
        );
        $this->assertEquals(array($pc,'_resultGroup'),$resultFormatter);
        $this->assertFalse($mapped);
    }
    public function testBuilder1OnEntityManager()
    {
        $collection = 'category';
        $data = array(
            array('name'=>'test1','field1'=>1, 'field2'=>20),
            array('name'=>'test2','field1'=>20,'field2'=>2),
            array('name'=>'test3','field1'=>3, 'field2'=>3),
            array('name'=>'test4','field1'=>11, 'field2'=>20),
            array('name'=>'test5','field1'=>11, 'field2'=>21),
            array('name'=>'test6','field1'=>10, 'field2'=>21),
        );
        $ids = $this->setData($collection,$data);
        $mm = new ModuleManager($this->getConfig());
        $em = $mm->getServiceLocator()->get('EntityManager');
        $cb = $mm->getServiceLocator()->get('CriteriaBuilder');

        $q = $cb->createQuery();
        $r = $q->from(__NAMESPACE__.'\TestEntity');
        $p1 = $cb->parameter('integer','p1');
        $q->select($r)
            ->where($cb->and_(
                $cb->gt($r->get('field1'),$p1),
                $cb->le($p1,$r->get('field2'))))
            ->orderBy(
                $r->get('field1'),
                $cb->desc($r->get('field2')));

        $query = $em->createQuery($q)->setParameter('p1', 7);
        $resultList = $query->getResultList();
        $count = 0;
        foreach ($resultList as $entity) {
            $this->assertInstanceOf(__NAMESPACE__.'\TestEntity',$entity);
            if($count==0)
                $this->assertEquals('test6',$entity->name);
            elseif($count==1)
                $this->assertEquals('test5',$entity->name);
            elseif($count==2)
                $this->assertEquals('test4',$entity->name);
            $count++;
        }
        $this->assertEquals(3,$count);
    }

    public function testPartOfQueryCount()
    {
		$cb = new CriteriaBuilder();
        $cm = new CriteriaMapper();

        $q = $cb->createQuery();
        $r = $q->from(__NAMESPACE__.'\Category');
        $p1 = $cb->parameter('integer','p1');
        $q->select($cb->count($r))
            ->where($cb->and_(
                $cb->gt($r->get('field1'),$p1),
                $cb->le($p1,$r->get('field1_2'))));

        $parameters = array(
        	'p1' => 1,
        );

        $pc = $cm->prepare($q);

        //$filter = $pc->buildFilter($parameters);
        $options = array('limit'=>10,'skip'=>20,'hint'=>'fooHint','readConcern'=>'boo');
        $tableName = 'foo';
        list($filter,$options,$command,$resultFormatter,$mapped) = $pc->buildQuery($parameters,$options,$tableName);
        //$this->assertEquals(
        //	array('$and'=>array(
        //		array('field1'=>array('$gt'=>1)),
        //		array('field1_2'=>array('$gte'=>1)),
        //	)),
        //	$filter);

		$this->assertEquals('FUNCTION',$pc->getSelectionType());

		//list($command,$resultFormatter) = $pc->buildCommand($tableName,$parameters,$filter,$options);
        $this->assertEquals(
        	array(
        		'count'=>'foo',
	        	'query'=>array('$and'=>array(
	        		array('field1'=>array('$gt'=>1)),
	        		array('field1_2'=>array('$gte'=>1)),
	        	)),
	        	'limit'=>10,
	        	'skip'=>20,
	        	'hint'=>'fooHint',
	        	'readConcern'=>'boo',
        	),
        	$command);
        $this->assertEquals(array($pc,'_resultCount'),$resultFormatter);
        $this->assertFalse($mapped);
    }

    public function testQueryCountOnEntityManager()
    {
        $collection = 'category';
        $data = array(
            array('name'=>'test1','field1'=>5, 'field2'=>20),
            array('name'=>'test2','field1'=>20,'field2'=>21),
            array('name'=>'test3','field1'=>21,'field2'=>6),
            array('name'=>'test4','field1'=>7, 'field2'=>7),
        );
        $ids = $this->setData($collection,$data);
        $mm = new ModuleManager($this->getConfig());
        $em = $mm->getServiceLocator()->get('EntityManager');
        $cb = $mm->getServiceLocator()->get('CriteriaBuilder');

        $q = $cb->createQuery();
        $r = $q->from(__NAMESPACE__.'\TestEntity');
        $p1 = $cb->parameter('integer','p1');
        $q->select($cb->count($r))
            ->where($cb->and_(
                $cb->gt($r->get('field1'),$p1),
                $cb->le($p1,$r->get('field2'))));

        $query = $em->createQuery($q)->setParameter('p1', 10);
        $this->assertEquals(1,$query->getSingleResult());
        $query = $em->createQuery($q)->setParameter('p1', 6);
        $this->assertEquals(3,$query->getSingleResult());
    }
/*
    public function testGroupCommand()
    {
        $collection = 'category';
        $data = array(
            array('name'=>'test1','level'=>1, 'qty'=>10),
            array('name'=>'test2','level'=>2, 'qty'=>20),
            array('name'=>'test1','level'=>3, 'qty'=>30),
            array('name'=>'test2','level'=>4, 'qty'=>40),
        );
        $ids = $this->setData($collection,$data);

        $client = $this->getMongoClient();
        $cmd = new \MongoDB\Driver\Command(array('group'=> array(
            'ns' => 'category',
            'key' => array('name'=>1),
            //'initial' => array('count'=>0, 'sum' => 0),
            //'$reduce' => 'function (curr, result) { result.count++; result.sum += curr.qty; }',
            'initial' => array('level'=>0, 'qty' => 0),
            '$reduce' => 'function (curr, result) { result.level = curr.level; result.qty = curr.qty; }',
            'cond' => array('level'=>array('$gte'=>2)),
        )));
        $cursor = $client->executeCommand('test',$cmd);
        foreach ($cursor as $data) {
            ;
        }
    }
*/
    public function testAggregationQueryOnEntityManager()
    {
        $collection = 'category';
        $data = array(
            array('name'=>'test1','level'=>1, 'qty'=>10),
            array('name'=>'test2','level'=>2, 'qty'=>20),
            array('name'=>'test1','level'=>3, 'qty'=>30),
            array('name'=>'test2','level'=>4, 'qty'=>40),
        );
        $ids = $this->setData($collection,$data);

        $mm = new ModuleManager($this->getConfig());
        $em = $mm->getServiceLocator()->get('EntityManager');
        $cb = $mm->getServiceLocator()->get('CriteriaBuilder');

        $q = $cb->createQuery();
        $r = $q->from(__NAMESPACE__.'\TestEntity');
        $p1 = $cb->parameter('integer','p1');
        $q->multiselect(
                $r->get('name'),
                $cb->count($r->get('name'))->alias('count'),
                $cb->min($r->get('qty'))->alias('min'),
                $cb->max($r->get('qty'))->alias('max'),
                $cb->sum($r->get('qty'))->alias('sum')
            )
            ->groupBy($r->get('name'))
            ->having($cb->gt($r->get('level'),$p1));

        $query = $em->createQuery($q)->setParameter('p1', 0);
        $count = 0;
        foreach ($query->getResultList() as $value) {
            if($value['name']=='test1') {
                $this->assertEquals(2,$value['count']);
                $this->assertEquals(10,$value['min']);
                $this->assertEquals(30,$value['max']);
                $this->assertEquals(40,$value['sum']);
            } elseif ($value['name']=='test2') {
                $this->assertEquals(2,$value['count']);
                $this->assertEquals(20,$value['min']);
                $this->assertEquals(40,$value['max']);
                $this->assertEquals(60,$value['sum']);
            }
            $count++;
        }
        $this->assertEquals(2,$count);

        $query = $em->createQuery($q)->setParameter('p1', 1);
        $count = 0;
        foreach ($query->getResultList() as $value) {
            if($value['name']=='test1') {
                $this->assertEquals(1,$value['count']);
                $this->assertEquals(30,$value['min']);
                $this->assertEquals(30,$value['max']);
                $this->assertEquals(30,$value['sum']);
            } elseif ($value['name']=='test2') {
                $this->assertEquals(2,$value['count']);
                $this->assertEquals(20,$value['min']);
                $this->assertEquals(40,$value['max']);
                $this->assertEquals(60,$value['sum']);
            }
            $count++;
        }
        $this->assertEquals(2,$count);
    }
}
