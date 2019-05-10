<?php
namespace RindowTest\Module\Mongodb\Pagenator\PaginatorAdapterTest;

use PHPUnit\Framework\TestCase;
use Rindow\Stdlib\Paginator\Paginator;

use Rindow\Module\Mongodb\Core\Connection;
use Rindow\Module\Mongodb\Support\AbstractStdClass;
use Rindow\Module\Mongodb\Paginator\MongodbAdapter;

class TestEntity extends AbstractStdClass
{
    public $num;
    public $name;
}

class Loader
{
    public function load($row)
    {
        if(!$row)
            return $row;
        $row->opt = 'opt-'.$row->num;
        return $row;
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
            $cursor = $client->executeQuery('test.test',$query);
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
        $cmd = new \MongoDB\Driver\Command(array('drop'=>'test'));
        try {
            $client->executeCommand('test',$cmd);
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
        $cmd = new \MongoDB\Driver\Command(array('drop'=>'test'));
        try {
            $client->executeCommand('test',$cmd);
        } catch(\Exception $e) {
            ;
        }
    }

    public function setUpData($count)
    {
        $manager = new \MongoDB\Driver\Manager();
        $db = 'test';
        $collection = 'test';
        $bulkWrite = new \MongoDB\Driver\BulkWrite();
        for ($i=0; $i < $count; $i++) { 
            $doc = array('num'=>$i+1, 'name'=>'item-'.($i+1));
            $bulkWrite->insert($doc);
        }
        $manager->executeBulkWrite($db.'.'.$collection,$bulkWrite);
    }

    public function testNormal()
    {
        $this->setUpData(20);
        $db = 'test';
        $collection = 'test';
        $filter = array('num'=>array('$lt'=>16));
        $className = __NAMESPACE__.'\TestEntity';

        $connection = new Connection(array('database'=>$db));
        $this->assertEquals('test',$connection->getDatabase());
        $paginatorAdapter = new MongodbAdapter($connection);
        $paginatorAdapter->setQuery($collection,$filter,$options=array(),$className);
        $loader = new Loader();
        $paginatorAdapter->setLoader(array($loader,'load'));

        $paginator = new Paginator($paginatorAdapter);
        $paginator->setPage(2);

        $this->assertEquals(15, $paginator->getTotalItems());
        $results = array();
        $opt = array();
        foreach ($paginator as $value) {
            $this->assertEquals($className,get_class($value));
            $results[$value->num] = $value->name;
            $opt[$value->num] = $value->opt;
        }
        $this->assertEquals(
            array(6=>'item-6',7=>'item-7',8=>'item-8',9=>'item-9',10=>'item-10'),
            $results);
        $this->assertEquals(
            array(6=>'opt-6',7=>'opt-7',8=>'opt-8',9=>'opt-9',10=>'opt-10'),
            $opt);
    }
}