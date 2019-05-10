<?php
namespace RindowTest\Module\Mongodb\Stdlib\TraversableIteratorMongodbTest;

use PHPUnit\Framework\TestCase;
use MongoDb;
use IteratorIterator;

class Test extends TestCase
{
    static $RINDOW_TEST_DATA;
    public static $skip = false;
    public static function setUpBeforeClass()
    {
        if (!extension_loaded('mongodb')) {
            self::$skip = 'mongodb extension not loaded';
            return;
        }
    }
    public function setUp()
    {
        if(self::$skip) {
            $this->markTestSkipped(self::$skip);
            return;
        }
        try {
            $client = $this->getClient();
            $cmd = new \MongoDB\Driver\Command(array('drop'=>'test'));
            $client->executeCommand('test',$cmd);
        } catch(\Exception $e) {
            ;
        }
        try {
            $bulk = new MongoDB\Driver\BulkWrite();
            $bulk->insert(array('id'=>1,'name'=>'foo1'));
            $bulk->insert(array('id'=>2,'name'=>'foo2'));
            $client->executeBulkWrite('test.test', $bulk);
        } catch(\Exception $e) {
            self::$skip = $e->getMessage();
            $this->markTestSkipped(self::$skip);
            return;
        }
    }
    public function getClient()
    {
        $client = new \MongoDB\Driver\Manager();
        return $client;
    }
    public function getIterator($client=null)
    {
        if($client==null)
            $client = $this->getClient();
        $options = array(
            'projection'=>array('_id'=>0),
            'sort'=>array('name'=>1));
        $query = new \MongoDB\Driver\Query(array(),$options);
        $cursor = $client->executeQuery('test.test',$query);
        $cursor->setTypeMap(array('root'=>'array','document'=>'array','array'=>'array'));
        return $cursor;
    }

    public function testMongodb()
    {
        $client = $this->getClient();
        $stmt = $this->getIterator($client);
        $iterator = new IteratorIterator($stmt);

        $results = array();
        foreach ($iterator as $value) {
            $results[] = $value;
        }
        $this->assertCount(2,$results);

        $stmt = $this->getIterator($client);
        $iterator = new IteratorIterator($stmt);
        $iterator->rewind();
        $this->assertTrue($iterator->valid());
        $this->assertEquals(array('id'=>1,'name'=>'foo1'),$iterator->current());
        $iterator->next();
        $this->assertTrue($iterator->valid());
        $this->assertEquals(array('id'=>2,'name'=>'foo2'),$iterator->current());
        $iterator->next();
        $this->assertFalse($iterator->valid());

        // ***** it is NOT able to rewind and throw exception ******
        $error = false;
        try{
            $iterator->rewind();
        } catch(\MongoDB\Driver\Exception\LogicException $e) {
            $error = true;
        }
        $this->assertTrue($error);
    }
    public function testNestIterator()
    {
        $client = $this->getClient();
        $stmt = $this->getIterator($client);
        $iterator = new IteratorIterator($stmt);
        $iterator = new IteratorIterator($iterator);

        $results = array();
        foreach ($iterator as $value) {
            $results[] = $value;
        }
        $this->assertCount(2,$results);
    }
}
