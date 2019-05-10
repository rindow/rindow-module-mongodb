<?php
namespace RindowTest\Module\Mongodb\Core\CursorTest;

use PHPUnit\Framework\TestCase;
use Rindow\Module\Mongodb\Core\Connection;
use Rindow\Module\Mongodb\Support\AbstractStdClass;

class TestEntity extends AbstractStdClass
{
    public $_id;
    public $a;
    public $b;
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
        $client = new \MongoDB\Driver\Manager();
        $cmd = new \MongoDB\Driver\Command(array('drop'=>'test2'));
        try {
            $client->executeCommand('test',$cmd);
        } catch(\Exception $e) {
            ;
        }
    }

    public function testDefault()
    {
        $connection = new Connection(array('database'=>'test'));
        $this->assertEquals('test',$connection->getDatabase());
        $id = $connection->insert('test',array('a'=>10,'b'=>20));
        $id2 = $connection->insert('test',array('a'=>30,'b'=>40));
        $cursor = $connection->find('test',array());
        $this->assertInstanceOf('Rindow\Module\Mongodb\Core\Cursor',$cursor);
        $count = 0;
        foreach ($cursor as $document) {
            $count++;
        }
        $this->assertEquals(2,$count);
    }

    public function testFetch()
    {
        $connection = new Connection(array('database'=>'test'));
        $this->assertEquals('test',$connection->getDatabase());
        $id = $connection->insert('test',array('a'=>10,'b'=>20));
        $id2 = $connection->insert('test',array('a'=>30,'b'=>40));
        $cursor = $connection->find('test',array());
        $this->assertInstanceOf('Rindow\Module\Mongodb\Core\Cursor',$cursor);
        $count = 0;
        while($document = $cursor->fetch()) {
            if($count==0) {
                $this->assertEquals(10,$document['a']);
                $this->assertEquals(20,$document['b']);
                $this->assertEquals($id,$document['_id']);
            } else {
                $this->assertEquals(30,$document['a']);
                $this->assertEquals(40,$document['b']);
                $this->assertEquals($id2,$document['_id']);
            }
            $count++;
        }
        $this->assertEquals(2,$count);
    }

    public function testTypeMap()
    {
        $connection = new Connection(array('database'=>'test'));
        $this->assertEquals('test',$connection->getDatabase());
        $entity = new TestEntity();
        $entity->a = 10;
        $entity->b = 20;
        $id = $connection->insert('test',$entity);
        $this->assertInstanceOf('MongoDB\BSON\ObjectId',$id);
        $cursor = $connection->find('test',array());
        $this->assertInstanceOf('Rindow\Module\Mongodb\Core\Cursor',$cursor);
        $typeMap = $connection->getTypeMap();
        $typeMap['root'] = __NAMESPACE__.'\TestEntity';
        $cursor->setTypeMap($typeMap);
        $count = 0;
        foreach ($cursor as $document) {
            $this->assertInstanceOf(__NAMESPACE__.'\TestEntity',$document);
            $this->assertEquals(10,$document->a);
            $this->assertEquals(20,$document->b);
            $this->assertEquals($id,$document->_id);
            $count++;
        }
        $this->assertEquals(1,$count);
    }

    public function testMultiCursor()
    {
        $connection = new Connection(array('database'=>'test'));
        $this->assertEquals('test',$connection->getDatabase());
        $id11 = $connection->insert('test',array('a'=>10,'b'=>20));
        $id12 = $connection->insert('test',array('a'=>30,'b'=>40));
        $id21 = $connection->insert('test2',array('a'=>10,'b'=>20));
        $id22 = $connection->insert('test2',array('a'=>30,'b'=>40));

        $cursor = $connection->find('test',array());
        $this->assertInstanceOf('Rindow\Module\Mongodb\Core\Cursor',$cursor);
        $count = 0;
        foreach ($cursor as $document) {
            $count++;
        }
        $this->assertEquals(2,$count);

        $cursor = $connection->find('test2',array());
        $this->assertInstanceOf('Rindow\Module\Mongodb\Core\Cursor',$cursor);
        $count = 0;
        foreach ($cursor as $document) {
            $count++;
        }
        $this->assertEquals(2,$count);

    }
}
