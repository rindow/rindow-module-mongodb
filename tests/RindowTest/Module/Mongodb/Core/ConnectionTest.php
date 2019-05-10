<?php
namespace RindowTest\Module\Mongodb\Core\ConnectionTest;

use PHPUnit\Framework\TestCase;
use Rindow\Module\Mongodb\Core\Connection;

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

    public function testInsertAndFind()
    {
        $connection = new Connection(array('database'=>'test'));
        $this->assertEquals('test',$connection->getDatabase());
        $id = $connection->insert('test',array('a'=>10,'b'=>20));
        $this->assertInstanceOf('MongoDB\BSON\ObjectId',$id);
        $cursor = $connection->find('test',array());
        $this->assertInstanceOf('Rindow\Module\Mongodb\Core\Cursor',$cursor);
        $count = 0;
        foreach ($cursor as $document) {
            $this->assertEquals(10,$document['a']);
            $this->assertEquals(20,$document['b']);
            $this->assertEquals($id,$document['_id']);
            $count++;
        }
        $this->assertEquals(1,$count);

        $id2 = $connection->insert('test',array('a'=>30,'b'=>40));
        $cursor = $connection->find('test',array());
        $count = 0;
        foreach ($cursor as $document) {
            $count++;
        }
        $this->assertEquals(2,$count);

        $cursor = $connection->find('test',array('_id'=>$id2));
        $count = 0;
        foreach ($cursor as $document) {
            $this->assertEquals(30,$document['a']);
            $this->assertEquals(40,$document['b']);
            $this->assertEquals($id2,$document['_id']);
            $count++;
        }
        $this->assertEquals(1,$count);

        $id3 = $connection->insert('test',array('a'=>50,'b'=>60));
        $cursor = $connection->find('test',null,array('skip'=>1,'limit'=>1,'sort'=>array('a'=>1)));
        $count = 0;
        foreach ($cursor as $document) {
            $this->assertEquals(30,$document['a']);
            $this->assertEquals(40,$document['b']);
            $this->assertEquals($id2,$document['_id']);
            $count++;
        }
        $this->assertEquals(1,$count);
    }

    public function testUpdate()
    {
        $connection = new Connection(array('database'=>'test'));
        $this->assertEquals('test',$connection->getDatabase());
        $id  = $connection->insert('test',array('a'=>10,'b'=>20));
        $id2 = $connection->insert('test',array('a'=>10,'b'=>40));
        $cursor = $connection->find('test',array());
        $count = 0;
        foreach ($cursor as $document) {
            $count++;
        }
        $this->assertEquals(2,$count);

        $connection->update('test',array('_id'=>$id),array('b'=>30));
        $cursor = $connection->find('test',array('_id'=>$id));
        $count = 0;
        foreach ($cursor as $document) {
            $this->assertFalse(isset($document['a']));
            $this->assertEquals(30,$document['b']);
            $this->assertEquals($id,$document['_id']);
            $count++;
        }
        $this->assertEquals(1,$count);
        $cursor = $connection->find('test',array('_id'=>$id2));
        $count = 0;
        foreach ($cursor as $document) {
            $this->assertEquals(10,$document['a']);
            $this->assertEquals(40,$document['b']);
            $this->assertEquals($id2,$document['_id']);
            $count++;
        }
        $this->assertEquals(1,$count);
    }

    public function testDelete()
    {
        $connection = new Connection(array('database'=>'test'));
        $this->assertEquals('test',$connection->getDatabase());
        $id  = $connection->insert('test',array('a'=>10,'b'=>20));
        $id2 = $connection->insert('test',array('a'=>10,'b'=>40));
        $cursor = $connection->find('test',array());
        $count = 0;
        foreach ($cursor as $document) {
            $count++;
        }
        $this->assertEquals(2,$count);

        $connection->delete('test',array('_id'=>$id));
        $cursor = $connection->find('test',array('_id'=>$id));
        $count = 0;
        foreach ($cursor as $document) {
            $count++;
        }
        $this->assertEquals(0,$count);
        $cursor = $connection->find('test',array());
        $count = 0;
        foreach ($cursor as $document) {
            $this->assertEquals(10,$document['a']);
            $this->assertEquals(40,$document['b']);
            $this->assertEquals($id2,$document['_id']);
            $count++;
        }
        $this->assertEquals(1,$count);
    }

    public function testCommand()
    {
        $connection = new Connection(array('database'=>'test'));
        $cursor = $connection->executeCommand(array('listCollections'=>1));
        $count=0;
        foreach ($cursor as $value) {
            if($value['name']=='test')
                $count++;
        }
        $this->assertEquals(0,$count);
        $cursor = $connection->executeCommand(array('create'=>'test'));
        $cursor = $connection->executeCommand(array('listCollections'=>1));
        $count=0;
        foreach ($cursor as $value) {
            if($value['name']=='test')
                $count++;
        }
        $this->assertEquals(1,$count);
    }

    public function testCount()
    {
        $connection = new Connection(array('database'=>'test'));
        $this->assertEquals('test',$connection->getDatabase());

        $this->assertEquals(0,$connection->count('test'));
        $this->assertEquals(0,$connection->count('test'),array());
        $this->assertEquals(0,$connection->count('test'),array('a'=>10));

        $id  = $connection->insert('test',array('a'=>10,'b'=>20));
        $id2 = $connection->insert('test',array('a'=>10,'b'=>40));

        $this->assertEquals(2,$connection->count('test'));
        $this->assertEquals(2,$connection->count('test',array()));
        $this->assertEquals(2,$connection->count('test',array('a'=>10)));
        $this->assertEquals(1,$connection->count('test',array('b'=>20)));
    }

    public function testGroup()
    {
        $connection = new Connection(array('database'=>'test'));
        $this->assertEquals('test',$connection->getDatabase());

        $key = array('b'=>1);
        $reduce = 'function(current, result) { result.sum += current.a; }';
        $initial = array('sum'=>0);
        $cursor = $connection->group('test',$key,$reduce,$initial);
        $count = 0;
        foreach($cursor as $row) {
            $count++;
        }
        $this->assertEquals(0,$count);

        $id  = $connection->insert('test',array('a'=>10,'b'=>20));
        $id2 = $connection->insert('test',array('a'=>10,'b'=>40));
        $id  = $connection->insert('test',array('a'=>20,'b'=>20));

        $cursor = $connection->group('test',$key,$reduce,$initial);
        $count = 0;
        foreach($cursor as $row) {
            if($row['b']==20)
                $this->assertEquals(30,$row['sum']);
            elseif($row['b']==40)
                $this->assertEquals(10,$row['sum']);
            else
                throw new \Exception("Error Processing Request", 1);
            $count++;
        }
        $this->assertEquals(2,$count);

        $options = array('cond'=>array('b'=>20));
        $cursor = $connection->group('test',$key,$reduce,$initial,$options);
        $count = 0;
        foreach($cursor as $row) {
            if($row['b']==20)
                $this->assertEquals(30,$row['sum']);
            else
                throw new \Exception("Error Processing Request", 1);
            $count++;
        }
        $this->assertEquals(1,$count);
    }

    public function testFlush()
    {
        $connection = new Connection(array('database'=>'test'));
        $connection->setBulkMode(true);
        $id  = $connection->insert('test',array('a'=>10,'b'=>20));
        $id2 = $connection->insert('test',array('a'=>10,'b'=>40));
        $id3 = $connection->insert('test2',array('a'=>10,'b'=>40));
        $cursor = $connection->find('test',array());
        $count = 0;
        foreach ($cursor as $document) {
            $count++;
        }
        $this->assertEquals(0,$count);
        $cursor = $connection->find('test2',array('_id'=>$id3));
        $count = 0;
        foreach ($cursor as $document) {
            $count++;
        }
        $this->assertEquals(0,$count);

        $connection->flush();
        $cursor = $connection->find('test',array());
        $count = 0;
        foreach ($cursor as $document) {
            $count++;
        }
        $this->assertEquals(2,$count);
        $cursor = $connection->find('test2',array('_id'=>$id3));
        $count = 0;
        foreach ($cursor as $document) {
            $count++;
        }
        $this->assertEquals(1,$count);
        $cursor = $connection->executeCommand(array('drop'=>'test2'));
    }

    public function testClean()
    {
        $connection = new Connection(array('database'=>'test'));
        $connection->setBulkMode(true);
        $id  = $connection->insert('test',array('a'=>10,'b'=>20));
        $id2 = $connection->insert('test',array('a'=>10,'b'=>40));
        $id3 = $connection->insert('test2',array('a'=>10,'b'=>40));
        $cursor = $connection->find('test',array());
        $count = 0;
        foreach ($cursor as $document) {
            $count++;
        }
        $this->assertEquals(0,$count);
        $cursor = $connection->find('test2',array('_id'=>$id3));
        $count = 0;
        foreach ($cursor as $document) {
            $count++;
        }
        $this->assertEquals(0,$count);

        $connection->clean();
        $cursor = $connection->find('test',array());
        $count = 0;
        foreach ($cursor as $document) {
            $count++;
        }
        $this->assertEquals(0,$count);
        $cursor = $connection->find('test2',array('_id'=>$id3));
        $count = 0;
        foreach ($cursor as $document) {
            $count++;
        }
        $this->assertEquals(0,$count);
        try {
            $cursor = $connection->executeCommand(array('drop'=>'test2'));
        } catch(\Exception $e) {
            ;
        }
    }

    public function testServers()
    {
        if(version_compare(MONGODB_VERSION, '1.2.0')<0) {
            $this->markTestSkipped('mongodb driver version < 1.2.0');
            return;
        }
        $connection = new Connection(array('database'=>'test'));
        $servers = $connection->getServers();
        $this->assertInstanceOf('MongoDB\Driver\Server',$servers[0]);
    }

    public function testConnected()
    {
        $status = new \stdClass();
        $status->called = 0;
        $connection = new Connection(array('database'=>'test'));
        $connection->setConnectedEventListener(function($connection) use ($status) {
            if(!($connection instanceof Connection))
                throw new \Exception('there is no connnection class');
            $status->called++;
        });
        $this->assertFalse($connection->isConnected());
        $this->assertEquals(0,$status->called);
        $id  = $connection->insert('test',array('a'=>10,'b'=>20));
        $this->assertTrue($connection->isConnected());
        $this->assertEquals(1,$status->called);
        $id2  = $connection->insert('test',array('a'=>10,'b'=>20));
        $this->assertTrue($connection->isConnected());
        $this->assertEquals(1,$status->called);
    }
}
