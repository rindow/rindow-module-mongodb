<?php
namespace RindowTest\Module\Mongodb\Core\DataSourceTest;

use PHPUnit\Framework\TestCase;
use Rindow\Module\Mongodb\Core\DataSource;
use Rindow\Transaction\Local\TransactionManager;

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

    public function testDefault()
    {
        $dataSource = new DataSource(array('database'=>'test'));
        $connection = $dataSource->getConnection();
        $this->assertInstanceOf('Rindow\Module\Mongodb\Core\Connection',$connection);
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

}