<?php
namespace RindowTest\Module\Mongodb\Repository\MongodbRepositoryTest;

use PHPUnit\Framework\TestCase;
use Rindow\Module\Mongodb\Repository\MongodbRepository;
//use Rindow\Module\Mongodb\Repository\MongodbRepositoryFactory;
use Rindow\Module\Mongodb\Core\DataSource;
use MongoDB\BSON\ObjectId;
use Interop\Lenient\Dao\Repository\DataMapper;
use Interop\Lenient\Dao\Query\Expression;
use Rindow\Database\Dao\Support\QueryBuilder;

class TestDataMapper implements DataMapper
{
    public function map($data)
    {
        $data['id'] = $data['_id'];
        unset($data['_id']);
        return (object)$data;
    }

    public function demap($entity)
    {
        //var_dump($entity);
        $data = get_object_vars($entity);
        if(isset($data['id'])) {
            $data['_id'] = $data['id'];
        }
        unset($data['id']);
        return $data;
    }

    public function fillId($entity,$id)
    {
        $entity->id = $id;
        return $entity;
    }
    public function getFetchClass()
    {
        return null;
    }
}

class TestMongodbRepository extends MongodbRepository
{
    public function map($data)
    {
        $data['id'] = $data['_id'];
        unset($data['_id']);
        return (object)$data;
    }

    public function demap($entity)
    {
        //var_dump($entity);
        $data = get_object_vars($entity);
        if(isset($data['id'])) {
            $data['_id'] = $data['id'];
        }
        unset($data['id']);
        return $data;
    }

    public function fillId($entity,$id)
    {
        $entity->id = $id;
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
            $cursor = $client->executeQuery('test.testcol',$query);
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
        $cmd = new \MongoDB\Driver\Command(array('drop'=>'testcol'));
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

    public function getRepository($collection,$className=null)
    {
        $queryBuilder = new QueryBuilder();
        $dataSource = new DataSource(array('database'=>'test'));
        if($className) {
            $repository = new $className($dataSource,$collection,$queryBuilder);
        } else {
            $repository = new MongodbRepository($dataSource,$collection,$queryBuilder);
        }
        return $repository;
    }

    public function testInsertNormal()
    {
        $repository = $this->getRepository('testcol');

        $row = $repository->save(array('name'=>'test','day'=>1,'ser'=>1));
        $id = $row['id'];
        $this->assertInstanceOf('MongoDB\BSON\ObjectId',$id);
        $this->assertEquals(array('id'=>$id,'name'=>'test','day'=>1,'ser'=>1),$row);
        $row = $repository->save(array('name'=>'test2','day'=>1,'ser'=>10));
        $id2 = $row['id'];
        $this->assertInstanceOf('MongoDB\BSON\ObjectId',$id2);
        $this->assertEquals(array('id'=>$id2,'name'=>'test2','day'=>1,'ser'=>10),$row);
        $this->assertNotEquals(strval($id),strval($id2));

        $cursor = $this->getData('testcol');
        $count = 0;
        foreach($cursor as $row) {
            if($row['_id']==$id)
                $this->assertEquals(array('_id'=>$id,'name'=>'test','day'=>1,'ser'=>1),$row);
            elseif($row['_id']==$id2)
                $this->assertEquals(array('_id'=>$id2,'name'=>'test2','day'=>1,'ser'=>10),$row);
            else {
                //var_dump(array($id,$id2,$row['_id']));
                $this->assertTrue(false);
            }
            $count++;
        }
        $this->assertEquals(2,$count);
    }

    public function testUpdateNormal()
    {
        $repository = $this->getRepository('testcol');
        $row = $repository->save(array('name'=>'test','day'=>1,'ser'=>1));
        $id = $row['id'];
        $this->assertInstanceOf('MongoDB\BSON\ObjectId',$id);
        $this->assertEquals(array('id'=>$id,'name'=>'test','day'=>1,'ser'=>1),$row);
        $row = $repository->save(array('name'=>'test2','day'=>1,'ser'=>10));
        $id2 = $row['id'];
        $this->assertInstanceOf('MongoDB\BSON\ObjectId',$id2);
        $this->assertEquals(array('id'=>$id2,'name'=>'test2','day'=>1,'ser'=>10),$row);
        $this->assertNotEquals(strval($id),strval($id2));

        $repository->save(array('id'=>$id,'name'=>'update1'));

        $cursor = $this->getData('testcol');
        $count = 0;
        foreach($cursor as $row) {
            if($row['_id']==$id)
                $this->assertEquals(array('_id'=>$id,'name'=>'update1'),$row);
            elseif($row['_id']==$id2)
                $this->assertEquals(array('_id'=>$id2,'name'=>'test2','day'=>1,'ser'=>10),$row);
            else {
                //var_dump(array($id,$id2,$row['_id']));
                $this->assertTrue(false);
            }
            $count++;
        }
        $this->assertEquals(2,$count);
    }

    public function testUpdateUpsert()
    {
        $repository = $this->getRepository('testcol');
        $id = new ObjectId();
        $row = $repository->save(array('id'=>$id,'name'=>'test','day'=>1,'ser'=>1));
        $this->assertEquals($id,$row['id']);
        $id = $row['id'];
        $this->assertInstanceOf('MongoDB\BSON\ObjectId',$id);
        $this->assertEquals(array('id'=>$id,'name'=>'test','day'=>1,'ser'=>1),$row);

        $cursor = $this->getData('testcol');
        $count = 0;
        foreach($cursor as $row) {
            if($row['_id']==$id)
                $this->assertEquals(array('_id'=>$id,'name'=>'test','day'=>1,'ser'=>1),$row);
            else {
                //var_dump(array($id,$id2,$row['_id']));
                $this->assertTrue(false);
            }
            $count++;
        }
        $this->assertEquals(1,$count);
    }

    public function testDeleteAndDeleteByIdNormal()
    {
        $repository = $this->getRepository('testcol');
        $row = $repository->save(array('name'=>'test','day'=>1,'ser'=>1));
        $id = $row['id'];
        $this->assertInstanceOf('MongoDB\BSON\ObjectId',$id);
        $this->assertEquals(array('id'=>$id,'name'=>'test','day'=>1,'ser'=>1),$row);
        $row = $repository->save(array('name'=>'test2','day'=>1,'ser'=>10));
        $id2 = $row['id'];
        $this->assertInstanceOf('MongoDB\BSON\ObjectId',$id2);
        $this->assertEquals(array('id'=>$id2,'name'=>'test2','day'=>1,'ser'=>10),$row);
        $this->assertNotEquals(strval($id),strval($id2));

        $repository->deleteById($id);

        $cursor = $this->getData('testcol');
        $count = 0;
        foreach($cursor as $row) {
            if($row['_id']==$id2)
                $this->assertEquals(array('_id'=>$id2,'name'=>'test2','day'=>1,'ser'=>10),$row);
            else {
                //r_dump(array($id,$id2,$row['_id']));
                $this->assertTrue(false);
            }
            $count++;
        }
        $this->assertEquals(1,$count);

        $row = $repository->save(array('name'=>'test3','day'=>1,'ser'=>30));
        $id3 = $row['id'];
        $repository->delete(array('id'=>$id2,'name'=>'test2','day'=>1,'ser'=>10));
        $cursor = $this->getData('testcol');
        $count = 0;
        foreach($cursor as $row) {
            if($row['_id']==$id3)
                $this->assertEquals(array('_id'=>$id3,'name'=>'test3','day'=>1,'ser'=>30),$row);
            else {
                $this->assertTrue(false);
            }
            $count++;
        }
        $this->assertEquals(1,$count);
    }

    public function testDeleteAllNormal()
    {
        $repository = $this->getRepository('testcol');
        $row = $repository->save(array('name'=>'test','day'=>1,'ser'=>1));
        $id = $row['id'];
        $this->assertInstanceOf('MongoDB\BSON\ObjectId',$id);
        $this->assertEquals(array('id'=>$id,'name'=>'test','day'=>1,'ser'=>1),$row);
        $row = $repository->save(array('name'=>'test2','day'=>1,'ser'=>10));
        $id2 = $row['id'];
        $this->assertInstanceOf('MongoDB\BSON\ObjectId',$id2);
        $this->assertEquals(array('id'=>$id2,'name'=>'test2','day'=>1,'ser'=>10),$row);
        $this->assertNotEquals(strval($id),strval($id2));

        $repository->deleteAll(array('name'=>'test'));

        $cursor = $this->getData('testcol');
        $count = 0;
        foreach($cursor as $row) {
            if($row['_id']==$id2)
                $this->assertEquals(array('_id'=>$id2,'name'=>'test2','day'=>1,'ser'=>10),$row);
            else {
                //r_dump(array($id,$id2,$row['_id']));
                $this->assertTrue(false);
            }
            $count++;
        }
        $this->assertEquals(1,$count);

        $row = $repository->save(array('name'=>'test3','day'=>1,'ser'=>30));
        $id3 = $row['id'];
        $repository->deleteAll();
        $cursor = $this->getData('testcol');
        $count = 0;
        foreach($cursor as $row) {
            $count++;
        }
        $this->assertEquals(0,$count);
    }

    public function testFindNormal()
    {
        $repository = $this->getRepository('testcol');
        $row = $repository->save(array('name'=>'test','day'=>1,'ser'=>1));
        $id = $row['id'];
        $this->assertInstanceOf('MongoDB\BSON\ObjectId',$id);
        $this->assertEquals(array('id'=>$id,'name'=>'test','day'=>1,'ser'=>1),$row);
        $row = $repository->save(array('name'=>'test2','day'=>1,'ser'=>10));
        $id2 = $row['id'];
        $this->assertInstanceOf('MongoDB\BSON\ObjectId',$id2);
        $this->assertEquals(array('id'=>$id2,'name'=>'test2','day'=>1,'ser'=>10),$row);
        $this->assertNotEquals(strval($id),strval($id2));

        $cursor = $repository->findAll(array('name'=>'test2'));
        $count = 0;
        foreach($cursor as $row) {
            if($row['id']==$id2)
                $this->assertEquals(array('id'=>$id2,'name'=>'test2','day'=>1,'ser'=>10),$row);
            else {
                //var_dump(array($id,$id2,$row['id']));
                $this->assertTrue(false);
            }
            $count++;
        }
        $this->assertEquals(1,$count);

        $filter = array();
        $filter[] = $repository->getQueryBuilder()->createExpression('ser',Expression::GREATER_THAN,1);
        $cursor = $repository->findAll($filter);
        $count = 0;
        foreach($cursor as $row) {
            if($row['id']==$id2)
                $this->assertEquals(array('id'=>$id2,'name'=>'test2','day'=>1,'ser'=>10),$row);
            else {
                //var_dump(array($id,$id2,$row['id']));
                $this->assertTrue(false);
            }
            $count++;
        }
        $this->assertEquals(1,$count);

        $filter = array();
        $filter[] = $repository->getQueryBuilder()->createExpression('ser',Expression::GREATER_THAN_OR_EQUAL,10);
        $cursor = $repository->findAll($filter);
        $count = 0;
        foreach($cursor as $row) {
            if($row['id']==$id2)
                $this->assertEquals(array('id'=>$id2,'name'=>'test2','day'=>1,'ser'=>10),$row);
            else {
                //var_dump(array($id,$id2,$row['id']));
                $this->assertTrue(false);
            }
            $count++;
        }
        $this->assertEquals(1,$count);

        $filter = array();
        $filter[] = $repository->getQueryBuilder()->createExpression('ser',Expression::LESS_THAN,10);
        $cursor = $repository->findAll($filter);
        $count = 0;
        foreach($cursor as $row) {
            if($row['id']==$id)
                $this->assertEquals(array('id'=>$id,'name'=>'test','day'=>1,'ser'=>1),$row);
            else {
                //var_dump(array($id,$id2,$row['id']));
                $this->assertTrue(false);
            }
            $count++;
        }
        $this->assertEquals(1,$count);

        $filter = array();
        $filter[] = $repository->getQueryBuilder()->createExpression('ser',Expression::LESS_THAN_OR_EQUAL,1);
        $cursor = $repository->findAll($filter);
        $count = 0;
        foreach($cursor as $row) {
            if($row['id']==$id)
                $this->assertEquals(array('id'=>$id,'name'=>'test','day'=>1,'ser'=>1),$row);
            else {
                //var_dump(array($id,$id2,$row['id']));
                $this->assertTrue(false);
            }
            $count++;
        }
        $this->assertEquals(1,$count);

        $filter = array();
        $filter[] = $repository->getQueryBuilder()->createExpression('ser',Expression::NOT_EQUAL,1);
        $cursor = $repository->findAll($filter);
        $count = 0;
        foreach($cursor as $row) {
            if($row['id']==$id2)
                $this->assertEquals(array('id'=>$id2,'name'=>'test2','day'=>1,'ser'=>10),$row);
            else {
                //var_dump(array($id,$id2,$row['id']));
                $this->assertTrue(false);
            }
            $count++;
        }
        $this->assertEquals(1,$count);

        $filter = array();
        $filter[] = $repository->getQueryBuilder()->createExpression('ser',Expression::IN,array(10));
        $cursor = $repository->findAll($filter);
        $count = 0;
        foreach($cursor as $row) {
            if($row['id']==$id2)
                $this->assertEquals(array('id'=>$id2,'name'=>'test2','day'=>1,'ser'=>10),$row);
            else {
                //var_dump(array($id,$id2,$row['id']));
                $this->assertTrue(false);
            }
            $count++;
        }
        $this->assertEquals(1,$count);

        $filter = array();
        $filter[] = $repository->getQueryBuilder()->createExpression('name',Expression::BEGIN_WITH,'test2');
        $cursor = $repository->findAll($filter);
        $count = 0;
        foreach($cursor as $row) {
            if($row['id']==$id2)
                $this->assertEquals(array('id'=>$id2,'name'=>'test2','day'=>1,'ser'=>10),$row);
            else {
                //var_dump(array($id,$id2,$row['id']));
                $this->assertTrue(false);
            }
            $count++;
        }
        $this->assertEquals(1,$count);

        $filter = array();
        $filter[] = $repository->getQueryBuilder()->createExpression('name',Expression::BEGIN_WITH,'test');
        $cursor = $repository->findAll($filter);
        $count = 0;
        foreach($cursor as $row) {
            $count++;
        }
        $this->assertEquals(2,$count);

        $row = $repository->save(array('name'=>'test3','day'=>1,'ser'=>100));
        $id3 = $row['id'];
        $filter = array();
        $filter[] = $repository->getQueryBuilder()->createExpression('ser',Expression::GREATER_THAN_OR_EQUAL,5);
        $filter[] = $repository->getQueryBuilder()->createExpression('ser',Expression::LESS_THAN_OR_EQUAL,10);
        $cursor = $repository->findAll($filter);
        $count = 0;
        foreach($cursor as $row) {
            if($row['id']==$id2)
                $this->assertEquals(array('id'=>$id2,'name'=>'test2','day'=>1,'ser'=>10),$row);
            else {
                //var_dump(array($id,$id2,$row['id']));
                $this->assertTrue(false);
            }
            $count++;
        }
        $this->assertEquals(1,$count);

    }

    /**
     * @expectedException        Rindow\Database\Dao\Exception\RuntimeException
     * @expectedExceptionMessage Normally expression must not include array value.
     */
    public function testIllegalArrayValue1()
    {
        $repository = $this->getRepository('testcol');
        $filter['a'] = array('a1','b1');
        $results = $repository->findAll($filter);
    }

    /**
     * @expectedException        Rindow\Database\Dao\Exception\RuntimeException
     * @expectedExceptionMessage Normally expression must not include array value.
     */
    public function testIllegalArrayValue2()
    {
        $repository = $this->getRepository('testcol');
        $filter = array();
        $filter[] = $repository->getQueryBuilder()->createExpression('a',Expression::EQUAL,array('a1'));
        $results = $repository->findAll($filter);
    }

    public function testGetNormal()
    {
        $repository = $this->getRepository('testcol');
        $row = $repository->save(array('name'=>'test','day'=>1,'ser'=>1));
        $id = $row['id'];
        $this->assertInstanceOf('MongoDB\BSON\ObjectId',$id);
        $this->assertEquals(array('id'=>$id,'name'=>'test','day'=>1,'ser'=>1),$row);
        $row = $repository->save(array('name'=>'test2','day'=>1,'ser'=>10));
        $id2 = $row['id'];
        $this->assertInstanceOf('MongoDB\BSON\ObjectId',$id2);
        $this->assertEquals(array('id'=>$id2,'name'=>'test2','day'=>1,'ser'=>10),$row);
        $this->assertNotEquals(strval($id),strval($id2));

        $row = $repository->findById($id2);
        $this->assertEquals(array('id'=>$id2,'name'=>'test2','day'=>1,'ser'=>10),$row);
    }

    public function testGetNoData()
    {
        $repository = $this->getRepository('testcol');
        $row = $repository->save(array('name'=>'test','day'=>1,'ser'=>1));
        $id = $row['id'];
        $repository->deleteById($id);

        $row = $repository->findById($id);
        $this->assertNull($row);
    }

    public function testCount()
    {
        $repository = $this->getRepository('testcol');
        $row = $repository->save(array('name'=>'test','day'=>1,'ser'=>1));
        $row = $repository->save(array('name'=>'test2','day'=>1,'ser'=>10));

        $count = $repository->count();
        $this->assertEquals(2,$count);

        $count = $repository->count(array('name'=>'test2'));
        $this->assertEquals(1,$count);

        $count = $repository->count(array('name'=>'test3'));
        $this->assertEquals(0,$count);
    }

    public function testExistsById()
    {
        $repository = $this->getRepository('testcol');
        $row = $repository->save(array('name'=>'test','day'=>1,'ser'=>1));
        $id = $row['id'];
        $this->assertEquals(array('id'=>$id,'name'=>'test','day'=>1,'ser'=>1),$row);
        $row = $repository->save(array('name'=>'test2','day'=>1,'ser'=>10));
        $id2 = $row['id'];
        $this->assertEquals(array('id'=>$id2,'name'=>'test2','day'=>1,'ser'=>10),$row);
        $row = $repository->save(array('name'=>'test3','day'=>1,'ser'=>10));
        $id3 = $row['id'];
        $repository->deleteById($id3);

        $this->assertTrue($repository->existsById($id));
        $this->assertTrue($repository->existsById($id2));
        $this->assertFalse($repository->existsById($id3));
    }

    public function testDataMapper()
    {
        $repository = $this->getRepository('testcol');
        $repository->setDataMapper(new TestDataMapper());

        $entity = new \stdClass();
        $entity->id = null;
        $entity->a = 'a1';
        $entity2 = $repository->save($entity);
        $this->assertInstanceOf('MongoDB\BSON\ObjectId',$entity->id);
        $this->assertEquals(array('a'=>'a1','id'=>$entity->id),get_object_vars($entity));
        $this->assertEquals($entity,$entity2);
        $id = $entity->id;

        $entity = new \stdClass();
        $entity->id = $id;
        $entity->field = 'boo';
        $entity2 = $repository->save($entity);
        $this->assertEquals(array('id'=>$id,'field'=>'boo'),get_object_vars($entity));
        $this->assertEquals($entity,$entity2);

        $results = $repository->findAll();
        $count=0;
        foreach ($results as $entity) {
            $r = new \stdClass();
            $r->id = $id;
            $r->field = 'boo';
            $this->assertEquals($r,$entity);
            $count++;
        }
        $this->assertEquals(1,$count);
    }

    public function testCustomizeForClassMapping()
    {
        $repository = $this->getRepository('testcol',__NAMESPACE__.'\TestMongodbRepository');

        $entity = new \stdClass();
        $entity->id = null;
        $entity->a = 'a1';
        $entity2 = $repository->save($entity);
        $this->assertInstanceOf('MongoDB\BSON\ObjectId',$entity->id);
        $this->assertEquals(array('a'=>'a1','id'=>$entity->id),get_object_vars($entity));
        $this->assertEquals($entity,$entity2);
        $id = $entity->id;

        $entity = new \stdClass();
        $entity->id = $id;
        $entity->field = 'boo';
        $entity2 = $repository->save($entity);
        $this->assertEquals(array('id'=>$id,'field'=>'boo'),get_object_vars($entity));
        $this->assertEquals($entity,$entity2);

        $results = $repository->findAll();
        $count=0;
        foreach ($results as $entity) {
            $r = new \stdClass();
            $r->id = $id;
            $r->field = 'boo';
            $this->assertEquals($r,$entity);
            $count++;
        }
        $this->assertEquals(1,$count);
    }
}
