<?php
namespace RindowTest\Module\Mongodb\Orm\AbstractMapperTest;

use PHPUnit\Framework\TestCase;
use Rindow\Stdlib\Entity\AbstractEntity;
use Rindow\Stdlib\Entity\PropertyAccessPolicy;
use Rindow\Container\ModuleManager;
use Rindow\Persistence\OrmShell\EntityManager;
use Rindow\Persistence\OrmShell\DataMapper;
use Rindow\Module\Mongodb\Orm\AbstractMapper;

class Color implements PropertyAccessPolicy
{
    public $id;

    public $product;

    public $color;
}

class Category
{
    public $id;

    public $name;

    public function setId($id)
    {
        $this->id = $id;
    }

    public function getId()
    {
        return $this->id;
    }

    public function setName($name)
    {
        $this->name = $name;
    }

    public function getName()
    {
        return $this->name;
    }
}

class Product extends AbstractEntity
{
    static public $colorNames = array(1=>"Red",2=>"Green",3=>"Blue");

    public function getColorNames()
    {
        return self::$colorNames;
    }

    public $id;

    public $category;

    public $name;

    public $colors;

    public function addColor($colorId)
    {
        $color = new Color();
        $color->color = $colorId;
        $color->product = $this;
        $this->colors[] = $color;
    }
}

class CategoryMapper extends AbstractMapper implements DataMapper
{
    //const CLASS_NAME = 'RindowTest\Module\Mongodb\Orm\AbstractMapperTest\Category';
    const TABLE_NAME = 'category';
    const PRIMARYKEY = 'id';

    protected function namedQueryFactories()
    {}

    public function className()
    {
        return __NAMESPACE__.'\Category';
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
        return array('id','name');
    }

    public function hash($entityManager,$entity)
    {
        $hash = md5(strval($this->getId($entity))) .
            md5(strval($this->getField($entity,'name')));
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

class ColorMapper extends AbstractMapper implements DataMapper
{
    //const CLASS_NAME = 'RindowTest\Module\Mongodb\Orm\AbstractMapperTest\Color';
    const TABLE_NAME = 'color';
    const PRIMARYKEY = 'id';

    const CLASS_PRODUCT = 'RindowTest\Module\Mongodb\Orm\AbstractMapperTest\Product';

    protected $productRepository;
    protected $mappingClasses = array(
        'product' => self::CLASS_PRODUCT,
    );

    protected function namedQueryFactories()
    {}

    public function getProductRepository($entityManager)
    {
        //if($this->productRepository)
        //    return $this->productRepository;
        return $entityManager->getRepository(self::CLASS_PRODUCT);
    }

    public function className()
    {
        return __NAMESPACE__.'\Color';
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
        return array('id','product','color');
    }

    public function hash($entityManager,$entity)
    {
        $hash = md5(strval($this->getId($entity))) .
            md5(strval($this->getField($entity,'product')->id)) .
            md5(strval($this->getField($entity,'color')));
        return md5($hash);
    }

    public function supplementEntity($entityManager,$entity)
    {
        $entity->product = $this->getProductRepository($entityManager)->find($entity->product);
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
        return array( 'product'=>$entity->product->id, 'color'=>$entity->color );
    }

    protected function bulidUpdateDocument($entity)
    {
        return array( 'product'=>$entity->product->id, 'color'=>$entity->color,'_id'=>$entity->id );
    }

    protected function buildEntity($document,$entity=null)
    {
        return $this->hydrateEntity($document,$entity);
    }
}

class ProductMapper extends AbstractMapper implements DataMapper
{
    //const CLASS_NAME = 'RindowTest\Module\Mongodb\Orm\AbstractMapperTest\Product';
    const TABLE_NAME = 'product';
    const PRIMARYKEY = 'id';

    const CLASS_CATEGORY = 'RindowTest\Module\Mongodb\Orm\AbstractMapperTest\Category';
    const CLASS_COLOR    = 'RindowTest\Module\Mongodb\Orm\AbstractMapperTest\Color';

    protected $categoryRepository;
    protected $colorRepository;
    protected $mappingClasses = array(
        'category' => self::CLASS_CATEGORY,
    );
    protected $namedQuerys = array();

    protected function namedQueryFactories()
    {
        if($this->namedQuerys)
            return $this->namedQuerys;
        $this->namedQuerys = array(
            'product.by.category' => array($this,'_buildToFindByCategory'),
        );
        return $this->namedQuerys;
    }

    protected function getCategoryRepository($entityManager)
    {
        //if($this->categoryRepository)
        //    return $this->categoryRepository;
        return $entityManager->getRepository(self::CLASS_CATEGORY);
    }

    protected function getColorRepository($entityManager)
    {
        //if($this->colorRepository)
        //    return $this->colorRepository;
        return $entityManager->getRepository(self::CLASS_COLOR);
    }

    public function className()
    {
        return __NAMESPACE__.'\Product';
    }

    public function tableName()
    {
        return self::TABLE_NAME;
    }
    public function fieldNames()
    {
        return array('id','category','name');
    }
    public function primaryKey()
    {
        return self::PRIMARYKEY;
    }

    public function hash($entityManager,$entity)
    {
        $categoryMapper = $this->getCategoryRepository($entityManager)->getMapper();
        $hash = md5(strval($this->getId($entity))) .
            md5(strval($categoryMapper->getId($this->getField($entity,'category')))) .
            md5(strval($this->getField($entity,'name')));
        return md5($hash);
    }

    public function supplementEntity($entityManager,$entity)
    {
        $entity->category = $this->getCategoryRepository($entityManager)->find($entity->category);
        $entity->colors = $this->getColorRepository($entityManager)->findBy(array('product'=>$entity->id));
        $entity->colors->setCascade(array('persist','remove'));
        return $entity;
    }

    public function subsidiaryPersist($entityManager,$entity)
    {
        if($entity->colors===null)
            return;
        $colorRepository = $this->getColorRepository($entityManager);
        foreach ($entity->colors as $color) {
            $colorRepository->persist($color);
        }
    }

    public function subsidiaryRemove($entityManager,$entity)
    {
        if($entity->colors===null)
            return;
        $colorRepository = $this->getColorRepository($entityManager);
        foreach ($entity->colors as $color) {
            $colorRepository->remove($color);
        }
    }

    protected function bulidInsertDocument($entity)
    {
        return array('category'=>$entity->category->id,'name'=>$entity->name);
    }
    protected function bulidUpdateDocument($entity)
    {
        return array('_id'=>$entity->id,'category'=>$entity->category->id,'name'=>$entity->name);
    }
    protected function buildEntity($document,$entity=null)
    {
        return $this->hydrateEntity($document,$entity);
    }

    public function _buildToFindByCategory($params,$options)
    {
        if(!isset($params['category']))
            throw new Exception\DomainException('category parameter is not specifed.');
        $filter = array('category'=>$params['category']);
        $command = null;
        $resultFormatter = null;
        $mapped = true;
        return array($filter,$options,$command,$resultFormatter,$mapped);
    }
}

class Test extends TestCase
{
    const TEST_DB = 'test';
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
            $client->executeCommand(self::TEST_DB,$cmd);
        } catch(\Exception $e) {
            ;
        }
        $cmd = new \MongoDB\Driver\Command(array('drop'=>'category'));
        try {
            $client->executeCommand(self::TEST_DB,$cmd);
        } catch(\Exception $e) {
            ;
        }
        $cmd = new \MongoDB\Driver\Command(array('drop'=>'product'));
        try {
            $client->executeCommand(self::TEST_DB,$cmd);
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
        $cursor = $mongo->executeQuery(self::TEST_DB.'.'.$collection,$query);
        $cursor->setTypeMap(array('root'=>'array','document'=>'array','array'=>'array'));
        return $cursor;
    }

    public function insert($manager,$collection,$doc)
    {
        $bulkWrite = new \MongoDB\Driver\BulkWrite();
        $id = $bulkWrite->insert($doc);
        $manager->executeBulkWrite(self::TEST_DB.'.'.$collection,$bulkWrite);
        return $id;
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
                ),
                'components' => array(
                    __NAMESPACE__.'\ProductMapper'=>array(
                        'parent' => 'Rindow\\Module\\Mongodb\\Orm\\DefaultAbstractMapper',
                        'properties' => array(
                            'hydrator'=>array('ref'=>__NAMESPACE__.'\EntityHydrator'),
                        ),
                    ),
                    __NAMESPACE__.'\CategoryMapper'=>array(
                        'parent' => 'Rindow\\Module\\Mongodb\\Orm\\DefaultAbstractMapper',
                        'properties' => array(
                            'hydrator'=>array('ref'=>__NAMESPACE__.'\SetterHydrator'),
                        ),
                    ),
                    __NAMESPACE__.'\ColorMapper'=>array(
                        'parent' => 'Rindow\\Module\\Mongodb\\Orm\\DefaultAbstractMapper',
                        'properties' => array(
                            'hydrator'=>array('ref'=>__NAMESPACE__.'\PropertyHydrator'),
                        ),
                    ),
                    __NAMESPACE__.'\EntityHydrator'=>array(
                        'class' => 'Rindow\Stdlib\Entity\EntityHydrator',
                    ),
                    __NAMESPACE__.'\SetterHydrator'=>array(
                        'class' => 'Rindow\Stdlib\Entity\SetterHydrator',
                    ),
                    __NAMESPACE__.'\PropertyHydrator'=>array(
                        'class' => 'Rindow\Stdlib\Entity\PropertyHydrator',
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
                    // O/R Mapping for PDO
                    __NAMESPACE__.'\Product'  => __NAMESPACE__.'\ProductMapper',
                    __NAMESPACE__.'\Category' => __NAMESPACE__.'\CategoryMapper',
                    __NAMESPACE__.'\Color'    => __NAMESPACE__.'\ColorMapper',
        
                    // O/D Mapping for MongoDB
                    //'Acme\MyApp\Entity\Product'  => 'Acme\MyApp\Persistence\ODM\ProductMapper',
                    //'Acme\MyApp\Entity\Category' => 'Acme\MyApp\Persistence\ODM\CategoryMapper',
                ),
            ),
        );
        return $config;
    }


    public function getLocalTransactionConfig()
    {
        $config = array(
            'module_manager' => array(
                'modules' => array(
                    'Rindow\Persistence\OrmShell\Module' => true,
                    'Rindow\Module\Mongodb\LocalTxModule' => true,
                ),
                'enableCache'=>false,
            ),
            'container' => array(
                'aliases' => array(
                    'EntityManager' => 'Rindow\\Persistence\\OrmShell\\Transaction\\DefaultPersistenceContext',
                    'TransactionManager' => 'Rindow\\Module\\Mongodb\\Transaction\\DefaultTransactionManager',
                ),
                'components' => array(
                    __NAMESPACE__.'\ProductMapper'=>array(
                        'parent' => 'Rindow\\Module\\Mongodb\\Orm\\DefaultAbstractMapper',
                        'properties' => array(
                            'hydrator'=>array('ref'=>__NAMESPACE__.'\EntityHydrator'),
                        ),
                    ),
                    __NAMESPACE__.'\CategoryMapper'=>array(
                        'parent' => 'Rindow\\Module\\Mongodb\\Orm\\DefaultAbstractMapper',
                        'properties' => array(
                            'hydrator'=>array('ref'=>__NAMESPACE__.'\SetterHydrator'),
                        ),
                    ),
                    __NAMESPACE__.'\ColorMapper'=>array(
                        'parent' => 'Rindow\\Module\\Mongodb\\Orm\\DefaultAbstractMapper',
                        'properties' => array(
                            'hydrator'=>array('ref'=>__NAMESPACE__.'\PropertyHydrator'),
                        ),
                    ),
                    __NAMESPACE__.'\EntityHydrator'=>array(
                        'class' => 'Rindow\Stdlib\Entity\EntityHydrator',
                    ),
                    __NAMESPACE__.'\SetterHydrator'=>array(
                        'class' => 'Rindow\Stdlib\Entity\SetterHydrator',
                    ),
                    __NAMESPACE__.'\PropertyHydrator'=>array(
                        'class' => 'Rindow\Stdlib\Entity\PropertyHydrator',
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
                    // O/R Mapping for PDO
                    __NAMESPACE__.'\Product'  => __NAMESPACE__.'\ProductMapper',
                    __NAMESPACE__.'\Category' => __NAMESPACE__.'\CategoryMapper',
                    __NAMESPACE__.'\Color'    => __NAMESPACE__.'\ColorMapper',
        
                    // O/D Mapping for MongoDB
                    //'Acme\MyApp\Entity\Product'  => 'Acme\MyApp\Persistence\ODM\ProductMapper',
                    //'Acme\MyApp\Entity\Category' => 'Acme\MyApp\Persistence\ODM\CategoryMapper',
                ),
            ),
        );
        return $config;
    }

    public function testSimpleCreateReadUpdateDelete()
    {
        /* Create */
        $mm = new ModuleManager($this->getConfig());
        $em = $mm->getServiceLocator()->get('EntityManager');

        $category = new Category();
        $category->name = 'test';
        $em->persist($category);
        $em->flush();
        $id = $category->id;
        $this->assertInstanceOf('MongoDB\BSON\ObjectID',$id);
        $em->close();

        $cursor = $this->getData('category');
        $count = 0;
        foreach($cursor as $row) {
            $this->assertEquals(array('_id'=>$id,'name'=>'test'),$row);
            $count++;
        }
        $this->assertEquals(1,$count);
        unset($cursor);
        /* Read */
        $mm = new ModuleManager($this->getConfig());
        $em = $mm->getServiceLocator()->get('EntityManager');
        $category = $em->find(__NAMESPACE__.'\Category', $id);
        $id = $category->id;
        $this->assertInstanceOf('MongoDB\BSON\ObjectID',$id);
        $this->assertEquals($id,$category->id);
        $this->assertEquals('test',$category->name);

        $cursor = $this->getData('category');
        $count = 0;
        foreach($cursor as $row) {
            $this->assertEquals(array('_id'=>$id,'name'=>'test'),$row);
            $count++;
        }
        $this->assertEquals(1,$count);
        unset($cursor);

        /* Update */
        $category->name ='updated';
        $em->flush();
        $id = $category->id;
        $this->assertInstanceOf('MongoDB\BSON\ObjectID',$id);

        $cursor = $this->getData('category');
        $count = 0;
        foreach($cursor as $row) {
            $this->assertEquals(array('_id'=>$id,'name'=>'updated'),$row);
            $count++;
        }
        $this->assertEquals(1,$count);
        unset($cursor);

        /* Delete */
        $em->remove($category);
        $em->flush();

        $cursor = $this->getData('category');
        $count = 0;
        foreach($cursor as $row) {
            $count++;
        }
        $this->assertEquals(0,$count);
        unset($cursor);
        $em->close();
    }


    public function testRefresh()
    {
        /* Create */
        $mm = new ModuleManager($this->getConfig());
        $em = $mm->getServiceLocator()->get('EntityManager');

        $category = new Category();
        $category->name = 'test';
        $em->persist($category);
        $em->flush();
        $em->close();
        $id = $category->id;
        $this->assertInstanceOf('MongoDB\BSON\ObjectID',$id);

        $cursor = $this->getData('category');
        $count = 0;
        foreach($cursor as $row) {
            $this->assertEquals(array('_id'=>$id,'name'=>'test'),$row);
            $count++;
        }
        $this->assertEquals(1,$count);
        unset($cursor);

        /* Read */
        $mm = new ModuleManager($this->getConfig());
        $em = $mm->getServiceLocator()->get('EntityManager');
        $category = $em->find(__NAMESPACE__.'\Category', $id);
        $this->assertEquals($id,$category->id);
        $this->assertEquals('test',$category->name);
        $category->name = 'update';
        $this->assertEquals('update',$category->name);
        $em->refresh($category);
        $this->assertEquals('test',$category->name);
    }

    public function testSubsidiaryPersistOnCreate1()
    {
        $mm = new ModuleManager($this->getConfig());
        $em = $mm->getServiceLocator()->get('EntityManager');

        $category = new Category();
        $category->name = 'cat1';
        $em->persist($category);
        $em->flush();

        $product = new Product();
        $product->name = 'prod1';
        $product->category = $category;
        $product->addColor(2);
        $product->addColor(3);

        $em->persist($product);
        $em->flush();
        $id = $product->id;

        $cursor = $this->getData('product');
        $count = 0;
        foreach($cursor as $row) {
            $this->assertEquals(array('_id'=>$id,'category'=>$category->id,'name'=>'prod1'),$row);
            $count++;
        }
        $this->assertEquals(1,$count);
        $cursor = $this->getData('category');
        $count = 0;
        foreach($cursor as $row) {
            $count++;
        }
        $this->assertEquals(1,$count);
        $cursor = $this->getData('color');
        $count = 0;
        foreach($cursor as $row) {
            if($count==0) {
                $this->assertInstanceOf('MongoDB\\BSON\\ObjectID',$row['_id']);
                $this->assertEquals($id,$row['product']);
                $this->assertEquals(2,$row['color']);
            }
            else {
                $this->assertInstanceOf('MongoDB\\BSON\\ObjectID',$row['_id']);
                $this->assertEquals($id,$row['product']);
                $this->assertEquals(3,$row['color']);
            }
            $count++;
        }
        $this->assertEquals(2,$count);

        $em->close();
    }

    public function testSupplementEntity1()
    {
        $manager = $this->getMongoClient();
        $bulkWrite = new \MongoDB\Driver\BulkWrite();
        $catId1 = $bulkWrite->insert(array('name'=>'cat1'));
        $manager->executeBulkWrite('test.category',$bulkWrite);
        $bulkWrite = new \MongoDB\Driver\BulkWrite();
        $prodId1 = $bulkWrite->insert(array('category'=>$catId1,'name'=>'prod1'));
        $manager->executeBulkWrite('test.product',$bulkWrite);
        $bulkWrite = new \MongoDB\Driver\BulkWrite();
        $colorId1 = $bulkWrite->insert(array('product'=>$prodId1,'color'=>2));
        $colorId2 = $bulkWrite->insert(array('product'=>$prodId1,'color'=>3));
        $manager->executeBulkWrite('test.color',$bulkWrite);
        unset($manager);

        $mm = new ModuleManager($this->getConfig());
        $em = $mm->getServiceLocator()->get('EntityManager');
        $product = $em->find(__NAMESPACE__.'\Product', $prodId1);
        $this->assertEquals($prodId1,$product->id);
        $this->assertEquals('prod1',$product->name);
        $this->assertEquals('cat1',$product->category->name);
        $colors = array();
        foreach ($product->colors as $color) {
            $colors[] = $color;
        }
        $this->assertCount(2,$colors);
        $this->assertEquals($colorId1,$colors[0]->id);
        $this->assertEquals(2,$colors[0]->color);
        $this->assertEquals($prodId1,$colors[0]->product->id);
        $this->assertEquals($colorId2,$colors[1]->id);
        $this->assertEquals(3,$colors[1]->color);
        $this->assertEquals($prodId1,$colors[1]->product->id);

        $em->close();
    }

    public function testSubsidiaryRemove1()
    {
        $manager = $this->getMongoClient();
        $bulkWrite = new \MongoDB\Driver\BulkWrite();
        $catId1 = $bulkWrite->insert(array('name'=>'cat1'));
        $manager->executeBulkWrite('test.category',$bulkWrite);
        $bulkWrite = new \MongoDB\Driver\BulkWrite();
        $prodId1 = $bulkWrite->insert(array('category'=>$catId1,'name'=>'prod1'));
        $manager->executeBulkWrite('test.product',$bulkWrite);
        $bulkWrite = new \MongoDB\Driver\BulkWrite();
        $colorId1 = $bulkWrite->insert(array('product'=>$prodId1,'color'=>2));
        $colorId2 = $bulkWrite->insert(array('product'=>$prodId1,'color'=>3));
        $manager->executeBulkWrite('test.color',$bulkWrite);
        unset($manager);

        $mm = new ModuleManager($this->getConfig());
        $em = $mm->getServiceLocator()->get('EntityManager');
        $product = $em->find(__NAMESPACE__.'\Product', $prodId1);
        $this->assertEquals($prodId1,$product->id);
        $this->assertEquals('prod1',$product->name);
        $this->assertEquals('cat1',$product->category->name);

        $colors = array();
        foreach ($product->colors as $color) {
            $colors[] = $color;
        }
        $this->assertCount(2,$colors);
        $this->assertEquals($colorId1,$colors[0]->id);
        $this->assertEquals(2,$colors[0]->color);
        $this->assertEquals($prodId1,$colors[0]->product->id);
        $this->assertEquals($colorId2,$colors[1]->id);
        $this->assertEquals(3,$colors[1]->color);
        $this->assertEquals($prodId1,$colors[1]->product->id);

        $em->remove($product);
        return;
        $em->flush();

        $cursor = $this->getData('category');
        $count = 0;
        foreach($cursor as $row) {
            $this->assertEquals(array('id'=>$catId1,'name'=>'cat1'),$row);
            $count++;
        }
        $this->assertEquals(1,$count);
        $cursor = $this->getData('color');
        $count = 0;
        foreach($cursor as $row) {
            $count++;
        }
        $this->assertEquals(0,$count);
        $cursor = $this->getData('product');
        $count = 0;
        foreach($cursor as $row) {
            $count++;
        }
        $this->assertEquals(0,$count);

        unset($cursor);

        $em->close();
    }

    public function testSubsidiaryUpdateAndCascadeRemove1()
    {
        $manager = $this->getMongoClient();
        $bulkWrite = new \MongoDB\Driver\BulkWrite();
        $catId1 = $bulkWrite->insert(array('name'=>'cat1'));
        $manager->executeBulkWrite('test.category',$bulkWrite);
        $bulkWrite = new \MongoDB\Driver\BulkWrite();
        $prodId1 = $bulkWrite->insert(array('category'=>$catId1,'name'=>'prod1'));
        $manager->executeBulkWrite('test.product',$bulkWrite);
        $bulkWrite = new \MongoDB\Driver\BulkWrite();
        $colorId1 = $bulkWrite->insert(array('product'=>$prodId1,'color'=>2));
        $colorId2 = $bulkWrite->insert(array('product'=>$prodId1,'color'=>3));
        $manager->executeBulkWrite('test.color',$bulkWrite);
        unset($manager);

        $mm = new ModuleManager($this->getConfig());
        $em = $mm->getServiceLocator()->get('EntityManager');
        $product = $em->find(__NAMESPACE__.'\Product', $prodId1);
        $this->assertEquals($prodId1,$product->id);
        $this->assertEquals('prod1',$product->name);
        $this->assertEquals('cat1',$product->category->name);
        $colors = array();
        foreach ($product->colors as $color) {
            $colors[] = $color;
        }
        $this->assertCount(2,$colors);
        $this->assertEquals($colorId1,$colors[0]->id);
        $this->assertEquals(2,$colors[0]->color);
        $this->assertEquals($prodId1,$colors[0]->product->id);
        $this->assertEquals($colorId2,$colors[1]->id);
        $this->assertEquals(3,$colors[1]->color);
        $this->assertEquals($prodId1,$colors[1]->product->id);

        // ======= Update category name and Remove ColorId =========
        $product->category->name = 'Updated';
        unset($product->colors[1]);
        $em->flush();
        // =========================================================

        $cursor = $this->getData('category');
        $count = 0;
        foreach($cursor as $row) {
            $this->assertEquals(array('_id'=>$catId1,'name'=>'Updated'),$row);
            $count++;
        }
        $this->assertEquals(1,$count);
        $cursor = $this->getData('color');
        $count = 0;
        foreach($cursor as $row) {
            $this->assertEquals(array('_id'=>$colorId1,'product'=>$prodId1,'color'=>2),$row);
            $count++;
        }
        $this->assertEquals(1,$count);
        unset($cursor);

        $em->close();
    }

    public function testSubsidiaryUpdateAndCascadePersist1()
    {
        $manager = $this->getMongoClient();
        $bulkWrite = new \MongoDB\Driver\BulkWrite();
        $catId1 = $bulkWrite->insert(array('name'=>'cat1'));
        $catId2 = $bulkWrite->insert(array('name'=>'cat2'));
        $manager->executeBulkWrite('test.category',$bulkWrite);
        $bulkWrite = new \MongoDB\Driver\BulkWrite();
        $prodId1 = $bulkWrite->insert(array('category'=>$catId1,'name'=>'prod1'));
        $manager->executeBulkWrite('test.product',$bulkWrite);
        $bulkWrite = new \MongoDB\Driver\BulkWrite();
        $colorId1 = $bulkWrite->insert(array('product'=>$prodId1,'color'=>2));
        $colorId2 = $bulkWrite->insert(array('product'=>$prodId1,'color'=>3));
        $manager->executeBulkWrite('test.color',$bulkWrite);
        unset($manager);

        $mm = new ModuleManager($this->getConfig());
        $em = $mm->getServiceLocator()->get('EntityManager');
        $product = $em->find(__NAMESPACE__.'\Product', $prodId1);
        $this->assertEquals($prodId1,$product->id);
        $this->assertEquals('prod1',$product->name);
        $this->assertEquals('cat1',$product->category->name);
        $colors = array();
        foreach ($product->colors as $color) {
            $colors[] = $color;
        }
        $this->assertCount(2,$colors);
        $this->assertEquals($colorId1,$colors[0]->id);
        $this->assertEquals(2,$colors[0]->color);
        $this->assertEquals($prodId1,$colors[0]->product->id);
        $this->assertEquals($colorId2,$colors[1]->id);
        $this->assertEquals(3,$colors[1]->color);
        $this->assertEquals($prodId1,$colors[1]->product->id);

        // ============== Change category and Add ColorId ==========
        $category2 = $em->find(__NAMESPACE__.'\Category', $catId2);
        $product->category = $category2;
        $product->addColor(4);
        $em->flush();
        $colorId3 = $product->colors[2]->id;
        // =========================================================

        $cursor = $this->getData('product');
        $count = 0;
        foreach($cursor as $row) {
            $this->assertEquals(array('_id'=>$prodId1,'category'=>$catId2,'name'=>'prod1'),$row);
            $count++;
        }
        $this->assertEquals(1,$count);
        $cursor = $this->getData('color');
        $count = 0;
        foreach($cursor as $row) {
            if($row['color']==2)
                $this->assertEquals(array('_id'=>$colorId1,'product'=>$prodId1,'color'=>2),$row);
            elseif($row['color']==3)
                $this->assertEquals(array('_id'=>$colorId2,'product'=>$prodId1,'color'=>3),$row);
            else
                $this->assertEquals(array('_id'=>$colorId3,'product'=>$prodId1,'color'=>4),$row);
            $count++;
        }
        $this->assertEquals(3,$count);
        unset($cursor);

        $em->close();
    }


    public function testLocalTransactionalSimpleCreateReadUpdateDelete()
    {
        /* Create */
        $mm = new ModuleManager($this->getLocalTransactionConfig());
        $em = $mm->getServiceLocator()->get('EntityManager');
        $tx = $mm->getServiceLocator()->get('TransactionManager');

        $tx->begin();
        $category = new Category();
        $category->name = 'test';
        $em->persist($category);
        $tx->commit();
        $catId1 = $category->id;

        $cursor = $this->getData('category');
        $count = 0;
        foreach($cursor as $row) {
            $this->assertEquals(array('_id'=>$catId1,'name'=>'test'),$row);
            $count++;
        }
        $this->assertEquals(1,$count);
        unset($cursor);

        /* Read */
        $mm = new ModuleManager($this->getLocalTransactionConfig());
        $em = $mm->getServiceLocator()->get('EntityManager');
        $tx = $mm->getServiceLocator()->get('TransactionManager');
        $tx->begin();
        $category = $em->find(__NAMESPACE__.'\Category', $catId1);
        $this->assertEquals($catId1,$category->id);
        $this->assertEquals('test',$category->name);

        $cursor = $this->getData('category');
        $count = 0;
        foreach($cursor as $row) {
            $this->assertEquals(array('_id'=>$catId1,'name'=>'test'),$row);
            $count++;
        }
        $this->assertEquals(1,$count);
        unset($cursor);

        /* Update */
        $category->name ='updated';
        $tx->commit();

        $cursor = $this->getData('category');
        $count = 0;
        foreach($cursor as $row) {
            $this->assertEquals(array('_id'=>$catId1,'name'=>'updated'),$row);
            $count++;
        }
        $this->assertEquals(1,$count);
        unset($cursor);

        /* Delete */
        $mm = new ModuleManager($this->getLocalTransactionConfig());
        $em = $mm->getServiceLocator()->get('EntityManager');
        $tx = $mm->getServiceLocator()->get('TransactionManager');
        $tx->begin();
        $category = $em->find(__NAMESPACE__.'\Category', $catId1);
        $this->assertEquals($catId1,$category->id);
        $this->assertEquals('updated',$category->name);

        $em->remove($category);
        $tx->commit();

        $cursor = $this->getData('category');
        $count = 0;
        foreach($cursor as $row) {
            $count++;
        }
        $this->assertEquals(0,$count);
        unset($cursor);
    }

    public function testNamedQuery()
    {
        $manager = $this->getMongoClient();
        $catId1   = $this->insert($manager,'category',array('name'=>'cat1'));
        $prodId1  = $this->insert($manager,'product',array('category'=>$catId1,'name'=>'prod1'));
        $colorId1 = $this->insert($manager,'color',array('product'=>$prodId1,'color'=>2));
        $colorId2 = $this->insert($manager,'color',array('product'=>$prodId1,'color'=>3));
        $prodId2  = $this->insert($manager,'product',array('category'=>$catId1,'name'=>'prod2'));
        $colorId3 = $this->insert($manager,'color',array('product'=>$prodId2,'color'=>1));
        $colorId4 = $this->insert($manager,'color',array('product'=>$prodId2,'color'=>2));
        unset($manager);

        $mm = new ModuleManager($this->getConfig());
        $em = $mm->getServiceLocator()->get('EntityManager');
        $dummy = new Product();
        $productClassName = get_class($dummy);
        $query = $em->createNamedQuery("product.by.category");
        //$this->assertEquals('SELECT * FROM product WHERE category = :category',$query->getPreparedCriteria()->getSql());
        $query->setParameter('category',$catId1);
        $results = $query->getResultList();
        $productCount = 0;
        foreach ($results as $product) {
            $this->assertInstanceOf(__NAMESPACE__.'\Product',  $product);
            $this->assertInstanceOf(__NAMESPACE__.'\Category', $product->category);
            $colorCount = 0;
            foreach ($product->colors as $color) {
                $this->assertInstanceOf(__NAMESPACE__.'\Color',$color);
                $colorCount++;
            }
            $this->assertEquals(2,$colorCount);
            $productCount++;
        }
        $this->assertEquals(2,$productCount);
        $em->close();
    }
}
