<?php
namespace Rindow\Module\Mongodb\Orm;

use Rindow\Stdlib\Entity\PropertyAccessPolicy;
use Rindow\Database\Pdo\Paginator\SqlAdapter;
use Rindow\Database\Dao\Exception;
use Rindow\Database\Dao\Support\ArrayCursor;
use Rindow\Module\Mongodb\Cursor;
use MongoDB\BSON\ObjectId;

use Rindow\Persistence\OrmShell\DataMapper;

abstract class AbstractMapper implements DataMapper
{
    abstract public function className();
    abstract public function supplementEntity($entityManager,$entity);
    abstract public function subsidiaryPersist($entityManager,$entity);
    abstract public function subsidiaryRemove($entityManager,$entity);
    abstract protected function bulidInsertDocument($entity);
    abstract protected function bulidUpdateDocument($entity);
    abstract protected function buildEntity($document,$entity=null);
    abstract protected function namedQueryFactories();

    abstract public function tableName();
    abstract public function primaryKey();
    abstract public function fieldNames();
    
    protected $dataSource;
    protected $entityManager;
    protected $hydrator;

    //public function setResource($resource)
    //{
    //    $this->resource = $resource;
    //}

    public function setDataSource($dataSource)
    {
        $this->dataSource = $dataSource;
    }

    public function getConnection()
    {
        if($this->dataSource==null)
            throw new Exception\DomainException('DataSource is not specifed.');
        return $this->dataSource->getConnection();
    }

    //public function setEntityManager($entityManager)
    //{
    //    $this->entityManager = $entityManager;
    //}

    public function setHydrator($hydrator)
    {
        $this->hydrator = $hydrator;
    }

    public function getHydrator()
    {
        return $this->hydrator;
    }

    public function primaryDocumentKey()
    {
        return '_id';
    }

    public function idToString($id)
    {
        return strval($id);
    }

    public function stringToId($string)
    {
        return new ObjectId(strval($string));
    }

    public function getMappedEntityClass($field)
    {
        if(!array_key_exists($field, $this->mappingClasses))
            return null;
        return $this->mappingClasses[$field];
    }

    protected function setField($entity,$name,$value)
    {
        if($entity instanceof PropertyAccessPolicy) {
            $entity->$name = $value;
        } else {
            $setter = 'set'.ucfirst($name);
            $entity->$setter($value);
        }
        return $entity;
    }

    protected function getField($entity,$name)
    {
        if(!is_object($entity)) {
            throw new Exception\DomainException('entity is not object.:'.gettype($entity));
        }

        if($entity instanceof PropertyAccessPolicy) {
            return $entity->$name;
        } else {
            $getter = 'get'.ucfirst($name);
            return $entity->$getter();
        }
    }

    protected function extractEntity($entity)
    {
        if(!$entity)
            return $entity;
        $document = $this->getHydrator()->extract($entity,$this->fieldNames());
        if(isset($document[$this->primaryKey()])) {
            $id = $document[$this->primaryKey()];
            unset($document[$this->primaryKey()]);
            if(is_scalar($id))
                $id = $this->stringToId($id);
            $document[$this->primaryDocumentKey()] = $id;
        } else {
            unset($document[$this->primaryKey()]);
        }
        return $document;
    }

    protected function hydrateEntity($document,$entity=null)
    {
        if(!$document)
            return $document;
        if($entity==null || is_string($entity)) {
            if($entity==null)
                $className = $this->className();
            if(!class_exists($className))
                throw new Exception\DomainException('entity class is not exists: '.$className);
            $entity = new $className();
        } elseif(!is_object($entity)) {
            throw new Exception\DomainException('Entity must be a class-name-string or a object-instance: '.gettype($entity));
        }
        if(isset($document[$this->primaryDocumentKey()])) {
            $id = $document[$this->primaryDocumentKey()];
            unset($document[$this->primaryDocumentKey()]);
            $document[$this->primaryKey()] = $id;
        } else {
            unset($document[$this->primaryDocumentKey()]);
        }
        $this->getHydrator()->hydrate($document,$entity);
        return $entity;
    }

    public function _buildEntity($document)
    {
        if(!$document)
            return $document;
        return $this->buildEntity($document,null);
    }

/*
    protected function bulidQueryStatement($query)
    {
        $tableName = $this->tableName();
        $where = '';
        foreach ($query as $key => $value) {
            if(empty($where))
                $where = ' WHERE '.$key.' = :'.$key;
            else
                $where .= ' AND '.$key.' = :'.$key;
        }
        return "SELECT * FROM ".$tableName." ".$where;
    }

    protected function bulidQueryParameter(array $query)
    {
        $params = array();
        foreach ($query as $key => $value) {
            $params[':'.$key] = $value;
        }
        return $params;
    }

    protected function buildQueryLimit($firstPosition,$maxResult)
    {
        if($firstPosition===null || $maxResult===null) {
            return '';
        }
        $limit = ' LIMIT '.$maxResult;
        if($firstPosition!==null)
            $limit .= ' OFFSET '.$firstPosition;
        return $limit;
    }
*/
    public function getId($entity)
    {
        return $this->getField($entity,$this->primaryKey());
    }

    public function create($entity)
    {
        $document = $this->bulidInsertDocument($entity);
        $primaryDocumentKey = $this->primaryDocumentKey();
        if(!isset($document[$primaryDocumentKey]))
            unset($document[$primaryDocumentKey]);
        $id = $this->getConnection()->insert($this->tableName(),$document);
        if($id)
            $this->setField($entity,$this->primaryKey(),$id);
        return $entity;
    }

    public function save($entity)
    {
        $id = $this->getField($entity,$this->primaryKey());
        if(empty($id))
            throw new \DomainException('a primary Key is empty');
        $document = $this->bulidUpdateDocument($entity);
        $documentKey = $this->primaryDocumentKey();
        $this->getConnection()->update($this->tableName(),array($documentKey=>$id),$document);
    }

    public function remove($entity)
    {
        $primaryKey = $this->primaryKey();
        $id = $this->getField($entity,$primaryKey);
        if(!($id instanceof ObjectId))
            throw new \DomainException('id must be a string or ObjectId');
        $documentKey = $this->primaryDocumentKey();
        $this->getConnection()->delete($this->tableName(),array($documentKey=>$id));
    }

    public function find($id,$entity=null,$lockMode=null,array $properties=null)
    {
        if(is_scalar($id)) {
            $idString = $id;
            $id = $this->stringToId($idString);
        } else {
            if(!($id instanceof ObjectId))
                throw new \DomainException('id must be a string or ObjectId');
            $idString = $this->idToString($id);
        }
        $documentKey = $this->primaryDocumentKey();

        $result = $this->executeQuery(array($documentKey=>$id),$entity);
        $data = $result->fetch();
        if(!$data)
            return $data;
        $data = $this->buildEntity($data,$entity);
        return $data;
    }
/*
    public function findAll($pagination=false)
    {
        $class = $this->className();
        if($pagination) {
            $sqlAdapter = new MongodbAdapter($this->getConnection());
            $sqlAdapter->setQuery($collection);
            return $sqlAdapter;
        } else {
            $result = 
                $this->entityManager->createResultList(
                    $this->executeQuery($sql,$params,$class));
            return $result;
        }
    }
*/
    public function findBy(
        $resultListFactory,
        $query,
        $params=null,
        $firstPosition=null,
        $maxResult=null,
        $lockMode=null)
    {
        $options = array();
        $command = $resultFormatter = null;
        $mapped = true;
        if($firstPosition)
            $options['skip'] = $firstPosition;
        if($maxResult)
            $options['limit'] = $maxResult;

        if(is_array($query)) {
            //
            // mongodb query filter 
            //
            $filter = $query;
        } elseif($query instanceof PreparedCriteria) {
            //
            // prepared query criteria
            //
            if($query->getCriteria()) {
                list($filter,$options,$command,$resultFormatter,$mapped) = $query->buildQuery($params,$options,$this->tableName());
            } else {
                $queryFactory = $query->getQueryFactory();
                $r = call_user_func($queryFactory,$params,$options);
                if(count($r)!=5)
                    throw new Exception\DomainException('factory result must have 5 values from mapper for '.$this->className());
                list($filter,$options,$command,$resultFormatter,$mapped) = $r;
            }
        } elseif(is_string($query)) {
            //
            // Native SQL or DQL or something
            //
            throw new Exception\InvalidArgumentException('SQL is not supported for "'.$this->className().'".');
        } else {
            throw new Exception\InvalidArgumentException('Invalid Type of Query for "'.$this->className().'".');
        }

        if($command==null) {
            $cursorFactory = $this->createQueryExecutor($filter,$options);
        } else {
            $cursorFactory = $this->createCommandExecutor($command,$resultFormatter);
        }

        $resultList = call_user_func($resultListFactory,$cursorFactory);
        if($mapped) {
            $resultList->addFilter(array($this,'_buildEntity'));
            $resultList->setMapped(true);
        } else {
            $resultList->setMapped(false);
        }

        return $resultList;
    }

    public function getNamedQuery($name,$resultClass=null)
    {
        $querys = $this->namedQueryFactories();
        if(!isset($querys[$name]))
            return null;
        $prepared = new PreparedCriteria(null,$querys[$name],$this->className(),$resultClass);
        return $prepared;
    }

    protected function executeUpdate($sql,$params)
    {
        try {
            $result = $this->getConnection()->executeUpdate($sql,$params);
        } catch (\Exception $e) {
            throw $e;
        }
        return $result;
    }

    protected function executeQuery(array $filter,$class=null)
    {
        $connection = $this->getConnection();
        $cursor = $connection->find($this->tableName(),$filter);
        $typeMap = $connection->getTypeMap();
        if($class==null) {
            if($typeMap['root']!='array'||$typeMap['document']!='array'||$typeMap['array']!='array') {
                $cursor->setTypeMap(array('root'=>'array','document'=>'array','array'=>'array'));
            }
        } elseif(is_string($class)) {
            $cursor->setTypeMap(array('root'=>$class,'document'=>'array','array'=>'array'));
        }
        return $cursor;
    }

    protected function createQueryExecutor($filter,$options)
    {
        $connection = $this->getConnection();
        $tableName = $this->tableName();
        return function () use ($connection,$tableName,$filter,$options) {
            $cursor = $connection->find($tableName,$filter,$options);
            $typeMap = $connection->getTypeMap();
            if($typeMap['root']!='array'||$typeMap['document']!='array'||$typeMap['array']!='array') {
                $cursor->setTypeMap(array('root'=>'array','document'=>'array','array'=>'array'));
            }
            return $cursor;
        };
    }

    protected function createCommandExecutor($command,$resultFormatter)
    {
        $connection = $this->getConnection();
        return function () use ($connection,$command,$resultFormatter) {
            $dbCursor = $connection->executeCommand($command);
            $dbCursor->setTypeMap(array('root'=>'array','document'=>'array','array'=>'array'));
            //$cursor = new Cursor($dbCursor,$connection);
            //return $cursor;
            $data = $dbCursor->toArray();
            $data = call_user_func($resultFormatter,$data[0]);
            return new ArrayCursor($data);
        };
    }

    public function createSchema()
    {
        $sql = $this->createSchemaStatement();
        return $this->executeUpdate($sql,array());
    }

    public function dropSchema()
    {
        $sql = $this->dropSchemaStatement();
        return $this->executeUpdate($sql,array());
    }

    public function close()
    {
        // *** CAUTION ***
        // Don't close the connection.
        // Because other entity managers use same connection.
    }
}