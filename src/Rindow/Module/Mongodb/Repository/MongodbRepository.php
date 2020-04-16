<?php
namespace Rindow\Module\Mongodb\Repository;

use Interop\Lenient\Dao\Query\Expression as ExpressionInterface;
use Interop\Lenient\Dao\Repository\CrudRepository;
use Interop\Lenient\Dao\Repository\DataMapper;
use Rindow\Stdlib\Entity\PropertyAccessPolicy;
use Rindow\Database\Dao\Exception;
use Rindow\Database\Dao\Support\ResultList;
use MongoDB\BSON\Serializable as BSONSerializable;
use MongoDB\BSON\Unserializable as BSONUnserializable;
use MongoDB\BSON\ObjectId;

class MongodbRepository implements CrudRepository,DataMapper
{
    static protected $operatorStrings = array(
        ExpressionInterface::EQUAL => '$eq',
        ExpressionInterface::GREATER_THAN => '$gt',
        ExpressionInterface::GREATER_THAN_OR_EQUAL => '$gte',
        ExpressionInterface::LESS_THAN => '$lt',
        ExpressionInterface::LESS_THAN_OR_EQUAL => '$lte',
        ExpressionInterface::NOT_EQUAL => '$ne',
        ExpressionInterface::IN => '$in',
    );
    protected $dataSource;
    protected $collection;
    protected $queryBuilder;
    protected $dataMapper;
    protected $keyName = 'id';
    protected $fetchClass;

    public function __construct($dataSource=null,$collection=null,$queryBuilder=null)
    {
        if($dataSource)
            $this->setDataSource($dataSource);
        if($collection)
            $this->setCollection($collection);
        if($queryBuilder)
            $this->setQueryBuilder($queryBuilder);
    }

    public function setCollection($collection)
    {
        $this->collection = $collection;
    }

    public function setDataSource($dataSource)
    {
        $this->dataSource = $dataSource;
    }

    public function getDataSource()
    {
        return $this->dataSource;
    }

    public function setQueryBuilder($queryBuilder)
    {
        $this->queryBuilder = $queryBuilder;
    }

    public function getQueryBuilder()
    {
        return $this->queryBuilder;
    }

    public function setKeyName($keyName)
    {
        $this->keyName = $keyName;
    }

    public function getKeyName()
    {
        return $this->keyName;
    }

    public function setDataMapper(DataMapper $dataMapper)
    {
        $this->dataMapper = $dataMapper;
    }

    public function getConnection()
    {
        return $this->dataSource->getConnection();
    }

    protected function toObjectId($string)
    {
        if($string===null)
            return null;
        if($string instanceof ObjectId)
            return $string;
        if(is_string($string))
            return new ObjectId($string);
        throw new Exception\DomainException('Invalid type Id');
    }

    protected function makeDocument($entity)
    {
        if($this->dataMapper) {
            $entity = $this->dataMapper->demap($entity);
            if(!is_array($entity) && !($entity instanceof BSONSerializable)) {
                throw new Exception\InvalidArgumentException('mapped document must be array.');
            }
        }
        $entity = $this->demap($entity);
        if(!is_array($entity) && !($entity instanceof BSONSerializable)) {
            throw new Exception\InvalidArgumentException('the entity must be array.');
        }
        if(!($entity instanceof BSONSerializable)) {
            $entity = $this->shiftId($entity);
        }
        return $entity;
    }

    protected function extractId($values)
    {
        if($values instanceof BSONSerializable) {
            if(method_exists($values,'extractId')) {
                return $values->extractId();
            }
            $values = $values->bsonSerialize();
        }
        if(isset($values['_id']))
            return $values['_id'];
        return null;
    }

    public function shiftId(array $values)
    {
        if(isset($values[$this->keyName])) {
            $values['_id'] = $this->toObjectId($values[$this->keyName]);
            if($this->keyName!='_id') {
                unset($values[$this->keyName]);
            }
        }
        return $values;
    }

    public function unshiftId(array $values)
    {
        if($this->keyName!='_id') {
            if(isset($values['_id']))
                $values[$this->keyName] = $values['_id'];
            unset($values['_id']);
        }
        return $values;
    }

    public function assertCollection()
    {
        if($this->collection==null)
            throw new Exception\DomainException('collection is not specified.');
    }

    protected function extractValue($value)
    {
        if($value instanceof Parameter) {
            $value = $value->getValue();
        }
        return $value;
    }

    protected function buildMongoExpression($propertyName,$value)
    {
        if($value instanceof ExpressionInterface) {
            if($value->getPropertyName())
                $propertyName = $value->getPropertyName();
            $operator = $value->getOperator();
            $value = $value->getValue();
        } else {
            $operator = ExpressionInterface::EQUAL;
        }
        $value = $this->extractValue($value);
        if(is_array($value) && $operator!=ExpressionInterface::IN)
            throw new Exception\RuntimeException('Normally expression must not include array value.');
        $params = array();
        switch($operator) {
            case ExpressionInterface::EQUAL:
                $params[$propertyName] = $value;
                break;
            case ExpressionInterface::GREATER_THAN:
            case ExpressionInterface::GREATER_THAN_OR_EQUAL:
            case ExpressionInterface::LESS_THAN:
            case ExpressionInterface::LESS_THAN_OR_EQUAL:
            case ExpressionInterface::NOT_EQUAL:
            case ExpressionInterface::IN:
                $operatorString = self::$operatorStrings[$operator];
                $params[$propertyName] = array($operatorString=>$value);
                break;
            case ExpressionInterface::BEGIN_WITH:
                $params[$propertyName] = array('$regex'=>'^'.$value);
                break;
            default:
                throw new Exception\InvalidArgumentException('Unkown operator code in a filter.: '.$operator);
        }
        return $params;
    }

    protected function buildMongoFilter(array $filter=null)
    {
        if(!$filter)
            return array();
        $params = array();
        foreach ($filter as $key => $value) {
            $params[] = $this->buildMongoExpression($key,$value);
        }
        if(count($params)==1) {
            return $params[0];
        }
        return array('$and'=>$params);
    }

    public function save($entity)
    {
        $this->assertCollection();
        $values = $this->makeDocument($entity);
        if($this->extractId($values)!==null) {
            $this->update($values);
            return $entity;
        } else {
            list($id,$values) = $this->create($values);
            if($this->dataMapper) {
                $entity = $this->dataMapper->fillId($entity,$id);
            } else {
                $entity = $this->fillId($entity,$id);
            }
        }
        return $entity;
    }

    protected function create($values)
    {
        if(!is_array($values) && !($values instanceof BSONSerializable))
            throw new Exception\InvalidArgumentException('mapped document must be array.');
        $id = $this->getConnection()->insert($this->collection,$values);
        return array($id,$values);
    }

    protected function update($values)
    {
        if(!is_array($values) && !($values instanceof BSONSerializable))
            throw new Exception\InvalidArgumentException('mapped document must be array.');
        $options = array('upsert'=>true);
        $id = $this->extractId($values);
        $filter = $this->buildMongoFilter(array('_id'=>$id));
        $connection = $this->getConnection();
        $connection->update($this->collection,$filter,$values,$options);
    }

    public function delete($entity)
    {
        $this->assertCollection();
        $values = $this->makeDocument($entity);
        $id = $this->extractId($values);
        if($id===null)
            throw new Exception\DomainException('the KeyName "'.$this->keyName.'" is not found in entity');
        $this->deleteById($id);
    }

    public function deleteById($id)
    {
        $this->assertCollection();
        $filter = $this->buildMongoFilter(array('_id'=>$id));
        $connection = $this->getConnection();
        $connection->delete($this->collection,$filter);
    }

    public function deleteAll(array $filter=null)
    {
        $this->assertCollection();
        if($filter==null)
            $filter=array();
        $filter = $this->shiftId($filter);
        if(!isset($filter['_id'])) {
            unset($filter['_id']);
        }
        $filter = $this->buildMongoFilter($filter);
        $connection = $this->getConnection();
        $connection->delete($this->collection,$filter);
    }

    public function findById($id)
    {
        $this->assertCollection();
        $id = $this->toObjectId($id);
        $filter = $this->buildMongoFilter(array('_id'=>$id));
        $connection = $this->getConnection();
        $cursor = $connection->find($this->collection,$filter);
        if($this->dataMapper)
            $fetchClass = $this->dataMapper->getFetchClass();
        else
            $fetchClass = $this->getFetchClass();
        if($fetchClass)
            $cursor->setTypeMap(array('root'=>$fetchClass));
        $values = $cursor->fetch();
        if(!$values)
            return null;
        $entity = $this->map($values);
        if($this->dataMapper) {
            $entity = $this->dataMapper->map($entity);
        }
        return $entity;
    }

    public function findAll(array $filter=null,array $sort=null,$limit=null,$offset=null)
    {
        $this->assertCollection();
        if($filter==null)
            $filter=array();
        $filter = $this->shiftId($filter);
        if(!isset($filter['_id'])) {
            unset($filter['_id']);
        }
        $options = array();
        if($sort) {
            $sort2 = array();
            foreach ($sort as $key => $direction) {
                if($key==$this->keyName)
                    $key = '_id';
                $sort2[$key] = $direction;
            }
            $options['sort'] = $sort2;
        }
        if($limit) {
            $options['limit'] = $limit;
        }
        if($offset) {
            $options['skip'] = $offset;
        }
        $filter = $this->buildMongoFilter($filter);
        $connection = $this->getConnection();
        $cursor = $connection->find($this->collection,$filter,$options);
        if($this->dataMapper)
            $fetchClass = $this->dataMapper->getFetchClass();
        else
            $fetchClass = $this->getFetchClass();
        if($fetchClass)
            $cursor->setTypeMap(array('root'=>$fetchClass));
        $resultList = new ResultList(array($cursor,'fetch'),array($cursor,'close'));
        $resultList->addFilter(array($this,'map'));
        if($this->dataMapper)
            $resultList->addFilter(array($this->dataMapper,'map'));
        return $resultList;
    }

    public function findOne(array $filter=null,array $sort=null,$offset=null)
    {
        $limit = 1;
        $results = $this->findAll($filter,$sort,$limit,$offset);
        $entity = null;
        foreach ($results as $result) {
            $entity = $result;
        }
        return $entity;
    }

    public function count(array $filter=null)
    {
        $this->assertCollection();
        if($filter) {
            $filter = $this->shiftId($filter);
            if(!isset($filter['_id'])) {
                unset($filter['_id']);
            }
        }
        $filter = $this->buildMongoFilter($filter);
        $connection = $this->getConnection();
        return $connection->count($this->collection,$filter);
    }

    public function existsById($id)
    {
        $id = $this->toObjectId($id);
        $count = $this->count(array('_id'=>$id));
        return $count ? true : false;
    }

    public function demap($data)
    {
        return $data;
    }

    public function map($entity)
    {
        if($entity instanceof BSONUnserializable)
            return $entity;
        return $this->unshiftId($entity);
    }

    public function fillId($values,$id)
    {
        if($values instanceof BSONSerializable) {
            if(method_exists($values,'fillId')) {
                $values->fillId($id);
            } elseif($values instanceof PropertyAccessPolicy) {
                $keyName = $this->keyName;
                $values->$keyName = $id;
            } else {
                $method = 'set'.ucfirst($this->keyName);
                if(method_exists($values,$method)) {
                    $values->$method($id);
                }
            }
        } else {
            $values[$this->keyName] = $id;
        }
        return $values;
    }

    public function setFetchClass($fetchClass)
    {
        $this->fetchClass = $fetchClass;
    }

    public function getFetchClass()
    {
        return $this->fetchClass;
    }
}
