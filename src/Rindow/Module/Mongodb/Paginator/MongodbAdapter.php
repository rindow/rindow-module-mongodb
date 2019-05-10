<?php
namespace Rindow\Module\Mongodb\Paginator;

use IteratorAggregate;
use MongoCollection;
use Interop\Lenient\Dao\Query\ResultList as ResultListInterface;
use Rindow\Stdlib\Paginator\PaginatorAdapter;
use Rindow\Database\Dao\Support\ResultList;
use Rindow\Module\Mongodb\Core\Connection;
use Rindow\Module\Mongodb\Exception;

class MongodbAdapter implements PaginatorAdapter,IteratorAggregate
{
    protected $connection;
    protected $collection;
    protected $filter;
    protected $options;
    protected $className;
    protected $loader;

    public function __construct(Connection $connection)
    {
        $this->connection = $connection;
    }

    protected function getResultList($cursor)
    {
        if($cursor instanceof ResultListInterface)
            return $cursor;
        return new ResultList($cursor);
    }

    public function setQuery($collection,array $filter=array(),array $options=array(),$className=null)
    {
        $this->collection = $collection;
        $this->filter = $filter;
        $this->options = $options;
        $this->className = $className;
        return $this;
    }

    public function setLoader($callback)
    {
        $this->loader = $callback;
        if(!is_callable($callback))
            throw new Exception\InvalidArgumentException('loader is not callable.');
        return $this;
    }

    public function count()
    {
        $count = $this->connection->count($this->collection,$this->filter);
        return $count;
    }

    public function getItems($offset, $itemMaxPerPage)
    {
        if($this->collection===null)
            throw new Exception\DomainException('collection is not specified.');
        $options = $this->options;
        $options['skip'] = $offset;
        $options['limit']= $itemMaxPerPage;
        $cursor = $this->connection->find($this->collection,$this->filter,$options);
        if($this->className) {
            $typeMap = $this->connection->getTypeMap();
            $typeMap['root'] = $this->className;
            $cursor->setTypeMap($typeMap);
        }

        $result = $this->getResultList(array($cursor,'fetch'));
        if($result && $this->loader)
            $result->addFilter($this->loader);
        return $result;
    }

    public function getIterator()
    {
        if($this->collection===null)
            throw new Exception\DomainException('collection is not specified.');
        $cursor = $this->connection->find($this->collection,$this->filter,$this->options);
        if($this->className) {
            $typeMap = $this->connection->getTypeMap();
            $typeMap['root'] = $this->className;
            $cursor->setTypeMap($typeMap);
        }

        $result = $this->getResultList(array($cursor,'fetch'));
        if($result && $this->loader)
            $result->addFilter($this->loader);
        return $result;
    }
}