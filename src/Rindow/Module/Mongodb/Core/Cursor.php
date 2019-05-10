<?php
namespace Rindow\Module\Mongodb\Core;

use IteratorAggregate;
use IteratorIterator;
use Interop\Lenient\Dao\Query\Cursor as CursorInterface;

class Cursor implements CursorInterface,IteratorAggregate
{
    protected $first = true;
    protected $mongoCursor;
    protected $iterator;
    protected $connection;

    public function __construct($mongoCursor,$connection)
    {
        $this->mongoCursor = $mongoCursor;
        $this->connection = $connection;
    }

    public function close()
    {
        $this->iterator = null;
        $this->mongoCursor = null;
    }

    protected function getIteratorIterator()
    {
        if($this->iterator) {
            return $this->iterator;
        }
        $this->iterator = new IteratorIterator($this->mongoCursor);
        return $this->iterator;
    }

    public function getConnection()
    {
        return $this->connection;
    }

    public function getId()
    {
        return $this->mongoCursor->getId();
    }

    public function getServer()
    {
        return $this->mongoCursor->getServer();
    }

    public function isDead()
    {
        return $this->mongoCursor->isDead();
    }

    public function setTypeMap(array $typeMap)
    {
        return $this->mongoCursor->setTypeMap($typeMap);
    }

    public function toArray()
    {
        return $this->mongoCursor->toArray();
    }

    public function fetch()
    {
        $iterator = $this->getIteratorIterator();
        if($this->first)
            $iterator->rewind();
        else
            $iterator->next();
        $this->first = false;
        return $iterator->current();
    }

    public function getIterator()
    {
        return $this->getIteratorIterator();
    }
}