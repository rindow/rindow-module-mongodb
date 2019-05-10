<?php
namespace Rindow\Module\Mongodb\Core;

use Rindow\Database\Dao\Exception;
use Rindow\Database\Dao\Exception\ExceptionInterface as DatabaseErrorCode;
use MongoDB\Driver\Manager;
use MongoDB\Driver\BulkWrite;
use MongoDB\Driver\Query;
use MongoDB\Driver\Command;

class Connection
{
    static protected $errorPattern = array(
        'E11000' => DatabaseErrorCode::ALREADY_EXISTS,
    );
    protected $config;
    protected $uri = 'mongodb://127.0.0.1/';
    protected $uriOptions = array();
    protected $driverOptions = array();
    protected $username;
    protected $password;
    protected $database;
    protected $manager;
    protected $bulkMode = false;
    protected $bulkWrite=array();
    protected $listener;
    protected $typeMap = array('root'=>'array','document'=>'array','array'=>'array');

    public function __construct(array $config=null)
    {
        if($config)
            $this->setConfig($config);
    }

    public function setConfig($config)
    {
        $this->config = $config;
        if(isset($config['uri']))
            $this->uri = $config['uri'];
        if(isset($config['uriOptions']))
            $this->uriOptions = $config['uriOptions'];
        if(isset($config['driverOptions']))
            $this->driverOptions = $config['driverOptions'];
        if(!isset($config['database']))
            throw new Exception\DomainException('a database is not specified.');
        $this->setDatabase($config['database']);
        if(isset($config['typeMap']))
            $this->setTypeMap($config['typeMap']);
    }

    public function setDatabase($database)
    {
        $this->database = $database;
    }
    public function getDatabase()
    {
        return $this->database;
    }

    public function setTypeMap($typeMap)
    {
        $this->typeMap = $typeMap;
    }

    public function getTypeMap()
    {
        return $this->typeMap;
    }

    public function authenticate($username,$password)
    {
        $this->username = $username;
        $this->password = $password;
    }

    public function connect()
    {
        if($this->manager)
            return;
        $uriOptions = $this->uriOptions;
        if($this->username) {
            $uriOptions['username'] = $this->username;
            $uriOptions['password'] = $this->password;
        }
        $this->manager = new Manager($this->uri,$this->uriOptions,$this->driverOptions);
        if($this->listener)
            call_user_func($this->listener,$this);
    }

    protected function getManager()
    {
        $this->connect();
        return $this->manager;
    }

    protected function assertDatabase()
    {
        if(!$this->database)
            throw new Exception\DomainException('a database is not specified.');
    }

    protected function getBulkWrite($collection)
    {
        $this->assertDatabase();
        $namespace = $this->database.'.'.$collection;
        if(isset($this->bulkWrite[$namespace]))
            return $this->bulkWrite[$namespace];
        if(!isset($this->config['write']))
            $this->bulkWrite[$namespace] = new BulkWrite();
        else
            $this->bulkWrite[$namespace] = new BulkWrite($this->config['write']);
        return $this->bulkWrite[$namespace];
    }

    public function insert($collection,$document)
    {
        $id = $this->getBulkWrite($collection)->insert($document);
        if(!$this->bulkMode) {
            $this->flush();
        }
        return $id;
    }

    public function update($collection,$filter,$document,array $options=null)
    {
        if($options==null)
            $id = $this->getBulkWrite($collection)->update($filter,$document);
        else
            $id = $this->getBulkWrite($collection)->update($filter,$document,$options);
        if(!$this->bulkMode)
            $this->flush();
    }

    public function delete($collection,$filter,array $options=null)
    {
        if($options==null)
            $id = $this->getBulkWrite($collection)->delete($filter);
        else
            $id = $this->getBulkWrite($collection)->delete($filter,$options);
        if(!$this->bulkMode)
            $this->flush();
    }

    public function find($collection, $filter, $options=null, $readPreference=null)
    {
        $this->assertDatabase();
        $namespace = $this->database.'.'.$collection;
        if($filter===null)
            $filter = array();
        if($options===null)
            $options = array();
        $query = new Query($filter, $options);
        if(!$readPreference)
            $cursor = $this->getManager()->executeQuery($namespace,$query);
        else
            $cursor = $this->getManager()->executeQuery($namespace,$query,$readPreference);
        $cursor->setTypeMap($this->typeMap);
        return new Cursor($cursor,$this);
    }

    public function count($collection, array $filter=null, $options=null, $readPreference=null)
    {
        if($filter)
            $command = array('count'=>$collection,'query'=>$filter);
        else
            $command = array('count'=>$collection);
        if($options)
            $command = array_merge($command,$options);
        $cursor = $this->executeCommand($command,$readPreference);
        $result = $cursor->toArray();
        $result = $result[0];
        if(is_array($result))
            return $result['n'];
        else
            return $result->n;
    }

    public function group($collection,array $key,$reduce,array $initial,array $options=null)
    {
        $command = array('group'=> array(
            'ns'=>$collection,'key'=>$key,'$reduce'=>$reduce,'initial'=>$initial));
        if($options)
            $command = array_merge_recursive($command,array('group'=>$options));
        $cursor = $this->executeCommand($command);
        $result = $cursor->toArray();
        $result = $result[0];
        if(is_array($result))
            return $result['retval'];
        else
            return $result->retval;
    }

    public function executeCommand(array $command,$readPreference=null)
    {
        $this->assertDatabase();
        $cmd = new Command($command);
        $cursor = $this->getManager()->executeCommand($this->database,$cmd,$readPreference);
        $cursor->setTypeMap($this->typeMap);
        return $cursor;
    }

    public function setConnectedEventListener($listener)
    {
        $this->listener = $listener;
    }

    public function isConnected()
    {
        return $this->manager ? true : false;
    }

    public function setBulkMode($bulkMode)
    {
        $this->bulkMode = $bulkMode;
    }

    public function getBulkMode()
    {
        return $this->bulkMode;
    }

    public function flush($writeConcern=null)
    {
        if(!empty($this->bulkWrite)) {
            $manager = $this->getManager();
            foreach($this->bulkWrite as $namespace => $bulkWrite) {
                if($writeConcern)
                    $manager->executeBulkWrite($namespace ,$bulkWrite, $writeConcern);
                else
                    $manager->executeBulkWrite($namespace ,$bulkWrite);
            }
        }

        $this->clean();
    }

    public function clean()
    {
        $this->bulkWrite = array();
    }

    public function getReadConcern()
    {
        return $this->getManager()->getReadConcern();
    }

    public function getReadPreference()
    {
        return $this->getManager()->getReadPreference();
    }

    public function getServers()
    {
        return $this->getManager()->getServers();
    }

    public function getWriteConcern()
    {
        return $this->getManager()->getWriteConcern();
    }

    public function selectServer()
    {
        return $this->getManager()->selectServer($readPreference);
    }

    protected function errorCodeMapping($code,$message)
    {
        $matched = 0;
        foreach (self::$errorPattern as $pattern => $code) {
            if(strpos($message, $pattern)===0) {
                $matched = $code;
                return $matched;
            }
        }
        return DatabaseErrorCode::ERROR;
    }
}