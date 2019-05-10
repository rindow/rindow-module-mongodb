<?php
namespace Rindow\Module\Mongodb\Core;

use Interop\Lenient\Dao\Resource\DataSource as DataSourceInterface;
use Rindow\Database\Dao\Exception;
use Rindow\Module\Mongodb\Core\Connection;

class DataSource implements DataSourceInterface
{
    protected $config;
    protected $connection;

    public function __construct($config = null)
    {
        if($config)
            $this->setConfig($config);
    }

    public function setConfig($config)
    {
        $this->config = $config;
    }

    public function getConnection($username=null,$password=null)
    {
        if($this->connection) {
            return $this->connection;
        }
        $connection = new Connection($this->config);
        $this->connection = $connection;
        return $this->connection;
    }
}
