<?php
namespace Rindow\Module\Mongodb\Support;

use MongoDB\BSON\Persistable;

abstract class AbstractStdClass implements Persistable
{
    public function bsonSerialize()
    {
        $data = get_object_vars($this);
        if(!isset($data['_id']))
            unset($data['_id']);
        return $data;
    }

    public function bsonUnserialize(array $data)
    {
        foreach ($data as $key => $value) {
            $this->$key = $value;
        }
    }
}
