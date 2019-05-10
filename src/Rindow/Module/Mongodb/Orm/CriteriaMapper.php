<?php
namespace Rindow\Module\Mongodb\Orm;

use Rindow\Database\Dao\Exception;
use Rindow\Persistence\Orm\Criteria\CriteriaMapper as CriteriaMapperInterface;

class CriteriaMapper implements CriteriaMapperInterface
{
    protected $context;

    public function setContext($context)
    {
        $this->context = $context;
    }

    public function prepare(/* CommonAbstractCriteria */$criteria,$resultClass=null)
    {
        $entityClass = $this->getEntityClass($criteria);
        return new PreparedCriteria(
            $criteria,
            null,
            $entityClass,
            $resultClass);
    }

    public function getEntityClass($criteria)
    {
        $roots = $criteria->getRoots();
        if($roots->getJoins())
            throw new Exception\DomainException('"join" is not supported.');
        return $roots->getNodeName();
    }
}
