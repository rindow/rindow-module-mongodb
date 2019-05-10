<?php
namespace Rindow\Module\Mongodb\Orm;

use Rindow\Persistence\Orm\Criteria\CriteriaQuery;
use Rindow\Module\Mongodb\Exception;

class PreparedCriteria
{
    protected $criteria;
    protected $resultClass;
    
    public function __construct($criteria,$queryFactory,$entityClass,$resultClass=null)
    {
        $this->criteria = $criteria;
        $this->queryFactory = $queryFactory;
        $this->entityClass = $entityClass;
        $this->resultClass = $resultClass;
    }

    public function getCriteria()
    {
        return $this->criteria;
    }

    public function getQueryFactory()
    {
        return $this->queryFactory;
    }

    public function getEntityClass()
    {
        return $this->entityClass;
    }

    public function getResultClass()
    {
        return $this->resultClass;
    }

    public function isFindCommand()
    {
        if($this->getGroupList() || $this->getSelectionType()=='FUNCTION')
            return false;
        return true;
    }

    public function getSelectionType()
    {
        return $this->criteria->getSelection()->getExpressionType();
    }

    public function getGroupList()
    {
        return $this->criteria->getGroupList();
    }

    protected function assertRoot($rootClass, $selection)
    {
        if($selection->getExpressionType()=='FUNCTION')
            throw new Exception\DomainException('"FUNCTION" expression is not supported on Google Cloud Datastore.');
        if($selection->getExpressionType()!='PATH') {
            var_dump($selection);
            throw new Exception\DomainException('selection must be property in "'.$rootClass.'".');
        }
        $selectionRoot = $selection->getParentPath();
        if($selectionRoot->getExpressionType()!='ROOT')
            throw new Exception\DomainException('selection must be property"'.$rootClass.'".');
        if($rootClass != $selectionRoot->getNodeName())
            throw new Exception\DomainException('selection must belong to same root.:"'.$rootClass.'","'.$selectionRoot->getNodeName().'"');
    }

    public function buildQuery($params,$options,$collection)
    {
        $mapped = true;
        $command = $resultFormatter = null;
        $filter = $this->buildFilter($params);
        if($this->isFindCommand()) {
            $sort = $this->buildSort();
            if(!empty($sort))
                $options['sort'] = $sort;
            if($this->getSelectionType()!='ROOT') {
                $mapped = false;
            }
        } else {
            list($command,$resultFormatter) = $this->buildCommand($collection,$params,$filter,$options);
            $mapped = false;
        }
        return array($filter,$options,$command,$resultFormatter,$mapped);
    }

/*
    protected function ffff()
    {
        $selection = $this->criteria->getSelection();
        if($selection->isCompoundSelection()) {
            $selections = $selection->getCompoundSelectionItems();
        } else {
            if($selection->getExpressionType()=='ROOT') {
                $kind = $this->getKind($selection);
                if($kind!=$query->getKind())
                    throw new Exception\DomainException('Selection must be comprised a root kind.');
                return;
            } elseif ($selection->getExpressionType()=='FUNCTION') {
                $this->buildFunctionProjection($query,$selection);
                return;
            }
            $selections = array($selection);
        }
        $rootKind = $query->getKind();
        if($this->criteria->getGroupRestriction())
            throw new Exception\DomainException('"HAVING" expression is not supported on Google Cloud Datastore.');

        foreach ($this->criteria->getGroupList() as $group) {
            $this->assertRoot($rootKind, $group);
            $groups[$group->getNodeName()] = true;
        }
        foreach ($selections as $selection) {
            $this->assertRoot($rootKind, $selection);
            $propertyName = $selection->getNodeName();
            $projection = new Projection($propertyName);
            if(array_key_exists($propertyName, $groups) && $groups[$propertyName])
                $projection->setGrouping();
            $query->addProjection($projection);
        }
        if($this->criteria->isDistinct())
            $query->setDistinct(true);
    }
*/
    public function buildCommand($collection,$params,$filter,$options)
    {
        $selection = $this->criteria->getSelection();
        $expressionType = $selection->getExpressionType();
        if($this->criteria->getGroupList())
            return $this->buildGroupCommand($selection,$collection,$params,$filter,$options);
        if($expressionType == 'FUNCTION')
            return $this->buildFunctionCommand($selection,$collection,$params,$filter,$options);
        throw new Exception\DomainException('command not supported:'.$expressionType);
    }

    public function buildFunctionCommand($function,$collection,$params,$filter,$options)
    {
        $entityClass = $this->getEntityClass();
        $operator = $function->getOperator();
        switch ($operator) {
            case 'COUNT':
                $command = $this->buildCountCommand($entityClass,$function,$collection,$params,$filter,$options);
                $resultFilter = array($this,'_resultCount');
                break;
            
            default:
                throw new Exception\DomainException('"'.$function->getOperator().'" function is not supported on the Mongodb CriteriaMapper.');
                break;
        }
        return array($command,$resultFilter);
    }

    protected function buildCountCommand($entityClass,$function,$collection,$params,$filter,$options)
    {
        $expressions = $function->getExpressions();
        if(count($expressions)!=1)
            throw new Exception\DomainException('COUNT must provide a property.');
        $expression = $expressions[0];
        if($expression->getExpressionType()!='ROOT')
            throw new Exception\DomainException('COUNT must have root entity.');
        if($entityClass!=$expression->getNodeName())
            throw new Exception\DomainException('COUNT must belong to same root.:"'.$entityClass.'","'.$expression->getNodeName().'"');
        $command = array();
        $command['count'] = $collection;
        if(!empty($filter))
            $command['query'] = $filter;
        if(isset($options['limit']))
            $command['limit'] = $options['limit'];
        if(isset($options['skip']))
            $command['skip'] = $options['skip'];
        if(isset($options['hint']))
            $command['hint'] = $options['hint'];
        if(isset($options['readConcern']))
            $command['readConcern'] = $options['readConcern'];
        return $command;
    }

    public function _resultCount($result)
    {
        if(!$result)
            return $result;
        if(!isset($result['ok']) || !$result['ok']) {
            throw new Exception\RuntimeException('count command error at "'.$this->getEntityClass().'"');
        }
        return array($result['n']);
    }

    public function buildFilter(array $parameters=null)
    {
        $restriction = $this->criteria->getRestriction();
        return $this->getFilterSub($restriction,$parameters);
    }

    public function getFilterSub($restriction,array $parameters=null)
    {
        if($parameters==null)
            $parameters = array();
        $rootClass = $this->getEntityClass();
        if($restriction === null)
            return array();
        if($restriction->getExpressionType() != 'OPERATOR')
            throw new Exception\DomainException('restriction must be "OPERATOR".');
        $filter = $this->getFilter($rootClass,$restriction,$parameters);
        return $filter;
    }

    protected function getFilter($rootClass,$restriction,$parameters)
    {
        if($restriction->getOperator()=='AND' || $restriction->getOperator()=='OR')
            return $this->getCompositeFilter($rootClass,$restriction,$parameters);

        list($property, $value, $swap) = $this->getFilterParameter($rootClass,$restriction);
        switch ($restriction->getOperator()) {
            case 'EQUAL':
                $operator = '$eq';
                break;
            case 'GREATER_THAN':
                if($swap)
                    $operator = '$lt';
                else
                    $operator = '$gt';
                break;
            case 'GREATER_THAN_OR_EQUAL':
                if($swap)
                    $operator = '$lte';
                else
                    $operator = '$gte';
                break;
            case 'LESS_THAN':
                if($swap)
                    $operator = '$gt';
                else
                    $operator = '$lt';
                break;
            case 'LESS_THAN_OR_EQUAL':
                if($swap)
                    $operator = '$gte';
                else
                    $operator = '$lte';
                break;
            default:
                throw new Exception\DomainException('unknown operator "'.$restriction->getOperator().'".');
        }
        if($value->getExpressionType()=='CONSTANT') {
            $v = $value->getValue();
        } elseif($value->getExpressionType()=='PARAMETER') {
            if(!array_key_exists($value->getName(), $parameters))
                throw new Exception\DomainException('the parameter "'.$value->getName().'" is not found in a query of "'.$rootClass.'"');
            $v = $parameters[$value->getName()];
        } else {
            throw new Exception\DomainException('Type "'.$value->getExpressionType().'" is invalid as a filter-value in "'.$rootClass.'"');
        }
        if($operator=='$eq')
            $filter = array($property->getNodeName()=>$v);
        else
            $filter = array($property->getNodeName()=>array($operator=>$v));
        return $filter;
    }

    protected function getFilterParameter($rootClass,$restriction)
    {
        $expressions = $restriction->getExpressions();
        if(count($expressions)!=2)
            throw new Exception\DomainException('a filter must have two expressions.');
        $x = $expressions[0];
        $y = $expressions[1];
        $swap = false;
        if($y->getExpressionType() == 'PATH') {
            list($x,$y) = array($y,$x);
            $swap = true;
        }
        if($x->getExpressionType() != 'PATH' ||
            ($y->getExpressionType()!='CONSTANT' && $y->getExpressionType()!='PARAMETER'))
            throw new Exception\DomainException('a filter must set of selection and constant value.');
        $this->assertRoot($rootClass, $x);
        return array($x,$y,$swap);
    }

    protected function getCompositeFilter($rootClass,$restriction,$parameters)
    {
        switch ($restriction->getOperator()) {
            case 'AND':
                $operator = '$and';
                break;
            case 'OR':
                $operator = '$or';
                break;
            default:
                throw new Exception\DomainException('unknown operator "'.$restriction->getOperator().'".');
        }
        $subFilters = array();
        foreach ($restriction->getExpressions() as $expression) {
            if($expression->getExpressionType() != 'OPERATOR')
                throw new Exception\DomainException('restriction must be "OPERATOR".');
            $subFilters[] = $this->getFilter($rootClass,$expression,$parameters);
        }
        return array($operator => $subFilters);
    }

    public function buildSort()
    {
        $sort = array();
        $rootClass = $this->getEntityClass();
        foreach ($this->criteria->getOrderList() as $order) {
            $expression = $order->getExpression();
            $this->assertRoot($rootClass, $expression);
            if($order->isAscending())
                $direction = 1;
            else
                $direction = -1;
            $sort[$expression->getNodeName()] = $direction;
        }
        return $sort;
    }

    protected function buildGroupCommand($selection,$collection,$params,$filter,$options)
    {
        $entityClass = $this->getEntityClass();
        if(!$selection->isCompoundSelection())
            throw new Exception\DomainException('group command must be "COMPOUND" in "'.$entityClass.'"');

        foreach ($this->criteria->getGroupList() as $path) {
            $this->assertRoot($entityClass, $path);
            $key[$path->getNodeName()] = 1;
        }
        $initial = array();
        $script = '';
        $finalizeScript = '';
        foreach ($selection->getCompoundSelectionItems() as $subSelection) {
            if($subSelection->getExpressionType()=='PATH') {
                if(isset($key[$subSelection->getNodeName()]))
                    continue;
                $alias = $nodeName = $subSelection->getNodeName();
                if($subSelection->getAlias())
                    $alias = $subSelection->getAlias();
                $script .= 'result.'.$alias.' = curr.'.$nodeName.';';
                $initial[$alias] = 0;
            } elseif($subSelection->getExpressionType()=='FUNCTION') {
                if($subSelection->getAlias()==null) {
                    throw new Exception\DomainException('A function "'.$subSelection->getOperator().'" must have alias in a compound selection.:"'.$entityClass.'"');
                }
                list($functionScript,$initialValue) = $this->getGroupFunctionScript($entityClass,$subSelection);
                $script .= $functionScript;
                $initial[$subSelection->getAlias()] = $initialValue;
            }
        }
        $script = 'function (curr, result) {'.$script.'}';
        $restriction = $this->criteria->getGroupRestriction();
        $condition = $this->getFilterSub($restriction,$params);
        $command = array(
            'group' => array(
                'ns' => $collection,
                'key' => $key,
                '$reduce' => $script,
                'initial' => $initial,
            ),
        );
        if(!empty($condition))
            $command['group']['cond'] = $condition;
        if($finalizeScript!='')
            $command['group']['finalize'] = $finalizeScript;
        return array($command,array($this,'_resultGroup'));
    }

    protected function getGroupFunctionScript($entityClass,$function)
    {
        $operator = $function->getOperator();
        $expressions = $function->getExpressions();
        if(count($expressions)!=1)
            throw new Exception\DomainException('Aggregate function "'.$operator.'" must have one expression in "'.$entityClass.'".');
        $expression = $expressions[0];
        $this->assertRoot($entityClass,$expression);

        switch ($operator) {
            case 'COUNT':
                $script = 'result.'.$function->getAlias().'++';
                $initial = 0;
                break;
            
            case 'SUM':
                $script = 'result.'.$function->getAlias().' += curr.'.$expression->getNodeName();
                $initial = 0;
                break;

            case 'MAX':
                $script = 'result.'.$function->getAlias().' = Math.max(result.'.$function->getAlias().',curr.'.$expression->getNodeName().')';
                $initial = ~PHP_INT_MAX;
                break;

            case 'MIN':
                $script = 'result.'.$function->getAlias().' = Math.min(result.'.$function->getAlias().',curr.'.$expression->getNodeName().')';
                $initial = PHP_INT_MAX;
                break;

            default:
                throw new Exception\DomainException('"'.$operator.'" function is not supported on the Mongodb CriteriaMapper.');
                break;
        }
        return array($script.';',$initial);
    }

    public function _resultGroup($result)
    {
        if(!$result)
            return $result;
        if(!isset($result['ok']) || !$result['ok'])
            throw new Exception\RuntimeException('group command error at "'.$this->getEntityClass().'"');
        return $result['retval'];
    }
}