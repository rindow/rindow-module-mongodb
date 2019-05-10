<?php
namespace Rindow\Module\Mongodb;

class StandaloneModule
{
    public function getConfig()
    {
        return array(
            'aop' => array(
                'intercept_to' => array(
                    'Rindow\\Module\\Mongodb\\Repository\\MongodbRepository' => true,
                ),
                'pointcuts' => array(
                    'Rindow\\Module\\Mongodb\\Repository\\MongodbRepository'=> 
                        'execution(Rindow\\Module\\Mongodb\\Repository\\MongodbRepository::'.
                            '(save|findById|findAll|findOne|delete|deleteById|existsById|count)())',
                ),
                'aspects' => array(
                    'Rindow\\Module\\Mongodb\\DefaultDaoExceptionAdvisor' => array(
                        'advices' => array(
                            'afterThrowingAdvice' => array(
                                'type' => 'after-throwing',
                                'pointcut_ref' => array(
                                    'Rindow\\Module\\Mongodb\\Repository\\MongodbRepository'=>true,
                                ),
                            ),
                        ),
                    ),
                ),
            ),
            'container' => array(
                'aliases' => array(
                    'Rindow\\Persistence\\OrmShell\\DefaultCriteriaMapper' => 'Rindow\\Module\\Mongodb\\Orm\\DefaultCriteriaMapper',
                    'Rindow\\Persistence\\OrmShell\\DefaultResource'       => 'Rindow\\Module\\Mongodb\\Orm\\DefaultResource',
                ),
                'components' => array(
                    'Rindow\\Module\\Mongodb\\DefaultDataSource' => array(
                        'class' => 'Rindow\\Module\\Mongodb\\Core\\DataSource',
                        'properties' => array(
                            'config' => array('config'=>'database::connections::mongodb'),
                        ),
                    ),
                    /*
                    *  ORM "OrmShell"
                    */
                    'Rindow\\Module\\Mongodb\\Orm\\DefaultAbstractMapper' => array(
                        'properties' => array(
                            'dataSource' => array('ref'=>'Rindow\\Module\\Mongodb\\DefaultDataSource'),
                        ),
                        'proxy' => 'disable',
                    ),
                    //'Rindow\\Module\\Mongodb\\Orm\\DefaultResource' => array(
                    //    'class' => 'Rindow\\Module\\Mongodb\\Orm\\Resource',
                    //    'properties' => array(
                    //        'dataSource' => array('ref'=>'Rindow\\Module\\Mongodb\\DefaultDataSource'),
                    //    ),
                    //    'proxy' => 'disable',
                    //),
                    'Rindow\\Module\\Mongodb\\Orm\\DefaultCriteriaMapper' => array(
                        'class' => 'Rindow\\Module\\Mongodb\\Orm\\CriteriaMapper',
                    ),
                ),
            ),
        );
    }
}
