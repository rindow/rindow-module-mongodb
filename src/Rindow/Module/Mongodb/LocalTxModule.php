<?php
namespace Rindow\Module\Mongodb;

class LocalTxModule
{
    public function getConfig()
    {
        return array(
            'aop' => array(
                'plugins' => array(
                    'Rindow\\Transaction\\Support\\AnnotationHandler'=>true,
                ),
                'transaction' => array(
                    'defaultTransactionManager' => 'Rindow\\Module\\Mongodb\\Transaction\\DefaultTransactionManager',
                    'managers' => array(
                        'Rindow\\Module\\Mongodb\\Transaction\\DefaultTransactionManager' => array(
                            'transactionManager' => 'Rindow\\Module\\Mongodb\\Transaction\\DefaultTransactionManager',
                            'advisorClass' => 'Rindow\\Transaction\\Support\\TransactionAdvisor',
                        ),
                    ),
                ),
                'intercept_to' => array(
                    'Rindow\\Module\\Mongodb\\Repository\\MongodbRepository' => true,
                ),
                'pointcuts' => array(
                    'Rindow\\Module\\Mongodb\\Repository\\MongodbRepository'=> 
                        'execution(Rindow\\Module\\Mongodb\\Repository\\MongodbRepository::'.
                            '(save|delete|deleteById)())',
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
                'aspectOptions' => array(
                    'Rindow\\Transaction\\DefaultTransactionAdvisor' => array(
                        'advices' => array(
                            'required' => array(
                                'pointcut_ref' => array(
                                    'Rindow\\Module\\Mongodb\\Repository\\MongodbRepository' => true,
                                ),
                            ),
                        ),
                    ),
                ),
            ),
            'container' => array(
                'aliases' => array(
                    'Rindow\\Persistence\\OrmShell\\DefaultCriteriaMapper' => 'Rindow\\Module\\Mongodb\\Orm\\DefaultCriteriaMapper',
                    //'Rindow\\Persistence\\OrmShell\\DefaultResource'       => 'Rindow\\Module\\Mongodb\\Orm\\DefaultResource',
                    'Rindow\\Persistence\\OrmShell\\Transaction\\DefaultTransactionSynchronizationRegistry' => 'Rindow\\Module\\Mongodb\\Transaction\\DefaultTransactionSynchronizationRegistry',
                    'Rindow\\Persistence\\OrmShell\\Repository\\DefaultQueryBuilder' => 'Rindow\\Module\\Mongodb\\Repository\\DefaultQueryBuilder',
                ),
                'components' => array(
                    'Rindow\\Module\\Mongodb\\Transaction\\DefaultDataSource' => array(
                        'class' => 'Rindow\\Module\\Mongodb\\Core\\DataSource',
                        'properties' => array(
                            'config' => array('config'=>'database::connections::mongodb'),
                            //'transactionManager' => array('ref'=>'Rindow\\Module\\Mongodb\\Transaction\\DefaultTransactionManager'),
                        ),
                    ),
                    'Rindow\\Module\\Mongodb\\Transaction\\DefaultTransactionManager' => array(
                        'class' => 'Rindow\\Transaction\\Local\\TransactionManager',
                        //'properties' => array(
                        //    'useSavepointForNestedTransaction' => array('value'=>false),
                        //),
                    ),
                    'Rindow\\Module\\Mongodb\\Transaction\\DefaultTransactionBoundary' => array(
                        'class'=>'Rindow\\Transaction\\Support\\TransactionBoundary',
                        'properties' => array(
                            'transactionManager' => array('ref'=>'Rindow\\Module\\Mongodb\\Transaction\\DefaultTransactionManager'),
                        ),
                        'proxy' => 'disable',
                    ),
                    'Rindow\\Module\\Mongodb\\Transaction\\DefaultTransactionSynchronizationRegistry' => array(
                        'class'=>'Rindow\\Transaction\\Support\\TransactionSynchronizationRegistry',
                        'properties' => array(
                            'transactionManager' => array('ref'=>'Rindow\\Module\\Mongodb\\Transaction\\DefaultTransactionManager'),
                        ),
                        'proxy' => 'disable',
                    ),
                    /*
                     *  ORM "OrmShell"
                     */
                    'Rindow\\Module\\Mongodb\\Orm\\DefaultAbstractMapper' => array(
                        'properties' => array(
                            'dataSource' => array('ref'=>'Rindow\\Module\\Mongodb\\Transaction\\DefaultDataSource'),
                        ),
                        'proxy' => 'disable',
                    ),
                    //'Rindow\\Module\\Mongodb\\Orm\\DefaultResource' => array(
                    //    'class' => 'Rindow\\Module\\Mongodb\\Orm\\Resource',
                    //    'properties' => array(
                    //        'dataSource' => array('ref'=>'Rindow\\Module\\Mongodb\\Transaction\\DefaultDataSource'),
                    //    ),
                    //    'proxy' => 'disable',
                    //),
                    'Rindow\\Module\\Mongodb\\Orm\\DefaultCriteriaMapper' => array(
                        'class' => 'Rindow\\Module\\Mongodb\\Orm\\CriteriaMapper',
                    ),
                    /*
                     *  Repository
                     */
                    /*
                    'Rindow\\Module\\Mongodb\\Repository\\DefaultRepositoryFactory' => array(
                        'class' => 'Rindow\\Module\\Mongodb\\Repository\\MongodbRepositoryFactory',
                        'properties' => array(
                            'dataSource' => array('ref'=>'Rindow\\Module\\Mongodb\\Transaction\\DefaultDataSource'),
                            'queryBuilder' => array('ref'=>'Rindow\\Module\\Mongodb\\Repository\\DefaultQueryBuilder'),
                        ),
                    ),
                    */
                    'Rindow\\Module\\Mongodb\\Repository\\DefaultQueryBuilder'=>array(
                        'class' => 'Rindow\\Database\\Dao\\Support\\QueryBuilder',
                    ),
                    'Rindow\\Module\\Mongodb\\Repository\\AbstractRepository' => array(
                        'class' => 'Rindow\\Module\\Mongodb\\Repository\\MongodbRepository',
                        'properties' => array(
                            'dataSource' => array('ref'=>'Rindow\\Module\\Mongodb\\Transaction\\DefaultDataSource'),
                            'queryBuilder' => array('ref'=>'Rindow\\Module\\Mongodb\\Repository\\DefaultQueryBuilder'),
                            // Override properties for the repository
                            //'collection' => array('value'=>'your_collection_name'),
                            //'dataMapper' => array('ref' =>'your_data_mapper'),
                        ),
                    ),

                    /*
                     * Interop DAO Exception Advisor for Mongodb
                     */
                    'Rindow\\Module\\Mongodb\\DefaultDaoExceptionAdvisor' => array(
                        'class' => 'Rindow\\Module\\Mongodb\\Support\\DaoExceptionAdvisor',
                    ),
                ),
            ),
        );
    }
}
