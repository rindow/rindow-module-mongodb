<?php
namespace RindowTest\Module\Mongodb\Authentication\CrudRepositoryUserDetailsManagerTest;

use PHPUnit\Framework\TestCase;
use Rindow\Module\Mongodb\Repository\MongodbRepository;
use Rindow\Module\Mongodb\Core\DataSource;
use Rindow\Security\Core\Authentication\UserDetails\UserManager\CrudRepositoryUserDetailsManager;
use Rindow\Security\Core\Authentication\UserDetails\User;
use Rindow\Security\Core\Authentication\Exception\DuplicateUsernameException;
use Rindow\Container\ModuleManager;

class Test extends TestCase
{
    const AUTHACCOUNTS = 'rindow_authusers';
    public static $skip = false;
    public static function setUpBeforeClass()
    {
        if (!extension_loaded('mongodb')) {
            self::$skip = true;
            return;
        }
        try {
            $client = new \MongoDB\Driver\Manager();
            $query = new \MongoDB\Driver\Query(array());
            $cursor = $client->executeQuery('test.'.self::AUTHACCOUNTS,$query);
        } catch(\Exception $e) {
            self::$skip = true;
            return;
        }
    }

    public static function tearDownAfterClass()
    {
        if(self::$skip)
            return;

        $client = new \MongoDB\Driver\Manager();
        $cmd = new \MongoDB\Driver\Command(array('drop'=>self::AUTHACCOUNTS));
        try {
            //$client->executeCommand('test',$cmd);
        } catch(\Exception $e) {
            ;
        }
    }

    public function dropCollection($collection)
    {
        $client = new \MongoDB\Driver\Manager();
        $cmd = new \MongoDB\Driver\Command(array('drop'=>$collection));
        try {
            $client->executeCommand('test',$cmd);
        } catch(\Exception $e) {
            ;
        }
    }

    public function insertToCollection($collection,$data)
    {
        $client = new \MongoDB\Driver\Manager();
        $bulkWrite = new \MongoDB\Driver\BulkWrite();
        $bulkWrite->insert($data);
        return $client->executeBulkWrite('test.'.$collection,$bulkWrite);
    }

    public function setUp()
    {
        if(self::$skip) {
            $this->markTestSkipped();
            return;
        }
        $this->dropCollection(self::AUTHACCOUNTS);
        $client = new \MongoDB\Driver\Manager();
        $cmd = new \MongoDB\Driver\Command(array('create'=>self::AUTHACCOUNTS));
        $client->executeCommand('test',$cmd);
        $cmd = new \MongoDB\Driver\Command(array(
            'createIndexes'=>self::AUTHACCOUNTS,
            'indexes' => array(
                array('name'=>'username_idx','key'=>array('username'=>1),'unique'=>true),
            ),
        ));
        $client->executeCommand('test',$cmd);
    }

    public static function getMongoClientStatic()
    {
        $client = new \MongoDB\Driver\Manager();
        return $client;
    }
    public function getMongoClient()
    {
        return self::getMongoClientStatic();
    }
    public function getData($collection,$filter=array())
    {
        $mongo = $this->getMongoClient();
        $query = new \MongoDB\Driver\Query($filter);
        $cursor = $mongo->executeQuery('test.'.$collection,$query);
        $cursor->setTypeMap(array('root'=>'array','document'=>'array','array'=>'array'));
        return $cursor;
    }

    public function getCrudRepositoryUserDetailsManager()
    {
        $config = array(
            'module_manager' => array(
                'modules'=>array(
                    'Rindow\Aop\Module'=>true,
                    'Rindow\Transaction\Local\Module'=>true,
                    'Rindow\Security\Core\Module'=>true,
                    'Rindow\Module\Mongodb\LocalTxModule'=>true,
                ),
                'enableCache'=>false,
            ),
            'container'=>array(
                'aliases'=>array(
                    //'Rindow\\Security\\Core\\Authentication\\DefaultContextStrage' => 'Your_Security_Context_Strage',
                    //'Rindow\\Security\\Core\\Authentication\\DefaultUserDetailsService' => 'Your_UserDetailsManager',
                    //'Rindow\\Security\\Core\\Authentication\\DefaultSqlUserDetailsManagerDataSource' => 'Sql_data_source',
                    //'Rindow\\Security\\Core\\Authentication\\DefaultSqlUserDetailsManagerTransactionBoundary' => 'Sql_Transaction_Boundary',
                    'Rindow\\Security\\Core\\Authentication\\DefaultUserDetailsRepository' => __NAMESPACE__.'\AuthUsersRepository',
                ),
                'components'=>array(
                    __NAMESPACE__.'\AuthUsersRepository'=>array(
                        'parent'=>'Rindow\\Module\\Mongodb\\Repository\\AbstractRepository',
                        'properties'=>array(
                            'collection'=>array('value'=>self::AUTHACCOUNTS),
                        ),
                    ),
                ),
            ),
            'database'=>array(
                'connections'=>array(
                    'mongodb' =>array(
                        'database'=>'test',
                    ),
                ),
            ),
            'security' => array(
                'authentication' => array(
                    'default' => array(
                        'maxPasswordAge' => 1, // 1 days
                    ),
                ),
            ),
        );
        $mm = new ModuleManager($config);
        $manager = $mm->getServiceLocator()->get('Rindow\\Security\\Core\\Authentication\\DefaultCrudRepositoryUserDetailsManager');
        return $manager;
    }

    public function dump()
    {
        $cursor = $this->getData(self::AUTHACCOUNTS);
        foreach ($cursor as $row) {
            var_dump($row);
        }
    }

    public function testLoadingDefaultSuccess()
    {
        $manager = $this->getCrudRepositoryUserDetailsManager();
        $user = new User('foo','fooPass',array('ROLE_ADMIN','ROLE_USER'));
        $manager->createUser($user);
        $this->assertInstanceOf('MongoDB\BSON\ObjectId',$user->getId());
        $id = $user->getId();

        $user = $manager->loadUserByUsername('foo');
        $this->assertInstanceOf('Rindow\Security\Core\Authentication\UserDetails\User',$user);
        $this->assertEquals('foo',$user->getUsername());
        $this->assertEquals('fooPass',$user->getPassword());
        $this->assertEquals(array('ROLE_ADMIN','ROLE_USER'),$user->getAuthorities());
        $this->assertTrue($user->isAccountNonExpired());
        $this->assertTrue($user->isAccountNonLocked());
        $this->assertTrue($user->isCredentialsNonExpired());
        $this->assertTrue($user->isEnabled());
    }

    public function testLoadingDisabledSuccess()
    {
        $manager = $this->getCrudRepositoryUserDetailsManager();
        $user = $manager->mapUser(array('username'=>'foo','password'=>'fooPass','authorities'=>array('ROLE_ADMIN'),
            'disabled'=>1,'accountExpirationDate'=>1,'lastPasswordChangeDate'=>1,'lockExpirationDate'=>time()+1000));
        $manager->createUser($user);
        $this->assertInstanceOf('MongoDB\BSON\ObjectId',$user->getId());
        $id = $user->getId();

        $user = $manager->loadUserByUsername('foo');
        $this->assertEquals('foo',$user->getUsername());
        $this->assertEquals('fooPass',$user->getPassword());
        $this->assertEquals(array('ROLE_ADMIN'),$user->getAuthorities());
        $this->assertFalse($user->isAccountNonExpired());
        $this->assertFalse($user->isAccountNonLocked());
        $this->assertFalse($user->isCredentialsNonExpired());
        $this->assertFalse($user->isEnabled());
    }

    public function testAccountNonExpiredWithExpiredDate()
    {
        $manager = $this->getCrudRepositoryUserDetailsManager();
        $user = $manager->mapUser(array('username'=>'foo','password'=>'fooPass','authorities'=>array('ROLE_ADMIN'),
            'accountExpirationDate'=>time()-1));
        $manager->createUser($user);
        $this->assertInstanceOf('MongoDB\BSON\ObjectId',$user->getId());
        $id = $user->getId();

        $user = $manager->loadUserByUsername('foo');
        $this->assertEquals('foo',$user->getUsername());
        $this->assertEquals('fooPass',$user->getPassword());
        $this->assertEquals(array('ROLE_ADMIN'),$user->getAuthorities());
        $this->assertFalse($user->isAccountNonExpired());
        $this->assertTrue($user->isAccountNonLocked());
        $this->assertTrue($user->isCredentialsNonExpired());
        $this->assertTrue($user->isEnabled());
    }

    public function testAccountNonExpiredWithZero()
    {
        $manager = $this->getCrudRepositoryUserDetailsManager();
        $user = $manager->mapUser(array('username'=>'foo','password'=>'fooPass','authorities'=>array('ROLE_ADMIN'),
            'accountExpirationDate'=>0));
        $manager->createUser($user);
        $this->assertInstanceOf('MongoDB\BSON\ObjectId',$user->getId());
        $id = $user->getId();

        $user = $manager->loadUserByUsername('foo');
        $this->assertEquals('foo',$user->getUsername());
        $this->assertEquals('fooPass',$user->getPassword());
        $this->assertEquals(array('ROLE_ADMIN'),$user->getAuthorities());
        $this->assertTrue($user->isAccountNonExpired());
        $this->assertTrue($user->isAccountNonLocked());
        $this->assertTrue($user->isCredentialsNonExpired());
        $this->assertTrue($user->isEnabled());
    }

    public function testAccountNonExpiredWithNonExpiredDate()
    {
        $manager = $this->getCrudRepositoryUserDetailsManager();
        $user = $manager->mapUser(array('username'=>'foo','password'=>'fooPass','authorities'=>array('ROLE_ADMIN'),
            'accountExpirationDate'=>time()+1000));
        $manager->createUser($user);
        $this->assertInstanceOf('MongoDB\BSON\ObjectId',$user->getId());
        $id = $user->getId();

        $user = $manager->loadUserByUsername('foo');
        $this->assertEquals('foo',$user->getUsername());
        $this->assertEquals('fooPass',$user->getPassword());
        $this->assertEquals(array('ROLE_ADMIN'),$user->getAuthorities());
        $this->assertTrue($user->isAccountNonExpired());
        $this->assertTrue($user->isAccountNonLocked());
        $this->assertTrue($user->isCredentialsNonExpired());
        $this->assertTrue($user->isEnabled());
    }

    public function testAccountNonLockedUntilExpireDate()
    {
        $manager = $this->getCrudRepositoryUserDetailsManager();
        $user = $manager->mapUser(array('username'=>'foo','password'=>'fooPass','authorities'=>array('ROLE_ADMIN'),
            'lockExpirationDate'=>(time()+1000)));
        $manager->createUser($user);
        $this->assertInstanceOf('MongoDB\BSON\ObjectId',$user->getId());
        $id = $user->getId();

        $user = $manager->loadUserByUsername('foo');
        $this->assertEquals('foo',$user->getUsername());
        $this->assertEquals('fooPass',$user->getPassword());
        $this->assertEquals(array('ROLE_ADMIN'),$user->getAuthorities());
        $this->assertTrue($user->isAccountNonExpired());
        $this->assertFalse($user->isAccountNonLocked());
        $this->assertTrue($user->isCredentialsNonExpired());
        $this->assertTrue($user->isEnabled());
    }

    public function testAccountNonLockedWithUnlockedDate()
    {
        $manager = $this->getCrudRepositoryUserDetailsManager();
        $user = $manager->mapUser(array('username'=>'foo','password'=>'fooPass','authorities'=>array('ROLE_ADMIN'),
            'lockExpirationDate'=>(time()-10)));
        $manager->createUser($user);
        $this->assertInstanceOf('MongoDB\BSON\ObjectId',$user->getId());
        $id = $user->getId();

        $user = $manager->loadUserByUsername('foo');
        $this->assertEquals('foo',$user->getUsername());
        $this->assertEquals('fooPass',$user->getPassword());
        $this->assertEquals(array('ROLE_ADMIN'),$user->getAuthorities());
        $this->assertTrue($user->isAccountNonExpired());
        $this->assertTrue($user->isAccountNonLocked());
        $this->assertTrue($user->isCredentialsNonExpired());
        $this->assertTrue($user->isEnabled());
    }

    public function testCredentialsNonExpiredWithExpiredDate()
    {
        $manager = $this->getCrudRepositoryUserDetailsManager();
        $user = $manager->mapUser(array('username'=>'foo','password'=>'fooPass','authorities'=>array('ROLE_ADMIN'),
            'lastPasswordChangeDate'=>time()-86400-1));
        $manager->createUser($user);
        $this->assertInstanceOf('MongoDB\BSON\ObjectId',$user->getId());
        $id = $user->getId();

        $user = $manager->loadUserByUsername('foo');
        $this->assertEquals('foo',$user->getUsername());
        $this->assertEquals('fooPass',$user->getPassword());
        $this->assertEquals(array('ROLE_ADMIN'),$user->getAuthorities());
        $this->assertTrue($user->isAccountNonExpired());
        $this->assertTrue($user->isAccountNonLocked());
        $this->assertFalse($user->isCredentialsNonExpired());
        $this->assertTrue($user->isEnabled());
    }

    public function testCredentialsNonExpiredWithNonExpiredDate()
    {
        $manager = $this->getCrudRepositoryUserDetailsManager();
        $user = $manager->mapUser(array('username'=>'foo','password'=>'fooPass','authorities'=>array('ROLE_ADMIN'),
            'lastPasswordChangeDate'=>time()));
        $manager->createUser($user);
        $this->assertInstanceOf('MongoDB\BSON\ObjectId',$user->getId());
        $id = $user->getId();

        $user = $manager->loadUserByUsername('foo');
        $this->assertEquals('foo',$user->getUsername());
        $this->assertEquals('fooPass',$user->getPassword());
        $this->assertEquals(array('ROLE_ADMIN'),$user->getAuthorities());
        $this->assertTrue($user->isAccountNonExpired());
        $this->assertTrue($user->isAccountNonLocked());
        $this->assertTrue($user->isCredentialsNonExpired());
        $this->assertTrue($user->isEnabled());
    }

    public function testDisabled()
    {
        $manager = $this->getCrudRepositoryUserDetailsManager();
        $user = $manager->mapUser(array('username'=>'foo','password'=>'fooPass','authorities'=>array('ROLE_ADMIN'),
            'disabled'=>1));
        $manager->createUser($user);
        $this->assertInstanceOf('MongoDB\BSON\ObjectId',$user->getId());
        $id = $user->getId();

        $user = $manager->loadUserByUsername('foo');
        $this->assertEquals('foo',$user->getUsername());
        $this->assertEquals('fooPass',$user->getPassword());
        $this->assertEquals(array('ROLE_ADMIN'),$user->getAuthorities());
        $this->assertTrue($user->isAccountNonExpired());
        $this->assertTrue($user->isAccountNonLocked());
        $this->assertTrue($user->isCredentialsNonExpired());
        $this->assertFalse($user->isEnabled());
    }

    /**
     * @expectedException        Rindow\Security\Core\Authentication\Exception\UsernameNotFoundException
     * @expectedExceptionMessage foo
     */
    public function testUsernameNotFoundException()
    {
        $manager = $this->getCrudRepositoryUserDetailsManager();
        $user = $manager->loadUserByUsername('foo');
    }

    public function testUpdateSuccess()
    {
        $manager = $this->getCrudRepositoryUserDetailsManager();
        $user = new User('foo','fooPass',array('ROLE_ADMIN'));
        $manager->createUser($user);
        $this->assertInstanceOf('MongoDB\BSON\ObjectId',$user->getId());
        $id = $user->getId();
        $user2 = new User('foo2','fooPass2',array('ROLE_ADMIN'));
        $manager->createUser($user2);
        $this->assertInstanceOf('MongoDB\BSON\ObjectId',$user->getId());
        $id2 = $user->getId();

        $user = $manager->loadUserByUsername('foo');
        $this->assertEquals('foo',$user->getUsername());
        $this->assertEquals('fooPass',$user->getPassword());
        $this->assertEquals(array('ROLE_ADMIN'),$user->getAuthorities());

        $user = new User('foo','boopass',array('ROLE_USER'));
        $manager->updateUser($user);
        $user = $manager->loadUserByUsername('foo');
        $this->assertEquals($id,$user->getId());
        $this->assertEquals('foo',$user->getUsername());
        $this->assertEquals('boopass',$user->getPassword());
        $this->assertEquals(array('ROLE_USER'),$user->getAuthorities());

        $user = new User('boo','boopass2',array('ROLE_OTHER'),$user->getId());
        $manager->updateUser($user);
        $user = $manager->loadUserByUsername('boo');
        $this->assertEquals($id,$user->getId());
        $this->assertEquals('boo',$user->getUsername());
        $this->assertEquals('boopass2',$user->getPassword());
        $this->assertEquals(array('ROLE_OTHER'),$user->getAuthorities());
    }

    /**
     * @expectedException        Rindow\Security\Core\Authentication\Exception\InvalidArgumentException
     * @expectedExceptionMessage unknown username or id:boo()
     */
    public function testUpdateFailed()
    {
        $manager = $this->getCrudRepositoryUserDetailsManager();
        $user = new User('foo','fooPass',array('ROLE_ADMIN'));
        $manager->createUser($user);
        $this->assertInstanceOf('MongoDB\BSON\ObjectId',$user->getId());
        $id = $user->getId();

        $user = $manager->loadUserByUsername('foo');
        $this->assertEquals('foo',$user->getUsername());
        $this->assertEquals('fooPass',$user->getPassword());
        $this->assertEquals(array('ROLE_ADMIN'),$user->getAuthorities());

        $user = new User('boo','boopass',array('ROLE_USER'));
        $manager->updateUser($user);
    }

    public function testCreateSuccess()
    {
        $manager = $this->getCrudRepositoryUserDetailsManager();
        $user = new User('foo','fooPass',array('ROLE_ADMIN'));
        $manager->createUser($user);
        $this->assertInstanceOf('MongoDB\BSON\ObjectId',$user->getId());
        $id = $user->getId();

        $this->assertFalse($manager->userExists('boo'));
        $user = new User('boo','boopass',array('ROLE_USER'));
        $manager->createUser($user);
        $this->assertInstanceOf('MongoDB\BSON\ObjectId',$user->getId());
        $id2 = $user->getId();
        $this->assertTrue($manager->userExists('boo'));

        $user = $manager->loadUserByUsername('boo');
        $this->assertEquals('boo',$user->getUsername());
        $this->assertEquals('boopass',$user->getPassword());
        $this->assertEquals(array('ROLE_USER'),$user->getAuthorities());
    }

    /**
     * @expectedException        Rindow\Security\Core\Authentication\Exception\DuplicateUsernameException
     * @expectedExceptionMessage duplicate username:boo
     */
    public function testCreateFailed()
    {
        $manager = $this->getCrudRepositoryUserDetailsManager();
        $user = new User('boo','fooPass',array('ROLE_ADMIN'));
        $manager->createUser($user);
        $this->assertInstanceOf('MongoDB\BSON\ObjectId',$user->getId());
        $id = $user->getId();

        $user = new User('boo','boopass',array('ROLE_USER'));
        $manager->createUser($user);
    }

    public function testDeleteSuccess()
    {
        $manager = $this->getCrudRepositoryUserDetailsManager();
        $user = new User('foo','fooPass',array('ROLE_ADMIN'));
        $manager->createUser($user);
        $this->assertInstanceOf('MongoDB\BSON\ObjectId',$user->getId());
        $id = $user->getId();

        $this->assertTrue($manager->userExists('foo'));
        $manager->deleteUser('foo');
        $this->assertFalse($manager->userExists('foo'));

        $this->assertFalse($manager->userExists('boo'));
        $manager->deleteUser('boo');
        $this->assertFalse($manager->userExists('boo'));
    }

    public function testLoadUser()
    {
        $manager = $this->getCrudRepositoryUserDetailsManager();
        $user = new User('foo','fooPass',array('ROLE_ADMIN'));
        $manager->createUser($user);
        $this->assertInstanceOf('MongoDB\BSON\ObjectId',$user->getId());
        $id = $user->getId();

        $user2 = $manager->loadUser($user->getId());
        $this->assertEquals($id,$user2->getId());
        $this->assertEquals('foo',$user2->getUsername());
        $this->assertEquals('fooPass',$user2->getPassword());
        $this->assertEquals(array('ROLE_ADMIN'),$user2->getAuthorities());
    }

}
