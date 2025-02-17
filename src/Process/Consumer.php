<?php

/**
 * This file is part of webman.
 *
 * Licensed under The MIT License
 * For full copyright and license information, please see the MIT-LICENSE.txt
 * Redistributions of files must retain the above copyright notice.
 *
 * @author    walkor<walkor@workerman.net>
 * @copyright walkor<walkor@workerman.net>
 * @link      http://www.workerman.net/
 * @license   http://www.opensource.org/licenses/mit-license.php MIT License
 */

namespace Thb\Redis\Process;

use support\Container;
use Thb\Redis\Client;

/**
 * Class Consumer
 * @package process
 */
class Consumer
{
    /**
     * @var string
     */
    protected $_consumerDir = '';

    /**
     * @var array
     */
    protected $_consumers = [];
    protected $_middlewaresArr = [];

    /**
     * StompConsumer constructor.
     * @param string $consumer_dir
     */
    public function __construct($consumer_dir = '', $middleware = [])
    {
        \support\Context::init();
        $this->_consumerDir = $consumer_dir;
        $this->_middlewaresArr = $middleware;
    }

    /**
     * onWorkerStart.
     */
    public function onWorkerStart()
    {
        if (!file_exists($this->_consumerDir)) {
            echo "Consumer directory {$this->_consumerDir} not exists\r\n";
            return false;
        }
        $fileinfo = new \SplFileInfo($this->_consumerDir);
        $ext = $fileinfo->getExtension();
        if ($ext === 'php') {
            $class = str_replace('/', "\\", substr(substr($this->_consumerDir, strlen(base_path())), 0, -4));
            if (is_a($class, 'Thb\Redis\Consumer', true)) {
                $consumer = Container::get($class);
                $connection_name = $consumer->connection ?? 'default';
                $queue = $consumer->queue;
                if (!$queue) {
                    echo "Consumer {$class} queue not exists\r\n";
                    return false;
                }
                $connection = Client::connection($connection_name);
                $middleware = config('plugin.thb.redis.redis.' . $connection_name . '.middleware', []);
                $middleware = array_merge($middleware, $this->_middlewaresArr);
                $connection->middleware = $middleware;
                $connection->subscribe($queue, [$consumer, 'consume']);
                if (method_exists($connection, 'onConsumeFailure')) {
                    $connection->onConsumeFailure(function ($exeption, $package) {
                        $consumer = $consumer ?? null;
                        if ($consumer && method_exists($consumer, 'onConsumeFailure')) {
                            return call_user_func([$consumer, 'onConsumeFailure'], $exeption, $package);
                        }
                    });
                }
            }
        }
    }
}
