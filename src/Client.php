<?php
/**
 * // | Copyright (c)  2003-2019
 * // +----------------------------------------------------------------------
 * // | Author:   郝力辉 <163828@qq.com>
 * // +----------------------------------------------------------------------
 * // | Date:  19-6-14 下午11:21
 * // +----------------------------------------------------------------------
 * // | lastModified:   19-6-14 下午11:20
 * // +----------------------------------------------------------------------
 * // | v2  Client.php
 * // +----------------------------------------------------------------------
 */

namespace Nsqphp;

/**
 * Class Client
 */
class Client {
    protected $nsqdAddr = ["127.0.0.1:4150"];
    private static $nsq;

    /**
     * Client constructor.
     * @param array $config
     */
    public function __construct(array $config=[]){
        if (!empty($config)) {
            $this->nsqdAddr = array_merge($this->nsqdAddr,$config);
        }
        self::$nsq = new \Nsq();
        if (!self::$nsq->connectNsqd($this->nsqdAddr)) {
            app()->log->error('nsq服务失败');
        }
    }

    /**
     * @param string $topic
     * @param array $message
     * @param int $delayTime
     * @return bool
     */
    public static function push(string $topic, array $message,int $delayTime = 0){
        $message = json_encode($message);
        if (!self::$nsq) {
            app()->log->error('nsq入队失败'.$topic.' '.$message);
            return false;
        }
        // 延迟队列
        if ($delayTime > 0) {
            self::$nsq->deferredPublish($topic, $message, $delayTime);
        } else {
            self::$nsq->publish($topic, $message);
        }
        self::$nsq->closeNsqdConnection();
    }
}
