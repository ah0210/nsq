<?php

namespace Nsqphp;

/**
 * Class Client
 */
class Client
{
    // 驱动
    protected $nsqdAddr = ["127.0.0.1:4150"];
    static $nsq;

    /**
     * 构造
     * @param array $config
     */
    public function __construct(array $config){
        $config = $config ? $config : $this->nsqdAddr;
        self::$nsq = new \Nsq();
        if (!self::$nsq->connectNsqd($config)) {
            app()->log->error('nsq服务失败');
        }
    }

    /**
     * 增加任务
     * @param Message $message
     * @param int $delayTime
     * @param int $maxLifetime
     */
    public function push(string $topic, array $message,int $delayTime = 0){
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
