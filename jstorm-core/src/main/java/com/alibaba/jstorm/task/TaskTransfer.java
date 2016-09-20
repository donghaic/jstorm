/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.jstorm.task;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import backtype.storm.tuple.TupleImplExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.callback.AsyncLoopRunnable;
import com.alibaba.jstorm.callback.AsyncLoopThread;
import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.common.metric.AsmGauge;
import com.alibaba.jstorm.common.metric.AsmHistogram;
import com.alibaba.jstorm.common.metric.QueueGauge;
import com.alibaba.jstorm.daemon.worker.WorkerData;
import com.alibaba.jstorm.metric.JStormHealthCheck;
import com.alibaba.jstorm.metric.JStormMetrics;
import com.alibaba.jstorm.metric.MetricDef;
import com.alibaba.jstorm.metric.MetricType;
import com.alibaba.jstorm.metric.MetricUtils;
import com.alibaba.jstorm.task.backpressure.BackpressureController;
import com.alibaba.jstorm.utils.JStormUtils;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;

import backtype.storm.Config;
import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.TaskMessage;
import backtype.storm.scheduler.WorkerSlot;
import backtype.storm.serialization.KryoTupleSerializer;
import backtype.storm.tuple.BatchTuple;
import backtype.storm.tuple.ITupleExt;
import backtype.storm.tuple.TupleExt;
import backtype.storm.utils.DisruptorQueue;
import backtype.storm.utils.Utils;
import backtype.storm.utils.WorkerClassLoader;

/**
 * Sending entrance
 * <p/>
 * Task sending all tuples through this Object
 * <p/>
 * Serialize the Tuple and put the serialized data to the sending queue
 * 消息发送的入口
 * Task通过这个对象发送所有tuples
 *
 * @author yannian
 */
public class TaskTransfer {

    private static Logger LOG = LoggerFactory.getLogger(TaskTransfer.class);

    protected Map storm_conf;
    protected DisruptorQueue transferControlQueue;
    protected KryoTupleSerializer serializer;
    protected Map<Integer, DisruptorQueue> innerTaskTransfer;
    protected Map<Integer, DisruptorQueue> controlQueues;
    protected DisruptorQueue serializeQueue;
    protected final AsyncLoopThread serializeThread;
    protected volatile TaskStatus taskStatus;
    protected String taskName;
    protected AsmHistogram serializeTimer;
    protected Task task;
    protected String topolgyId;
    protected String componentId;
    protected int taskId;

    protected ConcurrentHashMap<WorkerSlot, IConnection> nodeportSocket;
    protected ConcurrentHashMap<Integer, WorkerSlot> taskNodeport;

    protected BackpressureController backpressureController;

    protected boolean isTopologyMaster;

    public TaskTransfer(Task task, String taskName, KryoTupleSerializer serializer, TaskStatus taskStatus, WorkerData workerData) {
        this.task = task;
        this.taskName = taskName;
        this.serializer = serializer;
        this.taskStatus = taskStatus;
        this.storm_conf = workerData.getStormConf();
        this.transferControlQueue = workerData.getTransferCtrlQueue();
        this.innerTaskTransfer = workerData.getInnerTaskTransfer();
        this.controlQueues = workerData.getControlQueues();

        this.nodeportSocket = workerData.getNodeportSocket();
        this.taskNodeport = workerData.getTaskNodeport();

        this.topolgyId = workerData.getTopologyId();
        this.componentId = this.task.getComponentId();
        this.taskId = this.task.getTaskId();

        int queue_size = Utils.getInt(storm_conf.get(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE));
        WaitStrategy waitStrategy = (WaitStrategy) JStormUtils.createDisruptorWaitStrategy(storm_conf);
        this.serializeQueue = DisruptorQueue.mkInstance(taskName, ProducerType.MULTI, queue_size, waitStrategy);
        //this.serializeQueue.consumerStarted();

        String taskId = taskName.substring(taskName.indexOf(":") + 1);
        QueueGauge serializeQueueGauge = new QueueGauge(serializeQueue, taskName, MetricDef.SERIALIZE_QUEUE);
        JStormMetrics.registerTaskMetric(MetricUtils.taskMetricName(topolgyId, componentId, this.taskId, MetricDef.SERIALIZE_QUEUE, MetricType.GAUGE),
                new AsmGauge(serializeQueueGauge));
        JStormHealthCheck.registerTaskHealthCheck(Integer.valueOf(taskId), MetricDef.SERIALIZE_QUEUE, serializeQueueGauge);
        serializeTimer = (AsmHistogram) JStormMetrics.registerTaskMetric(MetricUtils.taskMetricName(
                topolgyId, componentId, this.taskId, MetricDef.SERIALIZE_TIME, MetricType.HISTOGRAM), new AsmHistogram());

        serializeThread = setupSerializeThread();

        backpressureController = new BackpressureController(storm_conf, task.getTaskId(), serializeQueue, queue_size);
        isTopologyMaster = (task.getTopologyContext().getTopologyMasterId() == task.getTaskId());

        LOG.info("Successfully start TaskTransfer thread");

    }

    public void transfer(TupleExt tuple) {

        int taskId = tuple.getTargetTaskId();

        //如果tuple对应task是要本地executor则exeQueue不为空，直接能DisruptorQueue进行消息投递
        DisruptorQueue exeQueue = innerTaskTransfer.get(taskId);
        if (exeQueue != null) {
            exeQueue.publish(tuple);
        } else {
            // taskId对应的Task不在本Worker内，则要通过Netty将消息投递到其他Worker
            push(taskId, tuple);
        }

        if (backpressureController.isBackpressureMode()) {
            backpressureController.flowControl();
        }
    }

    public void transferControl(TupleExt tuple) {

        int taskId = tuple.getTargetTaskId();

        DisruptorQueue controlQueue = controlQueues.get(taskId);
        if (controlQueue != null) {
            controlQueue.publish(tuple);
        } else {
            transferControlQueue.publish(tuple);
        }
    }

    public void transferControl(BatchTuple batch) {
        LOG.error("It is not allowed to send batch here!");
    }
    public void transfer(BatchTuple batch) {
        LOG.error("It is not allowed to send batch here!");
    }

    public void push(int taskId, TupleExt tuple) {
        serializeQueue.publish(tuple);
    }

    protected AsyncLoopThread setupSerializeThread() {
        return new AsyncLoopThread(new TransferRunnable());
    }

    public AsyncLoopThread getSerializeThread() {
        return serializeThread;
    }

    public BackpressureController getBackpressureController() {
        return backpressureController;
    }


    /**
     * 消费serializeQueue内的消息，将其序列化，通过网络（IConnection）发送到其他Worker处理
     */
    protected class TransferRunnable extends RunnableCallback implements EventHandler {

        private AtomicBoolean shutdown = AsyncLoopRunnable.getShutdown();

        @Override
        public String getThreadName() {
            return taskName + "-" + TransferRunnable.class.getSimpleName();
        }


        @Override
        public void preRun() {
            WorkerClassLoader.switchThreadContext();
        }

        @Override
        public void run() {
            while (!shutdown.get()) {
                serializeQueue.consumeBatchWhenAvailable(this);
            }
        }

        @Override
        public void postRun() {
            WorkerClassLoader.restoreThreadContext();
        }

        public byte[] serialize(ITupleExt tuple) {
            return serializer.serialize((TupleExt) tuple);
        }

        @Override
        public void onEvent(Object event, long sequence, boolean endOfBatch) throws Exception {
            if (event == null) {
                return;
            }

            long start = serializeTimer.getTime();
            try {
                ITupleExt tuple = (ITupleExt) event;
                int taskid = tuple.getTargetTaskId();
                //通过taskId找到task所在的Worker的地址<host:port>,再根据地址找到Connection，序列化后进行投递
                IConnection conn = getConnection(taskid);
                if (conn != null) {
                	byte[] tupleMessage = serialize(tuple);
                    TaskMessage taskMessage = new TaskMessage(taskid, tupleMessage);
                    conn.send(taskMessage);
                }
            } finally {
                serializeTimer.updateTime(start);
            }

        }

        protected IConnection getConnection(int taskId) {
            IConnection conn = null;
            WorkerSlot nodePort = taskNodeport.get(taskId);
            if (nodePort == null) {
                String errormsg = "IConnection to " + taskId + " can't be found";
                LOG.warn("Internal transfer warn, throw tuple,", new Exception(errormsg));
            } else {
                conn = nodeportSocket.get(nodePort);
                if (conn == null) {
                    String errormsg = "NodePort to" + nodePort + " can't be found";
                    LOG.warn("Internal transfer warn, throw tuple,", new Exception(errormsg));
                }
            }
            return conn;
        }

    }

}
