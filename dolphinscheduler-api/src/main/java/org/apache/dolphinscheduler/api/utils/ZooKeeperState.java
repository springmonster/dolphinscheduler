/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dolphinscheduler.api.utils;

import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.common.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Scanner;

/**
 * zookeeper state monitor
 */
public class ZooKeeperState {

    private static final Logger logger = LoggerFactory.getLogger(ZooKeeperState.class);

    private final String host;
    private final int port;

    private float minLatency = -1, avgLatency = -1, maxLatency = -1;
    private long received = -1;
    private long sent = -1;
    private int outStanding = -1;
    private long zxid = -1;
    private String mode = null;
    private int nodeCount = -1;
    private int watches = -1;
    private int connections = -1;

    private boolean healthFlag = false;

    public ZooKeeperState(String connectionString) {
        String host = connectionString.substring(0,
                connectionString.indexOf(':'));
        int port = Integer.parseInt(connectionString.substring(connectionString
                .indexOf(':') + 1));
        this.host = host;
        this.port = port;
    }

    /**
     * srvr
     * srvr命令和stat命令的功能一致，唯一的区别是srvr不会将客户端的连接情况输出，仅仅输出服务器的自身信息
     */
    public void getZookeeperInfo() {
        String content = cmd("srvr");
        if (StringUtils.isNotBlank(content)) {
            if (Constants.STRING_FALSE.equals(content)) {
                healthFlag = true;
            } else {
                try (Scanner scannerForStat = new Scanner(content)) {
                    while (scannerForStat.hasNext()) {
                        String line = scannerForStat.nextLine();
                        if (line.startsWith("Latency min/avg/max:")) {
                            String[] latencys = getStringValueFromLine(line).split("/");
                            minLatency = Float.parseFloat(latencys[0]);
                            avgLatency = Float.parseFloat(latencys[1]);
                            maxLatency = Float.parseFloat(latencys[2]);
                        } else if (line.startsWith("Received:")) {
                            received = Long.parseLong(getStringValueFromLine(line));
                        } else if (line.startsWith("Sent:")) {
                            sent = Long.parseLong(getStringValueFromLine(line));
                        } else if (line.startsWith("Outstanding:")) {
                            outStanding = Integer.parseInt(getStringValueFromLine(line));
                        } else if (line.startsWith("Zxid:")) {
                            zxid = Long.parseLong(getStringValueFromLine(line).substring(2), 16);
                        } else if (line.startsWith("Mode:")) {
                            mode = getStringValueFromLine(line);
                        } else if (line.startsWith("Node count:")) {
                            nodeCount = Integer.parseInt(getStringValueFromLine(line));
                        }
                    }
                }
            }

        }

        /**
         * wchs
         * wchs命令用于输出当前服务器上管理的Watcher的概要信息。
         */
        String wchsText = cmd("wchs");
        if (StringUtils.isNotBlank(wchsText)) {
            if (Constants.STRING_FALSE.equals(wchsText)) {
                healthFlag = true;
            } else {
                try (Scanner scannerForWchs = new Scanner(wchsText)) {
                    while (scannerForWchs.hasNext()) {
                        String line = scannerForWchs.nextLine();
                        if (line.startsWith("Total watches:")) {
                            watches = Integer.parseInt(getStringValueFromLine(line));
                        }
                    }
                }
            }
        }

        /**
         * cons
         * cons命令用于输出当前这台服务器上所有客户端连接的详细信息，包括每个客户端的客户端IP、会话ID和最后一次与服务器交互的操作类型等。
         */
        String consText = cmd("cons");
        if (StringUtils.isNotBlank(consText)) {
            if (Constants.STRING_FALSE.equals(consText)) {
                healthFlag = true;
            } else {
                Scanner scannerForCons = new Scanner(consText);
                if (StringUtils.isNotBlank(consText)) {
                    connections = 0;
                }
                while (scannerForCons.hasNext()) {
                    @SuppressWarnings("unused")
                    String line = scannerForCons.nextLine();
                    ++connections;
                }
                scannerForCons.close();
            }
        }
    }

    /**
     * ruok命令用于输出当前 ZooKeeper服务器是否正在运行。该命令的名字非常有趣，其协议正好是“Are you ok”。
     * 执行该命令后，如果当前 ZooKeeper服务器正在运行，那么返回“imok”，否则没有任何响应输出。
     * 请注意，ruok命令的输出仅仅只能表明当前服务器是否正在运行，
     * 准确的讲，只能说明2181端口打开着，同时四字命令执行流程正常，但是不能代表 ZooKeeper服务器是否运行正常。
     * 在很多时候，如果当前服务器无法正常处理客户端的读写请求，甚至已经无法和集群中的其他机器进行通信，ruok命令依然返回“imok”。
     * 因此，一般来说，该命令并不是一个特别有用的命令，他不能反映 ZooKeeper服务器的工作状态，想要更可靠的获取更多 ZooKeeper运行状态信息，
     * 可以使用下面马上要讲到的stat命令。
     *
     * @return
     */
    public boolean ruok() {
        return "imok\n".equals(cmd("ruok"));
    }


    private String getStringValueFromLine(String line) {
        return line.substring(line.indexOf(":") + 1, line.length()).replaceAll(
                " ", "").trim();
    }

    private class SendThread extends Thread {
        private String cmd;

        private String ret = Constants.STRING_FALSE;

        public SendThread(String cmd) {
            this.cmd = cmd;
        }

        @Override
        public void run() {
            try {
                ret = FourLetterWordMain.send4LetterWord(host, port, cmd);
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                return;
            }
        }

    }

    private String cmd(String cmd) {
        final int waitTimeout = 5;
        SendThread sendThread = new SendThread(cmd);
        sendThread.setName("FourLetterCmd:" + cmd);
        sendThread.start();
        try {
            sendThread.join(waitTimeout * 1000L);
            return sendThread.ret;
        } catch (InterruptedException e) {
            logger.error("send " + cmd + " to server " + host + ":" + port + " failed!", e);
        }
        return Constants.STRING_FALSE;
    }

    public Logger getLogger() {
        return logger;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public float getMinLatency() {
        return minLatency;
    }

    public float getAvgLatency() {
        return avgLatency;
    }

    public float getMaxLatency() {
        return maxLatency;
    }

    public long getReceived() {
        return received;
    }

    public long getSent() {
        return sent;
    }

    public int getOutStanding() {
        return outStanding;
    }

    public long getZxid() {
        return zxid;
    }

    public String getMode() {
        return mode;
    }

    public int getNodeCount() {
        return nodeCount;
    }

    public int getWatches() {
        return watches;
    }

    public int getConnections() {
        return connections;
    }

    public boolean isHealthFlag() {
        return healthFlag;
    }

    @Override
    public String toString() {
        return "ZooKeeperState [host=" + host + ", port=" + port
                + ", minLatency=" + minLatency + ", avgLatency=" + avgLatency
                + ", maxLatency=" + maxLatency + ", received=" + received
                + ", sent=" + sent + ", outStanding=" + outStanding + ", zxid="
                + zxid + ", mode=" + mode + ", nodeCount=" + nodeCount
                + ", watches=" + watches + ", connections="
                + connections + ",healthFlag=" + healthFlag + "]";
    }
}