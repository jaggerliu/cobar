/*
 * Copyright 1999-2012 Alibaba Group.
 *  
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  
 *      http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * (created at 2012-4-19)
 */
package com.alibaba.cobar.mysql.nio.handler;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.alibaba.cobar.CobarConfig;
import com.alibaba.cobar.CobarServer;
import com.alibaba.cobar.config.ErrorCode;
import com.alibaba.cobar.mysql.MySQLDataNode;
import com.alibaba.cobar.mysql.nio.MySQLConnection;
import com.alibaba.cobar.net.mysql.ErrorPacket;
import com.alibaba.cobar.net.mysql.OkPacket;
import com.alibaba.cobar.route.RouteResultsetNode;
import com.alibaba.cobar.server.ServerConnection;
import com.alibaba.cobar.server.session.NonBlockingSession;
import com.alibaba.cobar.util.StringUtil;

/**
 * @author <a href="mailto:shuo.qius@alibaba-inc.com">QIU Shuo</a>
 */
public class SingleNodeHandler implements ResponseHandler, Terminatable {
	private static final Logger LOGGER = Logger
			.getLogger(SingleNodeHandler.class);
    private final RouteResultsetNode route;
    private final NonBlockingSession session;
    private byte packetId;
    private volatile ByteBuffer buffer;
    private ReentrantLock lock = new ReentrantLock();
    private boolean isRunning;
    private Runnable terminateCallBack;

    public SingleNodeHandler(RouteResultsetNode route, NonBlockingSession session) {
        if (route == null) {
            throw new IllegalArgumentException("routeNode is null!");
        }
        if (session == null) {
            throw new IllegalArgumentException("session is null!");
        }
        this.session = session;
        this.route = route;
    }

    @Override
    public void terminate(Runnable callback) {
        boolean zeroReached = false;
        lock.lock();
        try {
            if (isRunning) {
                terminateCallBack = callback;
            } else {
                zeroReached = true;
            }
        } finally {
            lock.unlock();
        }
        if (zeroReached) {
            callback.run();
        }
    }

    private void endRunning() {
        Runnable callback = null;
        lock.lock();
        try {
            if (isRunning) {
                isRunning = false;
                callback = terminateCallBack;
                terminateCallBack = null;
            }
        } finally {
            lock.unlock();
        }
        if (callback != null) {
            callback.run();
        }
    }

    public void execute() throws Exception {
        lock.lock();
        try {
            this.isRunning = true;
            this.packetId = 0;
            this.buffer = session.getSource().allocate();
        } finally {
            lock.unlock();
        }
        final MySQLConnection conn = session.getTarget(route);
        if (conn == null) {
            CobarConfig conf = CobarServer.getInstance().getConfig();
            MySQLDataNode dn = conf.getDataNodes().get(route.getName());
            dn.getConnection(this, route);
        } else {
            conn.setRunning(true);
            session.getSource().getProcessor().getExecutor().execute(new Runnable() {
                @Override
                public void run() {
                    _execute(conn);
                }
            });
        }
    }

    @Override
    public void connectionAcquired(final MySQLConnection conn) {
        conn.setRunning(true);
        session.bindConnection(route, conn);
//        session.getSource().getProcessor().getExecutor().execute(new Runnable() {
//            @Override
//            public void run() {
                _execute(conn);
//            }
//        });
    }

    private void _execute(MySQLConnection conn) {
        if (session.closed()) {
            conn.setRunning(false);
            endRunning();
            session.clearConnections();
            return;
        }
        conn.setResponseHandler(this);
        try {
            conn.execute(route, session.getSource(), session.getSource().isAutocommit());
        } catch (UnsupportedEncodingException e1) {
            executeException(conn);
            return;
        }
    }

    private void executeException(MySQLConnection c) {
        		ErrorPacket err = new ErrorPacket();
		err.packetId = ++packetId;
		err.errno = ErrorCode.ER_YES;
		err.message = StringUtil.encode(
				"unknown backend charset: " + c.getCharset(), session
						.getSource().getCharset());

		this.backConnectionErr(err, c);
    }

    @Override
    public void connectionError(Throwable e, MySQLConnection conn) {
       		conn.setRunning(false);

        endRunning();
        ErrorPacket err = new ErrorPacket();
        err.packetId = ++packetId;
        err.errno = ErrorCode.ER_YES;
        err.message = StringUtil.encode(e.getMessage(), session.getSource().getCharset());
        ServerConnection source = session.getSource();
        source.write(err.write(allocBuffer(), source));
    }

    @Override
    public void errorResponse(byte[] data, MySQLConnection conn) {
        ErrorPacket err = new ErrorPacket();
		err.read(data);
		err.packetId = ++packetId;
		backConnectionErr(err, conn);
    }

	private void backConnectionErr(ErrorPacket errPkg, MySQLConnection conn) {
		conn.setRunning(false);
		endRunning();
		session.releaseConnectionIfSafe(conn, LOGGER.isDebugEnabled());
		ServerConnection source = session.getSource();
		source.setTxInterrupt();
		errPkg.write(source);
	}
    @Override
    public void okResponse(byte[] data, MySQLConnection conn) {
       		boolean executeResponse;
			try {
				executeResponse = conn.syncAndExcute();
				if (executeResponse) {
					conn.setRunning(false);
					session.releaseConnectionIfSafe(conn, LOGGER.isDebugEnabled());
					endRunning();
					ServerConnection source = session.getSource();
					OkPacket ok = new OkPacket();
					ok.read(data);

					// ok.packetId = ++packetId;// OK_PACKET
					source.setLastInsertId(ok.insertId);
					ok.write(source);

				}
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    }

    @Override
    public void rowEofResponse(byte[] eof, MySQLConnection conn) {
		ServerConnection source = session.getSource();
		conn.setRunning(false);
		conn.recordSql(source.getHost(), source.getSchema(),
				route.getStatement());
		session.releaseConnectionIfSafe(conn, LOGGER.isDebugEnabled());
		endRunning();
		eof[3] = ++packetId;
		buffer = source.writeToBuffer(eof, allocBuffer());
		source.write(buffer);
    }


	private ByteBuffer allocBuffer() {
		if (buffer == null) {
			buffer = session.getSource().allocate();
		}
		return buffer;
	}
    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, byte[] eof, MySQLConnection conn) {

		header[3] = ++packetId;
		ServerConnection source = session.getSource();
		buffer = source.writeToBuffer(header, allocBuffer());
		for (int i = 0, len = fields.size(); i < len; ++i) {
			byte[] field = fields.get(i);
			field[3] = ++packetId;
			buffer = source.writeToBuffer(field, buffer);
		}
		eof[3] = ++packetId;
		buffer = source.writeToBuffer(eof, buffer);

    }

    @Override
    public void rowResponse(byte[] row, MySQLConnection conn) {
        ++packetId;
        buffer = session.getSource().writeToBuffer(row, buffer);
    }


	public void connectionClose(MySQLConnection conn, String reason) {
		ErrorPacket err = new ErrorPacket();
		err.packetId = ++packetId;
		err.errno = ErrorCode.ER_YES;
		err.message = StringUtil.encode(reason, session.getSource()
				.getCharset());
		this.backConnectionErr(err, conn);

	}

	public void clearResources() {

	}
}
