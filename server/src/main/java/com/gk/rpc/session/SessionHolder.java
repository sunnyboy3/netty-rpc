/*
 *  Copyright 1999-2019 Seata.io Group.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.gk.rpc.session;

import com.gk.rpc.StoreMode;
import com.gk.rpc.constants.ConfigurationKeys;
import com.gk.rpc.exception.TransactionException;
import io.seata.StringUtils;
import io.seata.core.Configuration;
import io.seata.core.ConfigurationFactory;
import io.seata.exception.ShouldNeverHappenException;
import io.seata.loader.EnhancedServiceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;

import static com.gk.rpc.utils.Constants.*;

/**
 * The type Session holder.
 *
 * @author sharajava
 */
public class SessionHolder {

    private static final Logger LOGGER = LoggerFactory.getLogger(SessionHolder.class);

    /**
     * The constant CONFIG.
     */
    protected static final Configuration CONFIG = ConfigurationFactory.getInstance();
    /**
     * The constant ROOT_SESSION_MANAGER_NAME.
     */
    public static final String ROOT_SESSION_MANAGER_NAME = "root.data";
    /**
     * The constant ASYNC_COMMITTING_SESSION_MANAGER_NAME.
     */
    public static final String ASYNC_COMMITTING_SESSION_MANAGER_NAME = "async.commit.data";
    /**
     * The constant RETRY_COMMITTING_SESSION_MANAGER_NAME.
     */
    public static final String RETRY_COMMITTING_SESSION_MANAGER_NAME = "retry.commit.data";
    /**
     * The constant RETRY_ROLLBACKING_SESSION_MANAGER_NAME.
     */
    public static final String RETRY_ROLLBACKING_SESSION_MANAGER_NAME = "retry.rollback.data";

    /**
     * The default session store dir
     */
    public static final String DEFAULT_SESSION_STORE_FILE_DIR = "sessionStore";

    private static SessionManager ROOT_SESSION_MANAGER;
    private static SessionManager ASYNC_COMMITTING_SESSION_MANAGER;
    private static SessionManager RETRY_COMMITTING_SESSION_MANAGER;
    private static SessionManager RETRY_ROLLBACKING_SESSION_MANAGER;

    /**
     * Init.
     *
     * @param mode the store mode: file, db
     * @throws IOException the io exception
     */
    public static void init(String mode) {
        if (StringUtils.isBlank(mode)) {
            //通过
            mode = CONFIG.getConfig(ConfigurationKeys.STORE_MODE);
        }
        StoreMode storeMode = StoreMode.get(mode);
        if (StoreMode.REDIS.equals(storeMode)) {
            ROOT_SESSION_MANAGER = EnhancedServiceLoader.load(SessionManager.class, StoreMode.REDIS.getName());
            ASYNC_COMMITTING_SESSION_MANAGER = EnhancedServiceLoader.load(SessionManager.class,
                StoreMode.REDIS.getName(), new Object[] {ASYNC_COMMITTING_SESSION_MANAGER_NAME});
            RETRY_COMMITTING_SESSION_MANAGER = EnhancedServiceLoader.load(SessionManager.class,
                StoreMode.REDIS.getName(), new Object[] {RETRY_COMMITTING_SESSION_MANAGER_NAME});
            RETRY_ROLLBACKING_SESSION_MANAGER = EnhancedServiceLoader.load(SessionManager.class,
                StoreMode.REDIS.getName(), new Object[] {RETRY_ROLLBACKING_SESSION_MANAGER_NAME});
        } else {
            // unknown store
            throw new IllegalArgumentException("unknown store mode:" + mode);
        }
    }



    private static void removeInErrorState(GlobalSession globalSession) {
        try {
            LOGGER.warn("The global session should NOT be {}, remove it. xid = {}", globalSession.getStatus(), globalSession.getXid());
            ROOT_SESSION_MANAGER.removeGlobalSession(globalSession);
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Remove global session succeed, xid = {}, status = {}", globalSession.getXid(), globalSession.getStatus());
            }
        } catch (Exception e) {
            LOGGER.error("Remove global session failed, xid = {}, status = {}", globalSession.getXid(), globalSession.getStatus(), e);
        }
    }

    private static void queueToAsyncCommitting(GlobalSession globalSession) {
        try {
            globalSession.addSessionLifecycleListener(getAsyncCommittingSessionManager());
            getAsyncCommittingSessionManager().addGlobalSession(globalSession);
        } catch (TransactionException e) {
            throw new ShouldNeverHappenException(e);
        }
    }

    private static void lockBranchSessions(ArrayList<BranchSession> branchSessions) {
        branchSessions.forEach(branchSession -> {
            try {
                branchSession.lock();
            } catch (TransactionException e) {
                throw new ShouldNeverHappenException(e);
            }
        });
    }

    private static void queueToRetryCommit(GlobalSession globalSession) {
        try {
            globalSession.addSessionLifecycleListener(getRetryCommittingSessionManager());
            getRetryCommittingSessionManager().addGlobalSession(globalSession);
        } catch (TransactionException e) {
            throw new ShouldNeverHappenException(e);
        }
    }

    private static void queueToRetryRollback(GlobalSession globalSession) {
        try {
            globalSession.addSessionLifecycleListener(getRetryRollbackingSessionManager());
            getRetryRollbackingSessionManager().addGlobalSession(globalSession);
        } catch (TransactionException e) {
            throw new ShouldNeverHappenException(e);
        }
    }

    //endregion

    //region get session manager

    /**
     * Gets root session manager.
     *
     * @return the root session manager
     */
    public static SessionManager getRootSessionManager() {
        if (ROOT_SESSION_MANAGER == null) {
            throw new ShouldNeverHappenException("SessionManager is NOT init!");
        }
        return ROOT_SESSION_MANAGER;
    }

    /**
     * Gets async committing session manager.
     *
     * @return the async committing session manager
     */
    public static SessionManager getAsyncCommittingSessionManager() {
        if (ASYNC_COMMITTING_SESSION_MANAGER == null) {
            throw new ShouldNeverHappenException("SessionManager is NOT init!");
        }
        return ASYNC_COMMITTING_SESSION_MANAGER;
    }

    /**
     * Gets retry committing session manager.
     *
     * @return the retry committing session manager
     */
    public static SessionManager getRetryCommittingSessionManager() {
        if (RETRY_COMMITTING_SESSION_MANAGER == null) {
            throw new ShouldNeverHappenException("SessionManager is NOT init!");
        }
        return RETRY_COMMITTING_SESSION_MANAGER;
    }

    /**
     * Gets retry rollbacking session manager.
     *
     * @return the retry rollbacking session manager
     */
    public static SessionManager getRetryRollbackingSessionManager() {
        if (RETRY_ROLLBACKING_SESSION_MANAGER == null) {
            throw new ShouldNeverHappenException("SessionManager is NOT init!");
        }
        return RETRY_ROLLBACKING_SESSION_MANAGER;
    }

    //endregion

    /**
     * Find global session.
     *
     * @param xid the xid
     * @return the global session
     */
    public static GlobalSession findGlobalSession(String xid) {
        return findGlobalSession(xid, true);
    }

    /**
     * Find global session.
     *
     * @param xid                the xid
     * @param withBranchSessions the withBranchSessions
     * @return the global session
     */
    public static GlobalSession findGlobalSession(String xid, boolean withBranchSessions) {
        return getRootSessionManager().findGlobalSession(xid, withBranchSessions);
    }

    /**
     * lock and execute
     *
     * @param globalSession the global session
     * @param lockCallable the lock Callable
     * @return the value
     */
    public static <T> T lockAndExecute(GlobalSession globalSession, GlobalSession.LockCallable<T> lockCallable)
            throws TransactionException {
        return getRootSessionManager().lockAndExecute(globalSession, lockCallable);
    }

    /**
     * retry rollbacking lock
     *
     * @return the boolean
     */
    public static boolean retryRollbackingLock() {
        return getRootSessionManager().scheduledLock(RETRY_ROLLBACKING);
    }

    /**
     * retry committing lock
     *
     * @return the boolean
     */
    public static boolean retryCommittingLock() {
        return getRootSessionManager().scheduledLock(RETRY_COMMITTING);
    }

    /**
     * async committing lock
     *
     * @return the boolean
     */
    public static boolean asyncCommittingLock() {
        return getRootSessionManager().scheduledLock(ASYNC_COMMITTING);
    }

    /**
     * tx timeout check lOck
     *
     * @return the boolean
     */
    public static boolean txTimeoutCheckLock() {
        return getRootSessionManager().scheduledLock(TX_TIMEOUT_CHECK);
    }

    /**
     * undolog delete lock
     *
     * @return the boolean
     */
    public static boolean undoLogDeleteLock() {
        return getRootSessionManager().scheduledLock(UNDOLOG_DELETE);
    }

    /**
     * un retry rollbacking lock
     *
     * @return the boolean
     */
    public static boolean unRetryRollbackingLock() {
        return getRootSessionManager().unScheduledLock(RETRY_ROLLBACKING);
    }

    /**
     * un retry committing lock
     *
     * @return the boolean
     */
    public static boolean unRetryCommittingLock() {
        return getRootSessionManager().unScheduledLock(RETRY_COMMITTING);
    }

    /**
     * un async committing lock
     *
     * @return the boolean
     */
    public static boolean unAsyncCommittingLock() {
        return getRootSessionManager().unScheduledLock(ASYNC_COMMITTING);
    }

    /**
     * un tx timeout check lOck
     *
     * @return the boolean
     */
    public static boolean unTxTimeoutCheckLock() {
        return getRootSessionManager().unScheduledLock(TX_TIMEOUT_CHECK);
    }

    /**
     * un undolog delete lock
     *
     * @return the boolean
     */
    public static boolean unUndoLogDeleteLock() {
        return getRootSessionManager().unScheduledLock(UNDOLOG_DELETE);
    }

    public static void destroy() {
        if (ROOT_SESSION_MANAGER != null) {
            ROOT_SESSION_MANAGER.destroy();
        }
        if (ASYNC_COMMITTING_SESSION_MANAGER != null) {
            ASYNC_COMMITTING_SESSION_MANAGER.destroy();
        }
        if (RETRY_COMMITTING_SESSION_MANAGER != null) {
            RETRY_COMMITTING_SESSION_MANAGER.destroy();
        }
        if (RETRY_ROLLBACKING_SESSION_MANAGER != null) {
            RETRY_ROLLBACKING_SESSION_MANAGER.destroy();
        }
    }
}
