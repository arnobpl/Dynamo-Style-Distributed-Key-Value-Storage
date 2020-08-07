package edu.buffalo.cse.cse486586.simpledynamo;

import android.annotation.SuppressLint;
import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Scanner;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@SuppressLint("DefaultLocale")
public class SimpleDynamoProvider extends ContentProvider {
    private static final int SERVER_PORT = 10000;
    private static final String[] REMOTE_PORTS = {"11108", "11112", "11116", "11120", "11124"};
    private static final String[] EMULATOR_IDS = {"5554", "5556", "5558", "5560", "5562"};
    private static String MY_PORT;
    private static int MY_PORT_INDEX;

    private static String MY_NODE_ID;
    private static final Map<String, String> NODE_ID_TO_PORT_MAP = new HashMap<String, String>(REMOTE_PORTS.length);
    private static final Map<String, String> PORT_TO_NODE_ID_MAP = new HashMap<String, String>(REMOTE_PORTS.length);
    private static final List<String> ALL_SORTED_NODE_LIST;

    private static final List<String> ACTIVE_NODE_LIST = Collections.synchronizedList(new ArrayList<String>(REMOTE_PORTS.length));
    private static String PREV_NODE_ID = "";
    private static String NEXT_NODE_ID = "";

    private static final AtomicBoolean ON_CREATE_CALLED = new AtomicBoolean(false);
    private static final AtomicBoolean ON_CREATE_FINISHED = new AtomicBoolean(false);

    private static final int REPLICATION_COUNT = 3;
    private static final int FAILURE_TOLERANT = 1;

    private static final BigInteger SHA1_ALL_VALUES = new BigInteger("10000000000000000000000000000000000000000", 16);

    private static final int TIMEOUT_MSEC = 1000;

    private static final AtomicInteger CLIENTS_INITIALIZED_COUNT = new AtomicInteger(0);
    private static final AtomicBoolean[] CLIENTS_INITIALIZED_LIST = new AtomicBoolean[REMOTE_PORTS.length];
    private static final AtomicBoolean[] SERVERS_INITIALIZED_LIST = new AtomicBoolean[REMOTE_PORTS.length];

    private static final List<String> RECOVERY_MESSAGE_FETCHED_FROM_CLIENT_LIST = new CopyOnWriteArrayList<String>();

    private static final Socket[] CLIENT_SOCKETS = new Socket[REMOTE_PORTS.length];
    private static final PrintWriter[] CLIENT_PRINT_WRITERS = new PrintWriter[REMOTE_PORTS.length];
    private static final Scanner[] CLIENT_SCANNERS = new Scanner[REMOTE_PORTS.length];
    private static final BlockingQueue<Socket> CLIENT_PENDING_SOCKETS = new LinkedBlockingQueue<Socket>();

    // TODO: Save REPLICATE_RESPONSE_MAP and COORDINATOR_RESPONSE_MAP into DB for handling failure
    private static final AtomicInteger REQUEST_NUMBER = new AtomicInteger(0);
    private static final ConcurrentMap<Integer, String> QUERY_RESPONSE_MAP = new ConcurrentHashMap<Integer, String>(); // key = requestNumber
    private static final ConcurrentMap<String, Item> INSERT_RESPONSE_MAP = new ConcurrentHashMap<String, Item>(); // key = itemKey
    private static final ConcurrentMap<String, Boolean> DELETE_RESPONSE_MAP = new ConcurrentHashMap<String, Boolean>(); // key = queryKey
    private static final Map<String, Map<Integer, String>> NODE_ID_TO_RECOVERY_DATA_LIST_MAP = new HashMap<String, Map<Integer, String>>(REMOTE_PORTS.length); // key = requestNumber

    private static final ConcurrentMap<Long, List<String>> VERSION_TO_OWN_RECOVERY_DATA_LIST_MAP = new ConcurrentHashMap<Long, List<String>>(); // key = version


    private static final int RESPONSE_CHECK_TIMEOUT_MSEC = 100;
    private static final int RESPONSE_CHECK_TIMEOUT_RETRY = 20;

    private static final String HEADER_MY_PORT = "MY_PORT"; // MY_PORT <MY_PORT>
    private static final String HEADER_INSERT = "INSERT"; // INSERT <Requester_Node_ID> <Key> <Value> <Version>
    private static final String HEADER_INSERT_CONFIRM = "INSERT_CONFIRM"; // INSERT_CONFIRM <Requester_Node_ID> <Key> <Value> <Version>
    private static final String HEADER_QUERY_REQUEST = "QUERY_REQ"; // QUERY_REQ <Requester_Node_ID> <Request_Number> <Query_Key>
    private static final String HEADER_QUERY_RETURN = "QUERY_RET"; // QUERY_RET <Requester_Node_ID> <Request_Number> <Query_Key> <Count> <Key> <Value> ...
    private static final String HEADER_DELETE = "DELETE"; // DELETE <Requester_Node_ID> <Key> <Version>
    private static final String HEADER_DELETE_CONFIRM = "DELETE_CONFIRM"; // DELETE_CONFIRM <Requester_Node_ID> <Key>
    private static final String HEADER_REPLICATE_REQUEST = "REPLICATE_REQ"; // REPLICATE_REQ <Requester_Node_ID> <Request_Number> <Insert/Delete Message>
    private static final String HEADER_RECOVERY_REQUEST = "RECOVERY_REQ"; // RECOVERY_REQ <Requester_Node_ID> <Request_Number> <Insert/Delete Message>
    private static final String HEADER_REQUEST_CLEAR = "REQUEST_CLR"; // REQUEST_CLR <Requester_Node_ID> <Request_Number>
    private static final String HEADER_RECOVERY_DONE = "RECOVERY_DONE"; // RECOVERY_DONE <Requester_Node_ID>

    private static final String END_DATA_SYMBOL = "%";

    private static final String SELECTION_LOCAL_ALL = "@";
    private static final String SELECTION_GLOBAL_ALL = "*";

    private static final Uri URI = Uri.parse("content://edu.buffalo.cse.cse486586.simpledynamo.provider");
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";

    private static AppDatabase db = null;
    private static final String DB_TABLE_NAME = "item_table";
    private static final String DB_HASH_KEY_COL = "item_hash_key";
    private static final String DB_KEY_COL = "item_key";
    private static final String DB_VALUE_COL = "item_value";
    private static final String DB_VERSION_COL = "item_version";

    static final String TAG = SimpleDynamoProvider.class.getSimpleName();

    private final static Object MEMBERSHIP_LOCK = new Object();
    private final static Object RECOVERY_LOCK = new Object();

    static {
        List<String> allNodeList = new ArrayList<String>(REMOTE_PORTS.length);

        for (int i = 0; i < REMOTE_PORTS.length; i++) {
            String port = REMOTE_PORTS[i];
            String emulator_ID = EMULATOR_IDS[i];

            String node_ID = genHash(emulator_ID);
            NODE_ID_TO_PORT_MAP.put(node_ID, port);
            PORT_TO_NODE_ID_MAP.put(port, node_ID);

            allNodeList.add(node_ID);
        }

        Collections.sort(allNodeList);
        ALL_SORTED_NODE_LIST = Collections.unmodifiableList(allNodeList);

        for (int i = 0; i < REMOTE_PORTS.length; i++) {
            CLIENTS_INITIALIZED_LIST[i] = new AtomicBoolean(false);
            SERVERS_INITIALIZED_LIST[i] = new AtomicBoolean(false);
            NODE_ID_TO_RECOVERY_DATA_LIST_MAP.put(PORT_TO_NODE_ID_MAP.get(REMOTE_PORTS[i]), new ConcurrentHashMap<Integer, String>());
        }
    }

    @Override
    public boolean onCreate() {
        MyLog.v(TAG, "SimpleDynamoProvider: onCreate call");

        if (!ON_CREATE_CALLED.compareAndSet(false, true)) {
            MyLog.v(TAG, "SimpleDynamoProvider: onCreate call rejected as already called.");

            while (!ON_CREATE_FINISHED.get()) {
                try {
                    Thread.sleep(RESPONSE_CHECK_TIMEOUT_RETRY);
                } catch (InterruptedException e) {
                    MyLog.e(TAG, "Thread interrupted");
                    e.printStackTrace();
                }
            }

            return true;
        }

        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        MY_PORT = String.valueOf((Integer.parseInt(portStr) * 2));
        MyLog.d(TAG, "My_Port: " + MY_PORT);
        MY_NODE_ID = PORT_TO_NODE_ID_MAP.get(MY_PORT);
        MyLog.d(TAG, "My_Node_ID: " + MY_NODE_ID);
        MY_PORT_INDEX = Arrays.asList(REMOTE_PORTS).indexOf(MY_PORT);
        synchronized (MEMBERSHIP_LOCK) {
            CLIENTS_INITIALIZED_LIST[MY_PORT_INDEX].set(true);
            SERVERS_INITIALIZED_LIST[MY_PORT_INDEX].set(true);
            ACTIVE_NODE_LIST.add(MY_NODE_ID);
            CLIENTS_INITIALIZED_COUNT.getAndIncrement();
        }

        int myNodeIndex = ALL_SORTED_NODE_LIST.indexOf(MY_NODE_ID);
        int prevNodeIndex = mod(myNodeIndex - 1, ALL_SORTED_NODE_LIST.size());
        int nextNodeIndex = mod(myNodeIndex + 1, ALL_SORTED_NODE_LIST.size());
        PREV_NODE_ID = ALL_SORTED_NODE_LIST.get(prevNodeIndex);
        NEXT_NODE_ID = ALL_SORTED_NODE_LIST.get(nextNodeIndex);

        RECOVERY_MESSAGE_FETCHED_FROM_CLIENT_LIST.add(MY_NODE_ID);

        db = new AppDatabase(this.getContext());

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            MyLog.d(TAG, "ServerSocket has been created at port " + serverSocket.getLocalPort());
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

            Thread.sleep(TIMEOUT_MSEC);

            connectToDisconnectedClients();
        } catch (IOException e) {
            MyLog.e(TAG, "Can't create a ServerSocket");
            return false;
        } catch (InterruptedException e) {
            MyLog.e(TAG, "Thread interrupted");
            return false;
        }

        ON_CREATE_FINISHED.set(true);
        return true;
    }

    private static class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        @Override
        protected Void doInBackground(final ServerSocket... sockets) {
            final ServerSocket serverSocket = sockets[0];
            ThreadPoolExecutor multiClientThreadPoolExecutor = (ThreadPoolExecutor) Executors.newFixedThreadPool(REMOTE_PORTS.length * 2);
            multiClientThreadPoolExecutor.submit(new ServerAcceptThread(serverSocket));
            multiClientThreadPoolExecutor.submit(new ServerConnectionMaintainer());

            while (true) {
                try {
                    MyLog.d(TAG, "ServerTask: SERVERS_INITIALIZED_LIST: " + Arrays.toString(SERVERS_INITIALIZED_LIST));
                    multiClientThreadPoolExecutor.submit(new ServerDispatcherThread(CLIENT_PENDING_SOCKETS.take()));
                } catch (InterruptedException e) {
                    MyLog.e(TAG, "ServerTask socket InterruptedException at CLIENT_PENDING_SOCKETS's take");
                    e.printStackTrace();
                }
            }
        }

        private static class ServerAcceptThread implements Runnable {
            private final ServerSocket serverSocket;

            public ServerAcceptThread(ServerSocket serverSocket) {
                this.serverSocket = serverSocket;
            }

            @Override
            public void run() {
                while (true) {
                    try {
                        MyLog.v(TAG, "ServerTask trying to return serverSocket.accept()...");
                        Socket socket = serverSocket.accept();
                        CLIENT_PENDING_SOCKETS.put(socket);
                    } catch (IOException e) {
                        MyLog.e(TAG, "ServerTask socket IOException at socket accept");
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        MyLog.e(TAG, "ServerTask socket InterruptedException at CLIENT_PENDING_SOCKETS's put");
                        e.printStackTrace();
                    }
                }
            }
        }

        private static class ServerDispatcherThread implements Runnable {
            private final Socket socket;
            private final ThreadPoolExecutor perClientThreadPoolExecutor;
            private final BlockingQueue<String> perClientMesssageQueue;

            public ServerDispatcherThread(Socket socket) {
                this.socket = socket;
                perClientThreadPoolExecutor = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
                perClientMesssageQueue = new LinkedBlockingQueue<String>();
            }

            @Override
            public void run() {
                MyLog.d(TAG, "ServerSocket has accepted a connection at " + socket.getInetAddress().getHostAddress() + ":" + Integer.toString(socket.getPort()));

                String clientPort = "";
                String clientNodeId = "";
                try {
                    MyLog.v(TAG, "ServerTask trying to return socket.getInputStream()...");
                    Scanner scanner = new Scanner(socket.getInputStream());
                    MyLog.v(TAG, "ServerTask socket.getInputStream() return");

                    MyLog.v(TAG, "ServerTask trying to receive client port...");
                    clientPort = scanner.nextLine().trim().split(" ", 2)[1];
                    clientPort = clientPort.substring(0, clientPort.length() - 1);
                    MyLog.d(TAG, "ServerTask has got Client_Port: " + clientPort);
                    clientNodeId = PORT_TO_NODE_ID_MAP.get(clientPort);
                    MyLog.d(TAG, "ServerTask has got Client_Node_ID: " + clientNodeId);

                    synchronized (MEMBERSHIP_LOCK) {
                        int remotePortIndex = Arrays.asList(REMOTE_PORTS).indexOf(clientPort);
                        CLIENT_SOCKETS[remotePortIndex] = socket;
                        CLIENT_SCANNERS[remotePortIndex] = scanner;
                        SERVERS_INITIALIZED_LIST[remotePortIndex].set(true);

                        if (!ACTIVE_NODE_LIST.contains(clientNodeId)) {
                            ACTIVE_NODE_LIST.add(clientNodeId);
                        }

                        MyLog.d(TAG, String.format("ServerTask: Client added: Client_Port: %s; Client_Node_ID %s: CLIENTS_INITIALIZED_COUNT: %d; SERVERS_INITIALIZED_LIST: %s; ACTIVE_NODE_LIST: %s",
                                clientPort,
                                clientNodeId,
                                CLIENTS_INITIALIZED_COUNT.get(),
                                Arrays.toString(SERVERS_INITIALIZED_LIST),
                                Arrays.toString(ACTIVE_NODE_LIST.toArray())));
                    }

                    sendRecoveryMessageListToClient(clientNodeId);

                    while (true) {
                        MyLog.v(TAG, String.format("ServerTask: Client status: CLIENTS_INITIALIZED_COUNT: %d; SERVERS_INITIALIZED_LIST: %s; ACTIVE_NODE_LIST: %s",
                                CLIENTS_INITIALIZED_COUNT.get(),
                                Arrays.toString(SERVERS_INITIALIZED_LIST),
                                Arrays.toString(ACTIVE_NODE_LIST.toArray())));

                        MyLog.v(TAG, "ServerTask trying to receive message from Client_Port " + clientPort + "...");

                        StringBuilder dataReceivedBuilder = new StringBuilder(scanner.nextLine().trim());
                        while (!dataReceivedBuilder.toString().endsWith(END_DATA_SYMBOL)) {
                            dataReceivedBuilder.append(scanner.nextLine().trim());
                            MyLog.v(TAG, "ServerTask: DataReceived was split because of the low buffer size of Scanner");
                        }
                        dataReceivedBuilder.setLength(dataReceivedBuilder.length() - END_DATA_SYMBOL.length());
                        String dataReceived = dataReceivedBuilder.toString();

                        MyLog.v(TAG, "ServerTask received msgReceived from Client_Port " + clientPort + ": " + dataReceived.replace("\n", "\\n"));

                        String[] dataReceivedItems = dataReceived.split(" ", 2);
                        String header = dataReceivedItems[0];
                        String data = dataReceivedItems[1];

                        if (header.equals(HEADER_MY_PORT)) {
                            String[] dataItems = data.split(" ", 1);
                            clientPort = dataItems[0];
                            clientNodeId = PORT_TO_NODE_ID_MAP.get(clientPort);

                            synchronized (MEMBERSHIP_LOCK) {
                                int remotePortIndex = Arrays.asList(REMOTE_PORTS).indexOf(clientPort);

                                SERVERS_INITIALIZED_LIST[remotePortIndex].set(true);

                                if (!ACTIVE_NODE_LIST.contains(clientNodeId)) {
                                    ACTIVE_NODE_LIST.add(clientNodeId);
                                }

                                MyLog.d(TAG, String.format("ServerTask: Client refresh: CLIENTS_INITIALIZED_COUNT: %d; SERVERS_INITIALIZED_LIST: %s; ACTIVE_NODE_LIST: %s",
                                        CLIENTS_INITIALIZED_COUNT.get(),
                                        Arrays.toString(SERVERS_INITIALIZED_LIST),
                                        Arrays.toString(ACTIVE_NODE_LIST.toArray())));
                            }
                        } else if (header.equals(HEADER_INSERT_CONFIRM)) {
                            String[] dataItems = data.split(" ", 4);
                            String requesterNodeId = dataItems[0]; // unused
                            String itemKey = dataItems[1];
                            String itemValue = dataItems[2];
                            long itemVersion = Long.parseLong(dataItems[3]);

                            INSERT_RESPONSE_MAP.put(itemKey, new Item(itemKey, itemValue, itemVersion));

                            MyLog.v(TAG, String.format("ServerTask: INSERT_RESPONSE_MAP itemKey %s put for Client_Port %s",
                                    itemKey,
                                    clientPort));
                        } else if (header.equals(HEADER_DELETE_CONFIRM)) {
                            String[] dataItems = data.split(" ", 2);
                            String requesterNodeId = dataItems[0]; // unused
                            String itemKey = dataItems[1];

                            DELETE_RESPONSE_MAP.put(itemKey, true);

                            MyLog.v(TAG, String.format("ServerTask: DELETE_RESPONSE_MAP itemKey %s put for Client_Port %s",
                                    itemKey,
                                    clientPort));
                        } else if (header.equals(HEADER_QUERY_RETURN)) {
                            String[] dataItems = data.split(" ", 4);
                            String requesterNodeId = dataItems[0]; // unused
                            int requestNumber = Integer.parseInt(dataItems[1]);
                            String queryKey = dataItems[2];
                            String itemList = dataItems[3];

                            QUERY_RESPONSE_MAP.put(requestNumber, itemList);

                            MyLog.v(TAG, String.format("ServerTask: QUERY_RESPONSE_MAP queryKey %s, requestNumber %d put for Client_Port %s",
                                    queryKey,
                                    requestNumber,
                                    clientPort));
                        } else if (header.equals(HEADER_REQUEST_CLEAR)) {
                            String[] dataItems = data.split(" ", 2);
                            String requesterNodeId = dataItems[0];
                            String requestNumber = dataItems[1];

                            NODE_ID_TO_RECOVERY_DATA_LIST_MAP.get(requesterNodeId).remove(Integer.parseInt(requestNumber));
                        } else if (header.equals(HEADER_RECOVERY_REQUEST)) {
                            String[] dataItems = data.split(" ", 4);
                            String requesterNodeId = dataItems[0];
                            String requestNumber = dataItems[1];
                            String messageHeader = dataItems[2];
                            String message = dataItems[3];

                            long itemVersion = 0;
                            if (messageHeader.equals(HEADER_INSERT)) {
                                String[] messageItems = message.split(" ", 4);
                                itemVersion = Long.parseLong(messageItems[3]);
                            } else if (messageHeader.equals(HEADER_DELETE)) {
                                String[] messageItems = message.split(" ", 3);
                                itemVersion = Long.parseLong(messageItems[2]);
                            }

                            synchronized (RECOVERY_LOCK) {
                                VERSION_TO_OWN_RECOVERY_DATA_LIST_MAP.putIfAbsent(itemVersion, new CopyOnWriteArrayList<String>());
                                List<String> dataList = VERSION_TO_OWN_RECOVERY_DATA_LIST_MAP.get(itemVersion);
                                dataList.add(data);
                            }
                        } else if (header.equals(HEADER_RECOVERY_DONE)) {
                            String[] dataItems = data.split(" ", 1);
                            String requesterNodeId = dataItems[0];

                            if (!RECOVERY_MESSAGE_FETCHED_FROM_CLIENT_LIST.contains(requesterNodeId)) {
                                if (((RECOVERY_MESSAGE_FETCHED_FROM_CLIENT_LIST.size() + 1) == ACTIVE_NODE_LIST.size())
                                        && (ACTIVE_NODE_LIST.size() >= (REMOTE_PORTS.length - FAILURE_TOLERANT))) {
                                    synchronized (RECOVERY_LOCK) {
                                        MyLog.d(TAG, String.format("ServerTask: Recovery: Initiating recovery triggered by the last fetched Recovery_Done message from Client_Port %s",
                                                clientPort));

                                        List<Long> sortedItemVersionList = new ArrayList<Long>(VERSION_TO_OWN_RECOVERY_DATA_LIST_MAP.keySet());
                                        Collections.sort(sortedItemVersionList);

                                        MyLog.d(TAG, String.format("ServerTask: Recovery: Getting sortedItemVersionList triggered by Client_Port %s: %s",
                                                clientPort,
                                                sortedItemVersionList));

                                        while (!sortedItemVersionList.isEmpty()) {
                                            long itemVersion = sortedItemVersionList.get(0);
                                            List<String> dataList = VERSION_TO_OWN_RECOVERY_DATA_LIST_MAP.get(itemVersion);

                                            MyLog.d(TAG, String.format("ServerTask: Recovery: Getting dataList for itemVersion %d triggered by Client_Port %s: %s",
                                                    itemVersion,
                                                    clientPort,
                                                    toStringFromStringList(dataList)));

                                            for (String recoveryData : dataList) {
                                                MyLog.d(TAG, String.format("ServerTask: Recovery: Processing recovery data triggered by Client_Port %s: %s",
                                                        clientPort,
                                                        recoveryData));

                                                String[] recoveryDataItems = recoveryData.split(" ", 4);
                                                String recoveryRequesterNodeId = recoveryDataItems[0];
                                                String recoveryRequestNumber = recoveryDataItems[1];
                                                String recoveryMessageHeader = recoveryDataItems[2];
                                                String recoveryMessage = recoveryDataItems[3];

                                                if (recoveryMessageHeader.equals(HEADER_INSERT)) {
                                                    processInsert(recoveryMessageHeader + " " + recoveryMessage, true, true, true);

                                                    MyLog.d(TAG, String.format("ServerTask: Recovery: Calling recovery insert triggered by Client_Port %s: %s",
                                                            clientPort,
                                                            recoveryMessageHeader + " " + recoveryMessage));
                                                } else if (recoveryMessageHeader.equals(HEADER_DELETE)) {
                                                    processDelete(recoveryMessageHeader + " " + recoveryMessage, true, true, true);

                                                    MyLog.d(TAG, String.format("ServerTask: Recovery: Calling recovery delete triggered by Client_Port %s: %s",
                                                            clientPort,
                                                            recoveryMessageHeader + " " + recoveryMessage));
                                                }

                                                sendToOneByNodeId(recoveryRequesterNodeId,
                                                        String.format("%s %s %s",
                                                                HEADER_REQUEST_CLEAR,
                                                                recoveryRequesterNodeId,
                                                                recoveryRequestNumber));
                                            }

                                            VERSION_TO_OWN_RECOVERY_DATA_LIST_MAP.remove(itemVersion);
                                            sortedItemVersionList.remove(0);
                                        }

                                        MyLog.d(TAG, String.format("ServerTask: Recovery: all recovery messages processed triggered by Client_Port %s",
                                                clientPort));
                                    }
                                }

                                RECOVERY_MESSAGE_FETCHED_FROM_CLIENT_LIST.add(requesterNodeId);

                                MyLog.d(TAG, String.format("ServerTask: Recovery: Adding to RECOVERY_MESSAGE_FETCHED_FROM_CLIENT_LIST for Client_Port %s",
                                        clientPort));
                            }

                            MyLog.v(TAG, String.format("ServerTask: Recovery message receiving has been completed for Client_Port %s; RECOVERY_MESSAGE_FETCHED_FROM_CLIENT_LIST: %s",
                                    NODE_ID_TO_PORT_MAP.get(requesterNodeId),
                                    toStringFromStringList(RECOVERY_MESSAGE_FETCHED_FROM_CLIENT_LIST)));
                        } else {
                            perClientMesssageQueue.put(dataReceived);
                            perClientThreadPoolExecutor.submit(new ServerProcessingThread(perClientMesssageQueue, clientPort));
                        }
                    }
                } catch (IOException e) {
                    MyLog.e(TAG, "ServerTask socket IOException for Client_Port " + clientPort);
                    e.printStackTrace();
                } catch (IllegalStateException e) {
                    MyLog.e(TAG, "ServerTask socket IllegalStateException for Client_Port " + clientPort);
                    e.printStackTrace();
                } catch (NoSuchElementException e) {
                    MyLog.e(TAG, "ServerTask socket NoSuchElementException for Client_Port " + clientPort);
                    e.printStackTrace();
                } catch (Exception e) {
                    MyLog.e(TAG, "ServerTask socket Exception for Client_Port " + clientPort);
                    e.printStackTrace();
                }

                synchronized (MEMBERSHIP_LOCK) {
                    if (clientPort.isEmpty()) {
                        clientPort = Integer.toString(socket.getPort());

                        MyLog.d(TAG, "ServerTask: Exception MEMBERSHIP_LOCK: socket.getPort(): " + clientPort);
                    }

                    int remotePortIndex = Arrays.asList(REMOTE_PORTS).indexOf(clientPort);

                    if (CLIENTS_INITIALIZED_LIST[remotePortIndex].get()) {
                        CLIENTS_INITIALIZED_COUNT.getAndDecrement();
                    }
                    CLIENTS_INITIALIZED_LIST[remotePortIndex].set(false);
                    SERVERS_INITIALIZED_LIST[remotePortIndex].set(false);

                    ACTIVE_NODE_LIST.remove(clientNodeId);

                    MyLog.d(TAG, String.format("ServerTask: Client remove: Client_Port: %s; Client_Node_ID %s: CLIENTS_INITIALIZED_COUNT: %d; SERVERS_INITIALIZED_LIST: %s; ACTIVE_NODE_LIST: %s",
                            clientPort,
                            clientNodeId,
                            CLIENTS_INITIALIZED_COUNT.get(),
                            Arrays.toString(SERVERS_INITIALIZED_LIST),
                            Arrays.toString(ACTIVE_NODE_LIST.toArray())));

                }

                MyLog.v(TAG, "ServerTask doInBackground thread return");
            }
        }

        private static class ServerProcessingThread implements Runnable {
            private final BlockingQueue<String> messageQueue;
            private final String clientPort;

            public ServerProcessingThread(BlockingQueue<String> messageQueue, String clientPort) {
                this.messageQueue = messageQueue;
                this.clientPort = clientPort;
            }

            @Override
            public void run() {
                final String dataReceived;
                try {
                    dataReceived = messageQueue.take();
                } catch (InterruptedException e) {
                    MyLog.e(TAG, "ServerTask socket InterruptedException in MessageQueue for Client_Port " + clientPort);
                    e.printStackTrace();
                    return;
                }

                String[] dataReceivedItems = dataReceived.split(" ", 2);
                String header = dataReceivedItems[0];
                String data = dataReceivedItems[1];

                if (header.equals(HEADER_INSERT)) {
                    processInsert(header + " " + data, false, true, false);
                } else if (header.equals(HEADER_DELETE)) {
                    processDelete(header + " " + data, false, true, false);
                } else if (header.equals(HEADER_QUERY_REQUEST)) {
                    String[] dataItems = data.split(" ", 3);
                    String requesterNodeId = dataItems[0];
                    String requestNumber = dataItems[1];
                    String queryKey = dataItems[2];

                    processQuery(queryKey, requesterNodeId, requestNumber);
                } else if (header.equals(HEADER_REPLICATE_REQUEST)) {
                    String[] dataItems = data.split(" ", 4);
                    String requesterNodeId = dataItems[0];
                    String requestNumber = dataItems[1];
                    String messageHeader = dataItems[2];
                    String message = dataItems[3];

                    if (messageHeader.equals(HEADER_INSERT)) {
                        processInsert(messageHeader + " " + message, true, true, false);
                    } else if (messageHeader.equals(HEADER_DELETE)) {
                        processDelete(messageHeader + " " + message, true, true, false);
                    }

                    sendToOneByNodeId(requesterNodeId,
                            String.format("%s %s %s",
                                    HEADER_REQUEST_CLEAR,
                                    requesterNodeId,
                                    requestNumber));
                }
            }
        }

        private static class ServerConnectionMaintainer implements Runnable {
            @Override
            public void run() {
                while (true) {
                    if (ACTIVE_NODE_LIST.size() < REMOTE_PORTS.length) {
                        MyLog.v(TAG, "ServerConnectionMaintainer: Trying to reconnect all disconnected clients...");
                        connectToDisconnectedClients();

                        try {
                            Thread.sleep(TIMEOUT_MSEC);
                        } catch (InterruptedException e) {
                            MyLog.e(TAG, "Thread interrupted");
                            e.printStackTrace();
                        }
                    } else {
                        try {
                            Thread.sleep(RESPONSE_CHECK_TIMEOUT_RETRY);
                        } catch (InterruptedException e) {
                            MyLog.e(TAG, "Thread interrupted");
                            e.printStackTrace();
                        }
                    }
                }
            }
        }
    }

    private static class ClientTask extends AsyncTask<String, Void, Void> {
        @Override
        protected Void doInBackground(String... msgs) {
            MyLog.v(TAG, String.format("ClientTask: CLIENTS_INITIALIZED_COUNT: %d; CLIENTS_INITIALIZED_LIST: %s; ACTIVE_NODE_LIST: %s",
                    CLIENTS_INITIALIZED_COUNT.get(),
                    Arrays.toString(CLIENTS_INITIALIZED_LIST),
                    Arrays.toString(ACTIVE_NODE_LIST.toArray())));

            if (CLIENTS_INITIALIZED_COUNT.get() < REMOTE_PORTS.length) {
                for (int i = 0; i < REMOTE_PORTS.length; i++) {
                    if (REMOTE_PORTS[i].equals(MY_PORT)) continue;
                    if (CLIENTS_INITIALIZED_LIST[i].get() && SERVERS_INITIALIZED_LIST[i].get())
                        continue;

                    try {
                        Socket socket;
                        if (i >= MY_PORT_INDEX) {
                            if (SERVERS_INITIALIZED_LIST[i].get()) {
                                socket = CLIENT_SOCKETS[i];
                            } else {
                                continue;
                            }
                        } else {
                            socket = new Socket();
                            try {
                                MyLog.v(TAG, "ClientTask trying to establish a connection at Client_Port " + REMOTE_PORTS[i] + "...");
                                socket.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REMOTE_PORTS[i])), TIMEOUT_MSEC);
                                MyLog.d(TAG, "ClientTask has established a connection at " + socket.getInetAddress().getHostAddress() + ":" + Integer.toString(socket.getPort()));
                                CLIENT_SOCKETS[i] = socket;
                            } catch (IOException e) {
                                MyLog.e(TAG, "ClientTask socket IOException inside socket initialization");
                                e.printStackTrace();
                            }
                        }

                        MyLog.v(TAG, "ClientTask trying to return socket.getOutputStream()...");
                        PrintWriter printWriter = new PrintWriter(CLIENT_SOCKETS[i].getOutputStream(), true);
                        MyLog.v(TAG, "ClientTask socket.getOutputStream() return");

                        printWriter.println(myPortMessage() + END_DATA_SYMBOL);
                        printWriter.flush();
                        MyLog.d(TAG, "ClientTask: Sent My_Port \"" + MY_PORT + "\" to Client_Port " + REMOTE_PORTS[i]);
                        CLIENT_PRINT_WRITERS[i] = printWriter;

                        synchronized (MEMBERSHIP_LOCK) {
                            if (!CLIENTS_INITIALIZED_LIST[i].get()) {
                                CLIENTS_INITIALIZED_COUNT.getAndIncrement();
                            }
                            CLIENTS_INITIALIZED_LIST[i].set(true);
                        }

                        if (i < MY_PORT_INDEX) {
                            CLIENT_PENDING_SOCKETS.put(socket);
                        }
                    } catch (SocketException e) {
                        MyLog.e(TAG, "ClientTask socket SocketException inside initialization");
                        e.printStackTrace();
                    } catch (UnknownHostException e) {
                        MyLog.e(TAG, "ClientTask socket UnknownHostException inside initialization");
                        e.printStackTrace();
                    } catch (IOException e) {
                        MyLog.e(TAG, "ClientTask socket IOException inside initialization");
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        MyLog.e(TAG, "ClientTask socket InterruptedException inside initialization");
                        e.printStackTrace();
                    }
                }
            }

            if (msgs.length == 1 && msgs[0].isEmpty()) {
                MyLog.v(TAG, "ClientTask doInBackground return: just trying to connect all other clients");
                return null;
            }

            int sendToOne_portIndex = -1;
            if (msgs.length >= 2) {
                sendToOne_portIndex = Integer.parseInt(msgs[1]);
            }

            boolean isForceSend = false;
            if (msgs.length >= 3 && !msgs[2].isEmpty()) {
                isForceSend = true;
            }

            String msg = msgs[0].trim();

            for (int i = 0; i < REMOTE_PORTS.length; i++) {
                if (REMOTE_PORTS[i].equals(MY_PORT)) continue;
                if (!CLIENTS_INITIALIZED_LIST[i].get() && !isForceSend) continue;
                if (sendToOne_portIndex != -1 && sendToOne_portIndex != i) continue;

                PrintWriter printWriter = CLIENT_PRINT_WRITERS[i];
                if (printWriter != null) {
                    printWriter.println(msg + END_DATA_SYMBOL);
                    printWriter.flush();

                    MyLog.v(TAG, String.format("ClientTask sent msgToSend to Client_Port %s: %s",
                            REMOTE_PORTS[i],
                            msg));

                    if (sendToOne_portIndex != -1) break;
                } else if (isForceSend) {
                    MyLog.v(TAG, "ClientTask failed to force sent msgToSend: \"" + msg + "\" to Client_Port " + REMOTE_PORTS[i]);
                }
            }

            MyLog.v(TAG, "ClientTask doInBackground return");
            return null;
        }
    }


    private static void processInsert(String data, boolean isReplication, boolean shouldBeReplicated, boolean isRecovery) {
        MyLog.d(TAG, String.format("ProcessInsert call: data: %s; isReplication: %s; shouldBeReplicated: %s",
                data,
                isReplication,
                shouldBeReplicated));

        if (!isRecovery) {
            waitForRecoveryMessageDone();
        }

        String[] dataItems = data.split(" ", 5);
        String requesterNodeId = dataItems[1];
        String itemKey = dataItems[2];
        String itemValue = dataItems[3];
        String itemVersion = dataItems[4];

        Item itemToInsert = new Item(itemKey, itemValue, Long.parseLong(itemVersion));

        String keyHash = genHash(itemKey);
        String writeNode = getResponsibleNodeForWrite(keyHash);

        MyLog.v(TAG, String.format("Insert: Writing responsibility of {%s, keyHash=%s} is %s",
                itemToInsert.toString(),
                keyHash,
                writeNode));

        boolean isLocallyInserted = false;
        if (MY_NODE_ID.equals(writeNode) || isReplication || (!ACTIVE_NODE_LIST.contains(writeNode) && MY_NODE_ID.equals(nextNode(writeNode)))) {
            Item itemAlreadyStored = db.getItem(keyHash);
            if (itemAlreadyStored == null || itemAlreadyStored.version < itemToInsert.version) {
                db.insertOrUpdateItem(itemToInsert);
                isLocallyInserted = true;
                MyLog.v(TAG, "Insert: Locally inserted: " + itemToInsert.toString());
            } else {
                MyLog.d(TAG, "Insert: Locally insert skipped because the already stored version is higher or equal: " + itemAlreadyStored.toString());
            }

            if (!ACTIVE_NODE_LIST.contains(writeNode) && MY_NODE_ID.equals(nextNode(writeNode))) {
                int requestNumber = REQUEST_NUMBER.incrementAndGet();
                NODE_ID_TO_RECOVERY_DATA_LIST_MAP.get(writeNode).put(requestNumber, data);

                MyLog.d(TAG, String.format("Insert: WriteNode %s is not active! Saved message in the recovery list: %s",
                        writeNode,
                        data));
            }
        } else {
            int requestNumber = REQUEST_NUMBER.incrementAndGet();
            String replicateMessage = replicateMessage(
                    MY_NODE_ID,
                    Integer.toString(requestNumber),
                    data);

            sendToOneByNodeIdWithRecovery(writeNode, nextNode(writeNode), requestNumber, replicateMessage, data);
        }

        if (shouldBeReplicated) {
            List<String> replicationNodeList = getReplicationNodeList(keyHash);

            MyLog.v(TAG, String.format("Insert: In shouldBeReplicated: NEXT_NODE_ID: %s; replicationNodeList: %s",
                    NEXT_NODE_ID,
                    toStringFromStringList(replicationNodeList)));

            if (replicationNodeList.contains(NEXT_NODE_ID)) {
                int requestNumber = REQUEST_NUMBER.incrementAndGet();
                String replicateMessage = replicateMessage(
                        MY_NODE_ID,
                        Integer.toString(requestNumber),
                        data);

                String alternativeNodeId = nextNode(NEXT_NODE_ID);
                if (!replicationNodeList.contains(alternativeNodeId)) {
                    alternativeNodeId = null;
                }

                sendToOneByNodeIdWithRecovery(NEXT_NODE_ID, alternativeNodeId, requestNumber, replicateMessage, data);
            }

            if (!isRecovery) {
                if (replicationNodeList.contains(MY_NODE_ID)) {
                    if (!replicationNodeList.contains(NEXT_NODE_ID)
                            || (replicationNodeList.contains(NEXT_NODE_ID)
                            && !ACTIVE_NODE_LIST.contains(NEXT_NODE_ID)
                            && !replicationNodeList.contains(nextNode(NEXT_NODE_ID)))) {
                        if (!MY_NODE_ID.equals(requesterNodeId)) {
                            sendToOneByNodeId(requesterNodeId, insertConfirmMessage(MY_NODE_ID, itemToInsert));
                        } else if (isReplication) {
                            MyLog.d(TAG, String.format("Insert: INSERT_RESPONSE_MAP self-put for Item: %s",
                                    itemToInsert.toString()));

                            INSERT_RESPONSE_MAP.put(itemKey, itemToInsert);
                        }
                    }
                }

                if (MY_NODE_ID.equals(requesterNodeId) && !isReplication) {
                    Item itemInsertConfirmed;
                    String readNode = getResponsibleNodeForRead(keyHash);

                    MyLog.v(TAG, String.format("Insert: Waiting for insert propagation for node %s; Item: %s; isLocallyInserted: %s ...",
                            readNode,
                            itemToInsert.toString(),
                            isLocallyInserted));

                    while (((itemInsertConfirmed = INSERT_RESPONSE_MAP.get(itemKey)) == null)
                            || (isLocallyInserted && !itemToInsert.equals(itemInsertConfirmed))
                            || (!isLocallyInserted && !itemToInsert.weakEquals(itemInsertConfirmed))) {
                        MyLog.v(TAG, String.format("Insert: Still waiting for insert propagation for node %s; itemToInsert: %s; itemInsertConfirmed: %s; isLocallyInserted: %s ...",
                                readNode,
                                itemToInsert,
                                itemInsertConfirmed,
                                isLocallyInserted));
                        try {
                            Thread.sleep(RESPONSE_CHECK_TIMEOUT_MSEC);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    INSERT_RESPONSE_MAP.remove(itemKey);

                    MyLog.v(TAG, String.format("Insert: Waiting finished for insert propagation for node %s; Item: %s",
                            readNode,
                            itemToInsert.toString()));
                }
            }
        }
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        MyLog.d(TAG, "Insert call: " + values.toString());

        String itemKey = values.getAsString(KEY_FIELD);
        String itemValue = values.getAsString(VALUE_FIELD);
        Item itemToInsert = new Item(itemKey, itemValue);
        processInsert(insertMessage(MY_NODE_ID, itemToInsert), false, true, false);

        return uri;
    }

    private static void processDelete(String data, boolean isReplication, boolean shouldBeReplicated, boolean isRecovery) {
        MyLog.d(TAG, String.format("ProcessDelete call: data: %s; isReplication: %s; shouldBeReplicated: %s",
                data,
                isReplication,
                shouldBeReplicated));

        if (!isRecovery) {
            waitForRecoveryMessageDone();
        }

        String[] dataItems = data.split(" ", 4);
        String requesterNodeId = dataItems[1];
        String itemKey = dataItems[2];
        long itemVersion = Long.parseLong(dataItems[3]);

        String keyHash = genHash(itemKey);
        String writeNode = getResponsibleNodeForWrite(keyHash);

        if (itemKey.equals(SELECTION_LOCAL_ALL)) {
            db.deleteAllItems();

            MyLog.v(TAG, "Delete: Locally deleted key: " + itemKey);

            return;
        } else if (itemKey.equals(SELECTION_GLOBAL_ALL)) {
            db.deleteAllItems();

            MyLog.v(TAG, "Delete: Locally deleted key: " + itemKey);

            if (!MY_NODE_ID.equals(requesterNodeId)) {
                int requestNumber = REQUEST_NUMBER.incrementAndGet();
                String replicateMessage = replicateMessage(
                        MY_NODE_ID,
                        Integer.toString(requestNumber),
                        data);

                sendToOneByNodeIdWithRecovery(NEXT_NODE_ID, nextNode(NEXT_NODE_ID), requestNumber, replicateMessage, data);
            }

            return;
        } else {
            MyLog.v(TAG, String.format("Delete: Writing responsibility of {key=%s, keyHash=%s} is %s",
                    itemKey,
                    keyHash,
                    writeNode));

            if (MY_NODE_ID.equals(writeNode) || isReplication || (!ACTIVE_NODE_LIST.contains(writeNode) && MY_NODE_ID.equals(nextNode(writeNode)))) {
                Item itemAlreadyStored = db.getItem(keyHash);
                if (itemAlreadyStored == null || itemAlreadyStored.version < itemVersion) {
                    db.deleteItem(keyHash);

                    MyLog.v(TAG, "Delete: Locally deleted key: " + itemKey);
                } else {
                    MyLog.d(TAG, "Delete: Locally delete skipped because the already stored version is higher or equal: " + itemAlreadyStored.toString());
                }

                if (!ACTIVE_NODE_LIST.contains(writeNode) && MY_NODE_ID.equals(nextNode(writeNode))) {
                    int requestNumber = REQUEST_NUMBER.incrementAndGet();
                    NODE_ID_TO_RECOVERY_DATA_LIST_MAP.get(writeNode).put(requestNumber, data);

                    MyLog.d(TAG, String.format("Delete: WriteNode %s is not active! Saved message in the recovery list: %s",
                            writeNode,
                            data));
                }
            } else {
                int requestNumber = REQUEST_NUMBER.incrementAndGet();
                String replicateMessage = replicateMessage(
                        MY_NODE_ID,
                        Integer.toString(requestNumber),
                        data);

                sendToOneByNodeIdWithRecovery(writeNode, nextNode(writeNode), requestNumber, replicateMessage, data);
            }
        }

        if (shouldBeReplicated) {
            List<String> replicationNodeList = getReplicationNodeList(keyHash);

            MyLog.v(TAG, String.format("Delete: In shouldBeReplicated: NEXT_NODE_ID: %s; replicationNodeList: %s",
                    NEXT_NODE_ID,
                    toStringFromStringList(replicationNodeList)));

            if (replicationNodeList.contains(NEXT_NODE_ID)) {
                int requestNumber = REQUEST_NUMBER.incrementAndGet();
                String replicateMessage = replicateMessage(
                        MY_NODE_ID,
                        Integer.toString(requestNumber),
                        data);

                String alternativeNodeId = nextNode(NEXT_NODE_ID);
                if (!replicationNodeList.contains(alternativeNodeId)) {
                    alternativeNodeId = null;
                }

                sendToOneByNodeIdWithRecovery(NEXT_NODE_ID, alternativeNodeId, requestNumber, replicateMessage, data);
            }
            if (!isRecovery) {
                if (replicationNodeList.contains(MY_NODE_ID)) {
                    if (!replicationNodeList.contains(NEXT_NODE_ID)
                            || (replicationNodeList.contains(NEXT_NODE_ID)
                            && !ACTIVE_NODE_LIST.contains(NEXT_NODE_ID)
                            && !replicationNodeList.contains(nextNode(NEXT_NODE_ID)))) {
                        if (!MY_NODE_ID.equals(requesterNodeId)) {
                            sendToOneByNodeId(requesterNodeId, deleteConfirmMessage(MY_NODE_ID, itemKey));
                        } else if (isReplication) {
                            MyLog.d(TAG, String.format("Delete: DELETE_RESPONSE_MAP self-put for ItemKey: %s",
                                    itemKey));

                            DELETE_RESPONSE_MAP.put(itemKey, true);
                        }
                    }
                }

                if (MY_NODE_ID.equals(requesterNodeId) && !isReplication) {
                    String readNode = getResponsibleNodeForRead(keyHash);

                    MyLog.v(TAG, String.format("Delete: Waiting for delete propagation for node %s; ItemKey: %s ...",
                            readNode,
                            itemKey));

                    while (DELETE_RESPONSE_MAP.get(itemKey) == null) {
                        try {
                            Thread.sleep(RESPONSE_CHECK_TIMEOUT_MSEC);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    DELETE_RESPONSE_MAP.remove(itemKey);

                    MyLog.v(TAG, String.format("Delete: Waiting finished for delete propagation for node %s; ItemKey: %s",
                            readNode,
                            itemKey));
                }
            }
        }
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        MyLog.d(TAG, "Delete call: selection=\"" + selection + "\"; selectionArgs=\"" + Arrays.toString(selectionArgs) + "\"");

        processDelete(deleteMessage(MY_NODE_ID, selection, Long.toString(System.currentTimeMillis())), false, true, false);

        return 0;
    }

    private static Cursor processQuery(String queryKey, String requesterNodeId, String requestNumber) {
        MyLog.d(TAG, String.format("processQuery call: queryKey: %s; requesterNodeId: %s; requestNumber: %s",
                queryKey,
                requesterNodeId,
                requestNumber));

        waitForRecoveryMessageDone();

        MatrixCursor cursor = new MatrixCursor(new String[]{KEY_FIELD, VALUE_FIELD});

        if (queryKey.equals(SELECTION_LOCAL_ALL)) {
            List<Item> itemList = db.getAllItems();

            for (Item item : itemList) {
                cursor.addRow(new String[]{item.key, item.value});
            }

            MyLog.v(TAG, "Query: Locally stored: " + toStringFromItemList(itemList));

            if (!MY_NODE_ID.equals(requesterNodeId)) {
                sendToOneByNodeId(requesterNodeId, queryReturnMessage(requesterNodeId, requestNumber, queryKey, cursor));
            }

            return cursor;
        } else if (queryKey.equals(SELECTION_GLOBAL_ALL)) {
            List<Item> itemList = db.getAllItems();

            List<Item> itemListToReturn = new ArrayList<Item>(itemList.size());
            for (Item item : itemList) {
                itemListToReturn.add(new Item(item.key, item.value, 0));
            }

            MyLog.v(TAG, "Query: Locally stored: " + toStringFromItemList(itemList));

            MyLog.v(TAG, "Querying all other nodes to get their locally stored items...");
            for (String remotePort : REMOTE_PORTS) {
                String nodeId = PORT_TO_NODE_ID_MAP.get(remotePort);
                if (nodeId.equals(MY_NODE_ID)) continue;

                int requestNumberForNewQuery = REQUEST_NUMBER.incrementAndGet();
                String queryMessage = queryMessage(MY_NODE_ID, Integer.toString(requestNumberForNewQuery), SELECTION_LOCAL_ALL);
                sendToOneByNodeId(nodeId, queryMessage);

                MyLog.v(TAG, "Query: Waiting for response to the Request_Number " + Integer.toString(requestNumberForNewQuery) + "...");
                String response;
                int retry = RESPONSE_CHECK_TIMEOUT_RETRY;
                while (((response = QUERY_RESPONSE_MAP.get(requestNumberForNewQuery)) == null) && ACTIVE_NODE_LIST.contains(nodeId)) {
                    if (retry <= 0) {
                        MyLog.v(TAG, "Query: Retrying for Request_Number " + Integer.toString(requestNumberForNewQuery) + " ...");

                        sendToOneByNodeId(nodeId, queryMessage);

                        retry = RESPONSE_CHECK_TIMEOUT_RETRY;
                    }

                    MyLog.v(TAG, "Query: Still waiting for response to the Request_Number " + Integer.toString(requestNumberForNewQuery) + " ...");

                    try {
                        Thread.sleep(RESPONSE_CHECK_TIMEOUT_MSEC);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    retry--;
                }
                QUERY_RESPONSE_MAP.remove(requestNumberForNewQuery);

                MyLog.v(TAG, String.format("Query: Got Response for Request_Number %d: %s",
                        requestNumberForNewQuery,
                        response));

                if (response != null) {
                    String[] responseItems = response.split(" ", 2);
                    int count = Integer.parseInt(responseItems[0]);
                    if (count > 0) {
                        String[] itemStrings = responseItems[1].split(" ", count * 2);
                        MyLog.v(TAG, "Query: Split itemStrings: " + Arrays.toString(itemStrings));

                        MyLog.v(TAG, "Query: Going to \"for (int i = 0; i < count; i++)\" loop; itemListToReturn: " + toStringFromItemList(itemListToReturn));
                        for (int i = 0; i < count; i++) {
                            //noinspection PointlessArithmeticExpression
                            String key = itemStrings[(i * 2) + 0];
                            String value = itemStrings[(i * 2) + 1];

                            Item item = new Item(key, value, 0);
                            if (!itemListToReturn.contains(item)) {
                                itemListToReturn.add(item);

                                MyLog.v(TAG, "Query: Add to itemListToReturn: " + item.toString());
                            } else {
                                MyLog.d(TAG, "Query: Already exists in itemListToReturn: " + item.toString());
                            }
                        }
                    }
                } else {
                    MyLog.d(TAG, String.format("Query: Node %s is not active.",
                            nodeId));
                }
            }

            for (Item item : itemListToReturn) {
                MyLog.v(TAG, "Query: Add from itemListToReturn to cursor: " + item.toString());

                cursor.addRow(new String[]{item.key, item.value});
            }

            MyLog.v(TAG, "Query: Return: " + toStringFromItemList(itemListToReturn));
            return cursor;
        } else {
            String keyHash = genHash(queryKey);
            String readNode = getResponsibleNodeForRead(keyHash);

            MyLog.v(TAG, String.format("Query: Reading responsibility of {key=%s, keyHash=%s} is %s",
                    queryKey,
                    keyHash,
                    readNode));

            if (MY_NODE_ID.equals(readNode) || (!ACTIVE_NODE_LIST.contains(readNode) && MY_NODE_ID.equals(prevNode(readNode)))) {
                MyLog.v(TAG, "Query: Trying to get the value of \"" + queryKey + "\"; hash=" + keyHash + "...");

                Item itemQueried = db.getItem(keyHash);
                if (itemQueried != null) {
                    cursor.addRow(new String[]{itemQueried.key, itemQueried.value});
                    MyLog.d(TAG, "Query found locally: KeyHash=" + keyHash + " " + itemQueried.toString());
                } else {
                    MyLog.d(TAG, "Query not found locally: KeyHash=" + keyHash + " key=" + queryKey);
                }

                if (!MY_NODE_ID.equals(requesterNodeId)) {
                    sendToOneByNodeId(requesterNodeId, queryReturnMessage(requesterNodeId, requestNumber, queryKey, cursor));
                }

                MyLog.d(TAG, "Query returned for: KeyHash=" + keyHash + " " + ((itemQueried == null) ? "NULL" : itemQueried.toString()));
                return cursor;
            } else {
                if (!MY_NODE_ID.equals(requesterNodeId)) {
                    sendToOneByNodeId(readNode, queryMessage(requesterNodeId, requestNumber, queryKey));

                    return null;
                } else {
                    int requestNumberForNewQuery = REQUEST_NUMBER.incrementAndGet();
                    String queryMessage = queryMessage(MY_NODE_ID, Integer.toString(requestNumberForNewQuery), queryKey);

                    if (ACTIVE_NODE_LIST.contains(readNode)) {
                        sendToOneByNodeId(readNode, queryMessage);
                    } else {
                        MyLog.v(TAG, String.format("Query: ReadNode = %s is not active!",
                                readNode));
                        sendToOneByNodeId(prevNode(readNode), queryMessage);
                    }

                    MyLog.v(TAG, "Query: Waiting for response to the Request_Number " + Integer.toString(requestNumberForNewQuery) + "...");
                    String response;
                    int retry = RESPONSE_CHECK_TIMEOUT_RETRY;
                    while ((response = QUERY_RESPONSE_MAP.get(requestNumberForNewQuery)) == null) {
                        if (retry <= 0) {
                            MyLog.v(TAG, "Query: Retrying for Request_Number " + Integer.toString(requestNumberForNewQuery) + " ...");

                            sendToOneByNodeId(readNode, queryMessage);
                            sendToOneByNodeId(prevNode(readNode), queryMessage);

                            retry = RESPONSE_CHECK_TIMEOUT_RETRY;
                        }

                        MyLog.v(TAG, "Query: Still waiting for response to the Request_Number " + Integer.toString(requestNumberForNewQuery) + " ...");

                        try {
                            Thread.sleep(RESPONSE_CHECK_TIMEOUT_MSEC);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }

                        retry--;
                    }
                    QUERY_RESPONSE_MAP.remove(requestNumberForNewQuery);

                    String[] responseItems = response.split(" ", 3);
                    int count = Integer.parseInt(responseItems[0]);
                    if (count > 0) {
                        String key = responseItems[1];
                        String value = responseItems[2];

                        cursor.addRow(new String[]{key, value});
                    }

                    return cursor;
                }
            }
        }
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection,
                        String[] selectionArgs, String sortOrder) {
        MyLog.d(TAG, "Query call: selection=\"" + selection + "\"; selectionArgs=\"" + Arrays.toString(selectionArgs) + "\"");

        return processQuery(selection, MY_NODE_ID, null);
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection,
                      String[] selectionArgs) {
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        return null;
    }

    private static String genHash(String input) {
        MessageDigest sha1;
        try {
            sha1 = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            return "";
        }
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private static void sendToAll(String message) {
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
    }

    private static void sendToOneByPortIndex(int portIndex, String message) {
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message, Integer.toString(portIndex));
    }

    private static void sendToOneByNodeId(String nodeId, String message) {
        sendToOneByPortIndex(Arrays.asList(REMOTE_PORTS).indexOf(NODE_ID_TO_PORT_MAP.get(nodeId)), message);
    }

    private static void sendToOneByNodeIdWithRecovery(String nodeId,
                                                      String alternativeNodeId,
                                                      int requestNumber,
                                                      String messageToSend,
                                                      String messageForRecovery) {
        if (ACTIVE_NODE_LIST.contains(nodeId)) {
            MyLog.d(TAG, String.format("Node %s is active. Sending: %s",
                    nodeId,
                    messageToSend));
        } else {
            MyLog.d(TAG, String.format("Node %s is not active! Still trying to send: %s",
                    nodeId,
                    messageToSend));
        }

        NODE_ID_TO_RECOVERY_DATA_LIST_MAP.get(nodeId).put(requestNumber, messageForRecovery);
        sendToOneByNodeId(nodeId, messageToSend);

        if (alternativeNodeId != null) {
            MyLog.d(TAG, String.format("Node %s being active or not, sending to alternative node %s: %s",
                    nodeId,
                    alternativeNodeId,
                    messageToSend));

            NODE_ID_TO_RECOVERY_DATA_LIST_MAP.get(alternativeNodeId).put(requestNumber, messageForRecovery);
            sendToOneByNodeId(alternativeNodeId, messageToSend);
        }
    }

    private static void connectToDisconnectedClients() {
        sendToAll(myPortMessage());
    }

    private static void sendRecoveryMessageListToClient(String clientNodeId) {
        Map<Integer, String> messageResponseMap = NODE_ID_TO_RECOVERY_DATA_LIST_MAP.get(clientNodeId);
        List<Integer> sortedRequestNumberList = new ArrayList<Integer>(messageResponseMap.keySet());
        Collections.sort(sortedRequestNumberList);
        while (!sortedRequestNumberList.isEmpty()) {
            int requestNumber = sortedRequestNumberList.get(0);
            sendToOneByNodeId(clientNodeId, recoveryMessage(
                    MY_NODE_ID,
                    Integer.toString(requestNumber),
                    messageResponseMap.get(requestNumber)));

            sortedRequestNumberList.remove(0);
        }

        sendToOneByNodeId(clientNodeId, recoveryMessageDone(MY_NODE_ID));

        MyLog.v(TAG, String.format("ServerTask: Recovery message sending has been completed for Client_Port %s; RECOVERY_MESSAGE_FETCHED_FROM_CLIENT_LIST: %s",
                NODE_ID_TO_PORT_MAP.get(clientNodeId),
                toStringFromStringList(RECOVERY_MESSAGE_FETCHED_FROM_CLIENT_LIST)));
    }

    private static void waitForRecoveryMessageDone() {
        MyLog.v(TAG, String.format("Waiting for receiving all recovery messages; RECOVERY_MESSAGE_FETCHED_FROM_CLIENT_LIST: %s ...",
                toStringFromStringList(RECOVERY_MESSAGE_FETCHED_FROM_CLIENT_LIST)));

        boolean isRecoveryMessageFetchedInitially = true;
        while (RECOVERY_MESSAGE_FETCHED_FROM_CLIENT_LIST.size() < REMOTE_PORTS.length) {
            isRecoveryMessageFetchedInitially = false;

            MyLog.v(TAG, String.format("Still waiting for receiving all recovery messages; RECOVERY_MESSAGE_FETCHED_FROM_CLIENT_LIST: %s ...",
                    toStringFromStringList(RECOVERY_MESSAGE_FETCHED_FROM_CLIENT_LIST)));

            if ((RECOVERY_MESSAGE_FETCHED_FROM_CLIENT_LIST.size() == ACTIVE_NODE_LIST.size())
                    && (ACTIVE_NODE_LIST.size() >= (REMOTE_PORTS.length - FAILURE_TOLERANT))) {
                MyLog.v(TAG, "Waiting for receiving all recovery messages break for equal ACTIVE_NODE_LIST");

                break;
            }

            try {
                Thread.sleep(RESPONSE_CHECK_TIMEOUT_MSEC);
            } catch (InterruptedException e) {
                MyLog.e(TAG, "Thread interrupted");
                e.printStackTrace();
            }
        }

        if (!isRecoveryMessageFetchedInitially) {
            try {
                Thread.sleep(TIMEOUT_MSEC);
            } catch (InterruptedException e) {
                MyLog.e(TAG, "Thread interrupted");
                e.printStackTrace();
            }
        }

        MyLog.v(TAG, String.format("Waiting for receiving all recovery messages completed; RECOVERY_MESSAGE_FETCHED_FROM_CLIENT_LIST: %s",
                toStringFromStringList(RECOVERY_MESSAGE_FETCHED_FROM_CLIENT_LIST)));
    }


    private static String myPortMessage() {
        return HEADER_MY_PORT + " " + MY_PORT;
    }

    private static String insertMessage(String requesterNodeId, Item itemToInsert) {
        return HEADER_INSERT + " " +
                requesterNodeId + " " +
                itemToInsert.key + " " +
                itemToInsert.value + " " +
                Long.toString(itemToInsert.version);
    }

    private static String insertConfirmMessage(String requesterNodeId, Item item) {
        return HEADER_INSERT_CONFIRM + " " +
                requesterNodeId + " " +
                item.key + " " +
                item.value + " " +
                Long.toString(item.version);
    }

    private static String deleteMessage(String requesterNodeId, String itemKey, String itemVersion) {
        return HEADER_DELETE + " " +
                requesterNodeId + " " +
                itemKey + " " +
                itemVersion;
    }

    private static String deleteConfirmMessage(String requesterNodeId, String itemKey) {
        return HEADER_DELETE_CONFIRM + " " +
                requesterNodeId + " " +
                itemKey;
    }

    private static String queryMessage(String requesterNodeId, String requestNumber, String queryKey) {
        return HEADER_QUERY_REQUEST + " " +
                requesterNodeId + " " +
                requestNumber + " " +
                queryKey;
    }

    private static String queryReturnMessage(String requesterNodeId, String requestNumber, String queryKey, Cursor cursor) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(HEADER_QUERY_RETURN).append(" ");
        stringBuilder.append(requesterNodeId).append(" ");
        stringBuilder.append(requestNumber).append(" ");
        stringBuilder.append(queryKey).append(" ");
        stringBuilder.append(Integer.toString(cursor.getCount()));

        if (cursor.moveToFirst()) {
            int key_col_index = cursor.getColumnIndex(KEY_FIELD);
            int value_col_index = cursor.getColumnIndex(VALUE_FIELD);
            do {
                stringBuilder.append(" ").append(cursor.getString(key_col_index)).append(" ").append(cursor.getString(value_col_index));
            } while (cursor.moveToNext());
        }

        return stringBuilder.toString();
    }

    private static String replicateMessage(String requesterNodeId, String requestNumber, String message) {
        return HEADER_REPLICATE_REQUEST + " " +
                requesterNodeId + " " +
                requestNumber + " " +
                message;
    }

    private static String recoveryMessage(String requesterNodeId, String requestNumber, String message) {
        return HEADER_RECOVERY_REQUEST + " " +
                requesterNodeId + " " +
                requestNumber + " " +
                message;
    }

    private static String recoveryMessageDone(String requesterNodeId) {
        return HEADER_RECOVERY_DONE + " " +
                requesterNodeId;
    }


    /**
     * Does floor mod instead of regular Java mod, e.g., (-2 % 3) will return 1 instead -2.
     *
     * @param a an integer dividend
     * @param b an integer divisor
     * @return remainder of a / b
     */
    private static int mod(int a, int b) {
        return ((a % b + b) % b);
    }

    /**
     * Calculates the distance considering the circular pattern
     *
     * @param hexStart starting number represented by a hex string
     * @param hexEnd   ending number represented by a hex string
     * @return the distance from start to end considering the circular pattern
     */
    private static BigInteger distance(String hexStart, String hexEnd) {
        BigInteger start = new BigInteger(hexStart, 16);
        BigInteger end = new BigInteger(hexEnd, 16);

        if (start.compareTo(end) <= 0) {
            return end.subtract(start);
        } else {
            return SHA1_ALL_VALUES.subtract(start.subtract(end));
        }
    }

    /**
     * Determines if the value is in the range considering the circular pattern
     *
     * @param hexStart starting number represented by a hex string
     * @param hexEnd   ending number represented by a hex string
     * @return true if the value is in the range ( i.e., (hexStart, hexEnd] ) considering the circular pattern
     */
    private static boolean isInRange(String hexStart, String hexEnd, String hexValue) {
        BigInteger start = new BigInteger(hexStart, 16);
        BigInteger end = new BigInteger(hexEnd, 16);
        BigInteger value = new BigInteger(hexValue, 16);

        return (distance(hexValue, hexEnd).compareTo(distance(hexStart, hexEnd)) < 0);
    }

    private static boolean isInNodeRange(String hexValue, String nodeId) {
        int nodeIndex = ALL_SORTED_NODE_LIST.indexOf(nodeId);
        int prevNodeIndex = mod(nodeIndex - 1, ALL_SORTED_NODE_LIST.size());
        return (isInRange(
                ALL_SORTED_NODE_LIST.get(prevNodeIndex),
                ALL_SORTED_NODE_LIST.get(nodeIndex),
                hexValue));
    }

    private static String getCoordinatorNode(String hexValue) {
        for (int i = 0; i < ALL_SORTED_NODE_LIST.size(); i++) {
            if (isInNodeRange(hexValue, ALL_SORTED_NODE_LIST.get(i))) {
                return ALL_SORTED_NODE_LIST.get(i);
            }
        }
        return null;
    }

    private static String getResponsibleNodeForWrite(String hexValue) {
        return getCoordinatorNode(hexValue);
    }

    private static String getResponsibleNodeForRead(String hexValue) {
        for (int i = 0; i < ALL_SORTED_NODE_LIST.size(); i++) {
            if (isInNodeRange(hexValue, ALL_SORTED_NODE_LIST.get(i))) {
                return ALL_SORTED_NODE_LIST.get(mod(i + REPLICATION_COUNT - 1, ALL_SORTED_NODE_LIST.size()));
            }
        }
        return null;
    }

    private static String prevNode(String nodeId) {
        int nodeIndex = ALL_SORTED_NODE_LIST.indexOf(nodeId);
        int prevNodeIndex = mod(nodeIndex - 1, ALL_SORTED_NODE_LIST.size());
        return ALL_SORTED_NODE_LIST.get(prevNodeIndex);
    }

    private static String nextNode(String nodeId) {
        int nodeIndex = ALL_SORTED_NODE_LIST.indexOf(nodeId);
        int nextNodeIndex = mod(nodeIndex + 1, ALL_SORTED_NODE_LIST.size());
        return ALL_SORTED_NODE_LIST.get(nextNodeIndex);
    }

    private static List<String> getReplicationNodeList(String hexValue) {
        List<String> replicationNodeList = new ArrayList<String>(REPLICATION_COUNT);

        String currentNode = getCoordinatorNode(hexValue);
        for (int i = 0; i < REPLICATION_COUNT; i++) {
            replicationNodeList.add(currentNode);
            currentNode = nextNode(currentNode);
        }

        return replicationNodeList;
    }

    private static String toStringFromItemList(Collection<Item> itemList) {
        StringBuilder stringBuilder = new StringBuilder();
        final String joinString = ", ";

        for (Item item : itemList) {
            stringBuilder.append(item.toString()).append(joinString);
        }
        if (stringBuilder.length() > 0) {
            stringBuilder.setLength(stringBuilder.length() - joinString.length());
        }

        return stringBuilder.toString();
    }

    private static String toStringFromStringList(Collection<String> stringList) {
        StringBuilder stringBuilder = new StringBuilder();
        final String joinString = ", ";

        for (String string : stringList) {
            stringBuilder.append(string).append(joinString);
        }
        if (stringBuilder.length() > 0) {
            stringBuilder.setLength(stringBuilder.length() - joinString.length());
        }

        return stringBuilder.toString();
    }

    private static class Item {
        public final String key;
        public final String value;
        public long version;

        public Item(String key, String value) {
            this(key, value, System.currentTimeMillis());
        }

        public Item(String key, String value, long version) {
            this.key = key;
            this.value = value;
            this.version = version;
        }

        @Override
        public String toString() {
            return String.format("(%s,%s,%d)", key, value, version);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Item item = (Item) o;
            return version == item.version &&
                    key.equals(item.key) &&
                    value.equals(item.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, value, version);
        }

        public boolean weakEquals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Item item = (Item) o;
            return key.equals(item.key) &&
                    value.equals(item.value);
        }
    }

    public static class MyLog {
        private static final int MAX_LOG_LENGTH = 4000;

        public static void v(String tag, String message) {
            for (String messageSegment : splitString(message)) {
                Log.v(tag, messageSegment);
            }
        }

        public static void d(String tag, String message) {
            for (String messageSegment : splitString(message)) {
                Log.d(tag, messageSegment);
            }
        }

        public static void e(String tag, String message) {
            for (String messageSegment : splitString(message)) {
                Log.e(tag, messageSegment);
            }
        }

        private static List<String> splitString(String message) {
            List<String> messageSegmentList = new ArrayList<String>((int) Math.ceil((double) message.length() / MAX_LOG_LENGTH));
            while (true) {
                messageSegmentList.add(message.substring(0, Math.min(message.length(), MAX_LOG_LENGTH)));

                if (message.length() > MAX_LOG_LENGTH) {
                    message = "...." + message.substring(MAX_LOG_LENGTH);
                } else {
                    break;
                }
            }

            return messageSegmentList;
        }
    }

    public static class AppDatabase {
        private final SQLiteDatabase dbInstance;

        public AppDatabase(Context context) {
            AppDatabaseHelper appDatabaseHelper = new AppDatabaseHelper(context);
            dbInstance = appDatabaseHelper.getWritableDatabase();
        }

        public List<Item> getAllItems() {
            List<Item> itemList = new ArrayList<Item>();

            Cursor cursor = dbInstance.query(
                    DB_TABLE_NAME,
                    new String[]{DB_KEY_COL, DB_VALUE_COL, DB_VERSION_COL},
                    null, null, null, null, null);

            if (cursor.moveToFirst()) {
                int key_col_index = cursor.getColumnIndex(DB_KEY_COL);
                int value_col_index = cursor.getColumnIndex(DB_VALUE_COL);
                int version_col_index = cursor.getColumnIndex(DB_VERSION_COL);
                do {
                    itemList.add(new Item(
                            cursor.getString(key_col_index),
                            cursor.getString(value_col_index),
                            Long.parseLong(cursor.getString(version_col_index))));
                } while (cursor.moveToNext());
            }

            cursor.close();

            return itemList;
        }

        public void insertOrUpdateItem(Item item) {
            ContentValues contentValues = new ContentValues();
            contentValues.put(DB_HASH_KEY_COL, genHash(item.key));
            contentValues.put(DB_KEY_COL, item.key);
            contentValues.put(DB_VALUE_COL, item.value);
            contentValues.put(DB_VERSION_COL, Long.toString(item.version));

            dbInstance.insertWithOnConflict(
                    DB_TABLE_NAME,
                    null,
                    contentValues,
                    SQLiteDatabase.CONFLICT_REPLACE);
        }

        public Item getItem(String hashKey) {
            Item item = null;

            Cursor cursor = dbInstance.query(
                    DB_TABLE_NAME,
                    new String[]{DB_KEY_COL, DB_VALUE_COL, DB_VERSION_COL},
                    DB_HASH_KEY_COL + " = ?",
                    new String[]{hashKey},
                    null,
                    null,
                    null);

            if (cursor.moveToFirst()) {
                int key_col_index = cursor.getColumnIndex(DB_KEY_COL);
                int value_col_index = cursor.getColumnIndex(DB_VALUE_COL);
                int version_col_index = cursor.getColumnIndex(DB_VERSION_COL);
                item = new Item(cursor.getString(
                        key_col_index),
                        cursor.getString(value_col_index),
                        Long.parseLong(cursor.getString(version_col_index)));
            }

            cursor.close();

            return item;
        }

        public void deleteItem(String hashKey) {
            int deletedRows = dbInstance.delete(
                    DB_TABLE_NAME,
                    DB_HASH_KEY_COL + " = ?",
                    new String[]{hashKey});
        }

        public void deleteAllItems() {
            int deletedRows = dbInstance.delete(
                    DB_TABLE_NAME,
                    null,
                    null);
        }
    }

    public static class AppDatabaseHelper extends SQLiteOpenHelper {
        public AppDatabaseHelper(Context context) {
            super(context, DB_TABLE_NAME, null, 1);
        }

        @Override
        public void onCreate(SQLiteDatabase db) {
            db.execSQL(
                    "CREATE TABLE IF NOT EXISTS " + DB_TABLE_NAME + " (\n"
                            + DB_HASH_KEY_COL + " text PRIMARY KEY,\n"
                            + DB_KEY_COL + " text NOT NULL,\n"
                            + DB_VALUE_COL + " text NOT NULL,\n"
                            + DB_VERSION_COL + " text NOT NULL\n"
                            + ");");

            MyLog.v(TAG, "Database onCreate called.");
        }

        @Override
        public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
            // This database is only a cache for online data, so its upgrade policy is
            // to simply to discard the data and start over
            db.execSQL("DROP TABLE IF EXISTS " + DB_TABLE_NAME);
            onCreate(db);

            MyLog.v(TAG, "Database onUpgrade called.");
        }
    }
}
