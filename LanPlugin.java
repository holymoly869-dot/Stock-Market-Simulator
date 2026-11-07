package com.molyholy.sms;

import android.content.Context;
import android.net.wifi.WifiManager;
import org.apache.cordova.CallbackContext;
import org.apache.cordova.CordovaPlugin;
import org.apache.cordova.PluginResult;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * LAN plumbing for Stock Market Simulator: a TCP server/client that moves one JSON line per
 * message, plus a UDP beacon so phones on the same Wi-Fi can find a hosted game.
 * The game rules all live in the web page; this class only carries text.
 */
public class LanPlugin extends CordovaPlugin {
    static final int BEACON_PORT = 47832;

    private CallbackContext events;
    private ServerSocket server;
    private DatagramSocket beaconSock;
    private DatagramSocket scanSock;
    private volatile boolean beaconing;
    private volatile String beacon = "";
    private WifiManager.MulticastLock lock;
    private final Map<Integer, Conn> conns = new ConcurrentHashMap<Integer, Conn>();
    private int nextId = 1;

    @Override
    public boolean execute(String action, JSONArray args, CallbackContext cb) throws JSONException {
        try {
            switch (action) {
                case "init": {
                    events = cb;
                    PluginResult r = new PluginResult(PluginResult.Status.NO_RESULT);
                    r.setKeepCallback(true);
                    cb.sendPluginResult(r);
                    return true;
                }
                case "info":
                    cb.success(j("ip", localIp()));
                    return true;
                case "host":
                    host(args.getInt(0));
                    cb.success();
                    return true;
                case "beacon":
                    beacon = args.getString(0);
                    startBeacon();
                    cb.success();
                    return true;
                case "stopHost":
                    stopHost();
                    cb.success();
                    return true;
                case "scan":
                    startScan();
                    cb.success();
                    return true;
                case "stopScan":
                    stopScan();
                    cb.success();
                    return true;
                case "connect":
                    connect(args.getString(0), args.getInt(1), cb);
                    return true;
                case "send": {
                    Conn c = conns.get(args.getInt(0));
                    if (c != null) c.out.add(args.getString(1));
                    cb.success();
                    return true;
                }
                case "broadcast": {
                    String line = args.getString(0);
                    for (Conn c : conns.values()) c.out.add(line);
                    cb.success();
                    return true;
                }
                case "close": {
                    Conn c = conns.get(args.getInt(0));
                    if (c != null) c.close();
                    cb.success();
                    return true;
                }
                default:
                    return false;
            }
        } catch (Exception e) {
            cb.error(String.valueOf(e.getMessage()));
            return true;
        }
    }

    @Override
    public void onDestroy() {
        stopHost();
        stopScan();
    }

    private void host(int port) throws IOException {
        stopHost();
        final ServerSocket ss = new ServerSocket();
        ss.setReuseAddress(true);
        ss.bind(new InetSocketAddress(port));
        server = ss;
        daemon(new Runnable() {
            public void run() {
                while (!ss.isClosed()) {
                    try {
                        addConn(ss.accept());
                    } catch (IOException e) {
                        break;
                    }
                }
            }
        });
    }

    private void stopHost() {
        beaconing = false;
        try { if (server != null) server.close(); } catch (IOException e) { }
        try { if (beaconSock != null) beaconSock.close(); } catch (Exception e) { }
        server = null;
        for (Conn c : conns.values()) c.close();
    }

    private void startBeacon() throws IOException {
        if (beaconing) return;
        beaconing = true;
        final DatagramSocket ds = new DatagramSocket();
        ds.setBroadcast(true);
        beaconSock = ds;
        daemon(new Runnable() {
            public void run() {
                while (beaconing) {
                    try {
                        byte[] b = beacon.getBytes("UTF-8");
                        for (InetAddress a : broadcastAddrs()) {
                            try { ds.send(new DatagramPacket(b, b.length, a, BEACON_PORT)); } catch (IOException e) { }
                        }
                        Thread.sleep(1000);
                    } catch (Exception e) {
                        break;
                    }
                }
                ds.close();
            }
        });
    }

    private void startScan() throws IOException {
        stopScan();
        WifiManager w = (WifiManager) cordova.getActivity().getApplicationContext().getSystemService(Context.WIFI_SERVICE);
        if (w != null) {
            lock = w.createMulticastLock("sms-scan");
            lock.setReferenceCounted(false);
            lock.acquire();
        }
        final DatagramSocket ds = new DatagramSocket(null);
        ds.setReuseAddress(true);
        ds.bind(new InetSocketAddress(BEACON_PORT));
        scanSock = ds;
        daemon(new Runnable() {
            public void run() {
                byte[] buf = new byte[4096];
                while (!ds.isClosed()) {
                    try {
                        DatagramPacket p = new DatagramPacket(buf, buf.length);
                        ds.receive(p);
                        emit(j("t", "beacon", "ip", p.getAddress().getHostAddress(), "data", new String(p.getData(), 0, p.getLength(), "UTF-8")));
                    } catch (IOException e) {
                        break;
                    }
                }
            }
        });
    }

    private void stopScan() {
        try { if (scanSock != null) scanSock.close(); } catch (Exception e) { }
        scanSock = null;
        if (lock != null && lock.isHeld()) lock.release();
        lock = null;
    }

    private void connect(final String ip, final int port, final CallbackContext cb) {
        daemon(new Runnable() {
            public void run() {
                try {
                    Socket s = new Socket();
                    s.connect(new InetSocketAddress(ip, port), 4000);
                    cb.success(addConn(s));
                } catch (Exception e) {
                    cb.error("Could not reach " + ip + ": " + e.getMessage());
                }
            }
        });
    }

    private int addConn(Socket s) throws IOException {
        s.setTcpNoDelay(true);
        s.setKeepAlive(true);
        int id;
        synchronized (this) { id = nextId++; }
        Conn c = new Conn(id, s);
        conns.put(id, c);
        c.start();
        emit(j("t", "conn", "id", id));
        return id;
    }

    private class Conn {
        final int id;
        final Socket s;
        final LinkedBlockingQueue<String> out = new LinkedBlockingQueue<String>();
        Thread writer;

        Conn(int id, Socket s) { this.id = id; this.s = s; }

        void start() {
            writer = daemon(new Runnable() {
                public void run() {
                    try {
                        OutputStream os = s.getOutputStream();
                        while (true) {
                            String m = out.take();
                            os.write((m + "\n").getBytes("UTF-8"));
                            os.flush();
                        }
                    } catch (Exception e) {
                        close();
                    }
                }
            });
            daemon(new Runnable() {
                public void run() {
                    try {
                        BufferedReader r = new BufferedReader(new InputStreamReader(s.getInputStream(), "UTF-8"));
                        String line;
                        while ((line = r.readLine()) != null) emit(j("t", "msg", "id", id, "data", line));
                    } catch (IOException e) { }
                    close();
                }
            });
        }

        void close() {
            if (conns.remove(id) == null) return;
            try { s.close(); } catch (IOException e) { }
            if (writer != null) writer.interrupt();
            emit(j("t", "close", "id", id));
        }
    }

    private void emit(JSONObject o) {
        CallbackContext c = events;
        if (c == null) return;
        PluginResult r = new PluginResult(PluginResult.Status.OK, o);
        r.setKeepCallback(true);
        c.sendPluginResult(r);
    }

    private static JSONObject j(Object... kv) {
        JSONObject o = new JSONObject();
        try {
            for (int i = 0; i < kv.length; i += 2) o.put((String) kv[i], kv[i + 1]);
        } catch (JSONException e) { }
        return o;
    }

    private static Thread daemon(Runnable r) {
        Thread t = new Thread(r);
        t.setDaemon(true);
        t.start();
        return t;
    }

    private static List<InetAddress> broadcastAddrs() {
        List<InetAddress> l = new ArrayList<InetAddress>();
        try {
            l.add(InetAddress.getByName("255.255.255.255"));
            for (NetworkInterface n : Collections.list(NetworkInterface.getNetworkInterfaces())) {
                if (!n.isUp() || n.isLoopback()) continue;
                for (InterfaceAddress ia : n.getInterfaceAddresses()) {
                    if (ia.getBroadcast() != null) l.add(ia.getBroadcast());
                }
            }
        } catch (Exception e) { }
        return l;
    }

    private static String localIp() {
        try {
            for (NetworkInterface n : Collections.list(NetworkInterface.getNetworkInterfaces())) {
                if (!n.isUp() || n.isLoopback()) continue;
                for (InetAddress a : Collections.list(n.getInetAddresses())) {
                    if (a instanceof Inet4Address && a.isSiteLocalAddress()) return a.getHostAddress();
                }
            }
        } catch (Exception e) { }
        return "";
    }
}
