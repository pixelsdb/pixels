package cn.edu.ruc.iir.pixels.daemon.etcd.registry;

import com.coreos.jetcd.Client;

import java.net.InetAddress;

public class IpHelper {

    public static String getHostIp() throws Exception {
        String ip = InetAddress.getLocalHost().getHostAddress();
        return ip;
    }

    public static void main(String[] args) {
        Client client = null;
        String machineIp = null;
        String port = System.getProperty("ETCD_HOST");
        if (port == null) {
            port = "2379";
        }
        try {
            machineIp = IpHelper.getHostIp();
        } catch (Exception e) {
            System.out.println("Get host ip error.");
        }
        client = Client.builder().endpoints("http://" + machineIp + ":" + port).build();
        System.out.println(client.toString());
    }
}
