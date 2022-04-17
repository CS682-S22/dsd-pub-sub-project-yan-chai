package Model;

public class ServerInfo {

    private String addr;
    private int port;

    public ServerInfo(String addr, int port) {
        this.addr = addr;
        this.port = port;
    }

    public String getAddr() {
        return addr;
    }

    public int getPort() {
        return port;
    }
}
