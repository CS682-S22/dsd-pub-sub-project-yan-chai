package Driver;

import java.io.IOException;
import java.net.Socket;
import java.util.Arrays;

public class SocketTest {

    public static void main(String[] args) throws IOException {
        Socket socket = new Socket("127.0.0.1", 3000);
        byte[] tmp = new byte[3];
        socket.getOutputStream().write(new byte[]{4,5,6});
        socket.getOutputStream().write(new byte[]{40,50,60});
        socket.getInputStream().read(tmp);
        System.out.println(Arrays.toString(tmp));
        tmp = new byte[3];
        socket.getInputStream().read(tmp);
        System.out.println(Arrays.toString(tmp));
    }
}
