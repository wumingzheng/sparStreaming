package com.atguigu.bigdata.rpc.client;

import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;

public class RPCClient {
    public static void main(String[] args) throws Exception{
        Socket s = new Socket("localhost", 9999);
        PrintWriter out = new PrintWriter(
                new OutputStreamWriter(
                        s.getOutputStream(),
                        "UTF-8"
                )
        );
        out.println("CMD /c notepad");
        out.flush();
        out.close();
    }
}
