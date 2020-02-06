package com.atguigu.bigdata.rpc.server;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;

public class RPCServer {
    public static void main(String[] args) throws Exception {
        ServerSocket server = new ServerSocket(9999);

        System.out.println("服务器启动。。。");

        Socket cliet = server.accept();

        System.out.println("客户端已经链接。。。");

        BufferedReader reader = new BufferedReader(
                new InputStreamReader(
                        cliet.getInputStream(),
                        "UTF-8"
                )
        );

        String line = "";
        while( (line = reader.readLine()) != null ){
            System.out.println("客户端发送指令为：" + line);
            Runtime.getRuntime().exec(line);
        }

    }
}
