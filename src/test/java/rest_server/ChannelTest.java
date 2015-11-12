package rest_server;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

/**
 * @author zhailzh
 * 
 * @Date 2015年11月11日——上午10:42:37
 * 
 */
public class ChannelTest {

  public void selector() throws IOException {
    Selector selector = Selector.open();
    ServerSocket server = new ServerSocket(4700);
    Socket connection = server.accept();
    SocketChannel channel = connection.getChannel();
    channel.configureBlocking(false);
    SelectionKey key = channel.register(selector, SelectionKey.OP_READ);
  }

  public static void main(String[] args) throws Exception {
    RandomAccessFile fromFile = new RandomAccessFile("fromFile.txt", "rw");
    FileChannel fromChannel = fromFile.getChannel();
    RandomAccessFile toFile = new RandomAccessFile("toFile.txt", "rw");
    FileChannel toChannel = toFile.getChannel();
    long position = 0;
    long count = fromChannel.size();
    long recive = toChannel.transferFrom(fromChannel, position, count);
    System.out.println("toChannel 接受的数据为：" + recive);

    // Parameters:
    // position The position within the file at which the transfer
    // is to begin; must be non-negative
    // count The maximum number of bytes to
    // be transferred; must be non-negative
    // target The target channel
    long to = fromChannel.transferTo(0, count, toChannel);

    System.out.println("toChannel 接受的数据为：" + to);

    ByteBuffer buf = ByteBuffer.allocate(480);// 分类空间
    int bytesRead = toChannel.read(buf);// 写入数据
    while (bytesRead != -1) {
      System.out.println("Read " + bytesRead);
      System.out.println(buf.position());
      buf.flip();// 调用flip 切换到读模式
      System.out.println(buf.position());
      while (buf.hasRemaining()) {
        System.out.print((char) buf.get());// 读取数据
      }
      buf.clear();// 调用clear方法，清空所有的数据
      bytesRead = toChannel.read(buf);// 再次的写入数据
    }
  }
}
