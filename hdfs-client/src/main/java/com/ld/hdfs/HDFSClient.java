package com.ld.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;

public class HDFSClient {

    private FileSystem fs = null;
    private Configuration conf = new Configuration();

    /**
     * 获取连接
     * @throws Exception
     */
    @Before
    public void connection() throws Exception {
        fs = FileSystem.get(new URI("hdfs://192.168.8.111:9000"), conf, "ld");
        System.out.println(fs.toString());
        System.out.println("连接成功");
    }

    /**
     * 文件上传
     */
    @Test
    public void copyFromLocalFile() throws IOException {
        fs.copyFromLocalFile(new Path("C:/test/hello.txt"),new Path("/test/hello.txt"));
        System.out.println("上传成功");
        fs.close();
    }

    /**
     * 文件下载
     * @throws IOException
     */
    @Test
    public void copyToLocalFile() throws IOException {
        fs.copyToLocalFile(false,new Path("/test/hello.txt"),new Path("C:/test/hello1.txt"),true);
        System.out.println("下载成功");
        fs.close();
    }

    /**
     * 创建目录
     */
    @Test
    public void mkdirs() throws IOException {
        boolean mkdirs = fs.mkdirs(new Path("/sanguo/shuguo"));
        System.out.println("创建成功");
        fs.close();
    }

    /**
     * 删除文件夹
     * @throws IOException
     */
    @Test
    public void delete() throws IOException {
        fs.delete(new Path("/sanguo"),true);
        System.out.println("删除成功");
        fs.close();
    }

    /**
     * 文件重命名
     */
    @Test
    public void rename() throws IOException {
        fs.rename(new Path("/test/hello.txt"),new Path("/test/hello1.txt"));
        System.out.println("文件重命名成功");
        fs.close();
    }

    /**
     * 获取文件信息
     */
    @Test
    public void listFiles() throws IOException {
        RemoteIterator<LocatedFileStatus> list = fs.listFiles(new Path("/"), true);
        while (list.hasNext()){
            LocatedFileStatus status = list.next();
            //文件名称
            System.out.println(status.getPath().getName());
            //长度
            System.out.println(status.getLen());
            //权限
            System.out.println(status.getPermission());
            //组
            System.out.println(status.getGroup());

            BlockLocation[] blockLocations = status.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations){
                String[] hosts = blockLocation.getHosts();
                for (String host:hosts){
                    System.out.println(host);
                }
            }
            System.out.println("-----------------华丽的分割线----------------");
        }
        fs.close();
    }

    /**
     * 判断是否是文件夹
     * @throws IOException
     */
    @Test
    public void listStatus() throws IOException {
        FileStatus[] listStatus = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus:listStatus) {
            if(fileStatus.isFile()){
                System.out.println("f:" + fileStatus.getPath().getName());
            }else{
                System.out.println("d:" + fileStatus.getPath().getName());
            }
        }
        fs.close();
    }

    /**
     * 通过io流上传文件
     */
    @Test
    public void putFileByIO() throws IOException {
        //获取输入流
        FileInputStream fis = new FileInputStream(new File("c:/test/io.txt"));
        //获取输出流
        FSDataOutputStream fos = fs.create(new Path("/test/io.txt"));

        //流之间的数据传输
        IOUtils.copyBytes(fis,fos,conf);

        //关闭流
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
    }

    /**
     * 文件下载
     * @throws IOException
     */
    @Test
    public void getFileByIO() throws IOException {
        //获取输出流
        FileOutputStream fos = new FileOutputStream(new File("c:/test/io1.txt"));
        //获取输入流
        FSDataInputStream fis = fs.open(new Path("/test/io.txt"));

        IOUtils.copyBytes(fis,fos,conf);

        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
    }

    /**
     * 下载第一块
     */
    @Test
    public void readFileSeek1() throws IOException {
        //获取输入流
        FSDataInputStream fis = fs.open(new Path("/test/1.rar"));

        //获取输出流
        FileOutputStream fos = new FileOutputStream(new File("c:/test/1.rar.part1"));

        byte[] bytes = new byte[1024];
        for(int i = 0;i < 1024 * 128;i++){
            fis.read(bytes);
            fos.write(bytes);
        }

        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
    }

    /**
     * 下载第二块
     */
    @Test
    public void readFileSeek2() throws IOException {
        //获取输入流
        FSDataInputStream fis = fs.open(new Path("/test/1.rar"));
        //定位输入数据的位置
        fis.seek(1024*1024*128);

        //获取输出流
        FileOutputStream fos = new FileOutputStream(new File("c:/test/1.rar.part2"));

        IOUtils.copyBytes(fis,fos,conf);

        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
    }

}
