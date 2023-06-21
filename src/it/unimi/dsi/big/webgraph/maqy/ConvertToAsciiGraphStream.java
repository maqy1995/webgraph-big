package it.unimi.dsi.big.webgraph.maqy;

import it.unimi.dsi.big.webgraph.ArcListASCIIGraph;
import it.unimi.dsi.big.webgraph.ImmutableGraph;
import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.big.webgraph.NodeIterator;
import it.unimi.dsi.fastutil.longs.LongIterator;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * 流式转化ArcListAsciiGraph格式为zuckerli所需的AsciiGraph格式，针对大图的情况进行转化。
 * 需要提前知道图中的节点数量并作为参数输入。
 *
 * zkr input format description:
 * // Format description:
 * // - 8 bytes of fingerprint
 * // - 4 bytes to represent the number of nodes N
 * // - N+1 8-byte integers that represent the index of the first edge of the i-th
 * //   adjacency list. The last of these integers is the total number of edges, M.
 * // - M 4-byte integers that represent the destination node of each graph edge.
 */
public class ConvertToAsciiGraphStream {
    public static void main(String[] args) throws IOException {
        // 需要提前知道节点数量
        String inputFilePath = args[0];
        String outputFilePath = "/home/maqy/work/xh/py_demo/arxiv_zkr/zkr_test";
        int N = Integer.parseInt(args[1]);

        File file = new File(inputFilePath);
        InputStream inputStream = Files.newInputStream(file.toPath());
        ImmutableGraph graph = ArcListASCIIGraph.loadOnce(inputStream);
        DataOutputStream dos = new DataOutputStream(Files.newOutputStream(Paths.get(outputFilePath)));

        long fingerprint = 132L;

        System.out.println(N);

        // 写入的字节Buffer
        ByteBuffer intBuffer = ByteBuffer.allocate(4);
        intBuffer.order(ByteOrder.LITTLE_ENDIAN);
        ByteBuffer longBuffer = ByteBuffer.allocate(8);
        longBuffer.order(ByteOrder.LITTLE_ENDIAN);
        // - 8 bytes of fingerprint
        longBuffer.putLong(0, fingerprint);
        dos.write(longBuffer.array());
        // 写入节点总数
        intBuffer.putInt(0, N);
        dos.write(intBuffer.array());
        // 写入各节点的邻居偏移量，第一个为0
        // N+1 8-byte integers that represent the index of the first edge of the i-th
        // adjacency list. The last of these integers is the total number of edges, M.
        LongIterator outDegreeIter = graph.outdegrees();
        long curIndex = 0;
        while (outDegreeIter.hasNext()) {
            longBuffer.putLong(0, curIndex);
            dos.write(longBuffer.array());
            // 最后一个不写入
            curIndex = curIndex + outDegreeIter.nextLong();
        }
        long totalEdgeNum = curIndex;
        longBuffer.putLong(0, totalEdgeNum);
        dos.write(longBuffer.array());

        // 写入每个节点的邻居ID
        // M 4-byte integers that represent the destination node of each graph edge.
        long curNode = 0;
        long neighbor = 0;
        // 重新读取一次Graph，写入所有邻居点
        inputStream.close();
        File file2 = new File(inputFilePath);
        InputStream inputStream2 = Files.newInputStream(file2.toPath());
        ImmutableGraph graph2 = ArcListASCIIGraph.loadOnce(inputStream2);
        NodeIterator nodeIterator = graph2.nodeIterator();
        while (nodeIterator.hasNext()) {
            curNode = nodeIterator.nextLong();
            LazyLongIterator neighborsIter = nodeIterator.successors();
            while ((neighbor = neighborsIter.nextLong()) != -1) {
//                System.out.println(neighbor);
                intBuffer.putInt(0, (int) neighbor);
                dos.write(intBuffer.array());
            }
        }
        dos.flush();
        dos.close();
    }
}
