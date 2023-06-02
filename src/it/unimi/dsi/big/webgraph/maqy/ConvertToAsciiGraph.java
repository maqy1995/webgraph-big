package it.unimi.dsi.big.webgraph.maqy;

import it.unimi.dsi.big.webgraph.*;
import it.unimi.dsi.fastutil.longs.LongIterator;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * 转化ArcListAsciiGraph格式为zuckerli所需的AsciiGraph格式
 */
public class ConvertToAsciiGraph {
    public static void main(String[] args) throws IOException {
//        ASCIIGraph graph = ASCIIGraph.loadOffline("/media/maqy/data/dataset/test/ascii_test");
        ImmutableGraph graph = ArcListASCIIGraph.loadOffline("/media/maqy/data/dataset/enron-edgelist.txt");
        DataOutputStream dos = new DataOutputStream(new FileOutputStream("/home/maqy/work/xh/zuckerli/testdata/enron_ascii"));
        ByteBuffer intBuffer = ByteBuffer.allocate(4);
        intBuffer.order(ByteOrder.LITTLE_ENDIAN);
        ByteBuffer longBuffer = ByteBuffer.allocate(8);
        longBuffer.order(ByteOrder.LITTLE_ENDIAN);
        long fingerprint = 132L;
//        System.out.println(Long.toBinaryString(fingerprint));
//        System.out.println(Long.toBinaryString(fingerprint).length());
        int N = graph.intNumNodes();

        // - 8 bytes of fingerprint
        longBuffer.putLong(0, fingerprint);
        dos.write(longBuffer.array());
        // 4 bytes to represent the number of nodes N
        intBuffer.putInt(0, N);
        dos.write(intBuffer.array());
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
        long totalEdgeNum = graph.numArcs();
        longBuffer.putLong(0, totalEdgeNum);
        dos.write(longBuffer.array());
        // M 4-byte integers that represent the destination node of each graph edge.
        long curNode = 0;
        long neighbor = 0;
        NodeIterator nodeIterator = graph.nodeIterator();
        while (nodeIterator.hasNext()) {
            curNode = nodeIterator.nextLong();
            LazyLongIterator neighborsIter = graph.successors(curNode);
            while ((neighbor = neighborsIter.nextLong()) != -1) {
//                System.out.println(neighbor);
                intBuffer.putInt(0, (int) neighbor);
                dos.write(intBuffer.array());
            }
        }
        dos.flush();
        dos.close();
//        int[] numbers = {10, 20, 30, 40, 50};
//
//        try (DataOutputStream dos = new DataOutputStream(new FileOutputStream("numbers.dat"))) {
//            for (int number : numbers) {
//                dos.writeInt(number);
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
// Format description:
// - 8 bytes of fingerprint
// - 4 bytes to represent the number of nodes N
// - N+1 8-byte integers that represent the index of the first edge of the i-th
//   adjacency list. The last of these integers is the total number of edges, M.
// - M 4-byte integers that represent the destination node of each graph edge.
    }
}
