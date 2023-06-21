package it.unimi.dsi.big.webgraph.maqy;

import it.unimi.dsi.big.webgraph.*;
import it.unimi.dsi.logging.ProgressLogger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ReadGraphDemo {
    public static void main(String[] args) {
        try {
            ProgressLogger progressLogger = new ProgressLogger();
//            BVGraph bvGraph = BVGraph.load("/media/maqy/data/dataset/test/enron",2);
//            ImmutableGraph graph = ArcListASCIIGraph.loadOffline("/media/maqy/data/dataset/test/ascii_enron.graph-txt", progressLogger);
            ImmutableGraph graph = ArcListASCIIGraph.loadOffline("/home/maqy/work/xh/py_demo/arxiv_zkr/edges.csv");
//            BVGraph.store(graph, "/media/maqy/data/dataset/test/enron");
//            ImmutableGraph graph = ArcListASCIIGraph.loadOffline("/media/maqy/data/dataset/enron-edgelist.txt", progressLogger);
//            Transform
            // System.out.println(graph.outdegree(1));
            System.out.println(graph.numNodes());
            List<Integer> targetNodes = new ArrayList<>();
            targetNodes.add(1);
            targetNodes.add(2);
            targetNodes.add(3);
            targetNodes.add(4);
            for (int node : targetNodes) {
                LazyLongIterator iter = graph.successors(node);

                System.out.println("successors:" + graph.outdegree(node));
                long cur = -1L;
                while ((cur = iter.nextLong()) != -1) {
                    System.out.print(cur + " ");
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
