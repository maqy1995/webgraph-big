/*
 * Copyright (C) 2003-2022 Sebastiano Vigna
 *
 * This program and the accompanying materials are made available under the
 * terms of the GNU Lesser General Public License v2.1 or later,
 * which is available at
 * http://www.gnu.org/licenses/old-licenses/lgpl-2.1-standalone.html,
 * or the Apache Software License 2.0, which is available at
 * https://www.apache.org/licenses/LICENSE-2.0.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later OR Apache-2.0
 */

package it.unimi.dsi.big.webgraph.test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.Switch;
import com.martiansoftware.jsap.UnflaggedOption;

import it.unimi.dsi.Util;
import it.unimi.dsi.big.webgraph.ImmutableGraph;
import it.unimi.dsi.big.webgraph.ImmutableGraph.LoadMethod;
import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.big.webgraph.NodeIterator;
import it.unimi.dsi.lang.ObjectParser;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.util.XoRoShiRo128PlusRandom;
import it.unimi.dsi.webgraph.GraphClassParser;


public class SpeedTest {
	private final static int WARMUP = 3;
	private final static int REPEAT = 10;
	private SpeedTest() {}

    @SuppressWarnings("boxing")
	static public void main(final String arg[]) throws IllegalArgumentException, SecurityException, JSAPException, IOException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, ClassNotFoundException, InstantiationException {
		final SimpleJSAP jsap = new SimpleJSAP(SpeedTest.class.getName(), "Tests the access speed of an ImmutableGraph. By default, the graph is enumerated sequentially, but you can specify a number of nodes to be accessed randomly.\n\nThis class executes " + WARMUP + " warmup iterations, and then averages the timings of the following " + REPEAT + " iterations.",
				new Parameter[] {
						new FlaggedOption("graphClass", GraphClassParser.getParser(), JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'g', "graphClass", "Forces a Java class for the source graph."),
						new Switch("mapped", 'm', "mapped", "Use memory-mapping."),
						new Switch("spec", 's', "spec", "The basename is a specification of the form <ImmutableGraphImplementation>(arg,arg,...)."),
						new FlaggedOption("seed", JSAP.LONG_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'S', "seed", "A seed for the pseudorandom number generator."),
						new FlaggedOption("random", JSAP.LONGSIZE_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'r', "random", "Perform a random-access test on this number of nodes instead of enumerating sequentially the whole graph."),
						new Switch("first", 'f', "first", "Just enumerate the first successor of each tested node."),
						new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the graph."),
					}
				);

		final JSAPResult jsapResult = jsap.parse(arg);
		if (jsap.messagePrinted()) System.exit(1);

		final boolean random = jsapResult.userSpecified("random");
		final boolean mapped = jsapResult.userSpecified("mapped");
		final boolean spec = jsapResult.getBoolean("spec");
		final boolean first = jsapResult.userSpecified("first");
		final Class<?> graphClass = jsapResult.getClass("graphClass");
		final String basename = jsapResult.getString("basename");
		if (graphClass != null && spec) throw new IllegalArgumentException("Options --graph-class and --spec are incompatible.");

		final ProgressLogger pl = new ProgressLogger();
		final long seed = jsapResult.userSpecified("seed") ? jsapResult.getLong("seed") : Util.randomSeed();
		final XoRoShiRo128PlusRandom r = new XoRoShiRo128PlusRandom();

		System.err.println("Seed: 0x" + Long.toHexString(seed));

		// The number of overall links, unless first is true, in which case the number of tested nodes.
		long totLinks = 0;
		long cumulativeTime = 0;

		long z = -1;
		final long samples;
		final ImmutableGraph graph;

		if (random) {
			if (jsapResult.userSpecified("graphClass")) graph = (ImmutableGraph)graphClass.getMethod(mapped ? LoadMethod.MAPPED.toMethod() : LoadMethod.STANDARD.toMethod(), CharSequence.class, ProgressLogger.class).invoke(null, basename, pl);
			else if (spec) graph = ObjectParser.fromSpec(basename, ImmutableGraph.class, GraphClassParser.PACKAGE);
			else graph = mapped ? ImmutableGraph.loadMapped(basename, pl) : ImmutableGraph.load(basename, pl);

			final long n = graph.numNodes();
			samples = jsapResult.getLong("random");

			r.setSeed(seed);
			if (first) totLinks = samples;
			else for(long i = samples; i-- != 0;) totLinks += graph.outdegree(r.nextLong(n));

			System.err.println(first ? "Accessing the first link on " + samples + " random nodes using ImmutableGraph.successors()..." : "Accessing links on " + samples + " random nodes using ImmutableGraph.successors()...");

			for(int k = WARMUP + REPEAT; k-- != 0;) {
				r.setSeed(seed);
				long time = -System.nanoTime();
				if (first)
					for(long i = samples; i-- != 0;) z ^= graph.successors(r.nextLong(n)).nextLong();
				else
					for(long i = samples; i-- != 0;)
						for(final LazyLongIterator links = graph.successors(r.nextLong(n)); links.nextLong() != - 1;) z++;

				time += System.nanoTime();

				if (k < REPEAT) cumulativeTime += time;
				System.err.printf("Intermediate time: %3fs nodes: %d; arcs %d; nodes/s: %.3f arcs/s: %.3f ns/node: %3f, ns/link: %.3f\n",
						time / 1E9, samples, totLinks, (samples * 1E9) / time, (totLinks * 1E9) / time, time / (double)samples, time / (double)totLinks);
			}
		}
		else {
			if (first) throw new IllegalArgumentException("Option --first requires --random.");
			if (jsapResult.userSpecified("graphClass")) graph = (ImmutableGraph)graphClass.getMethod(mapped ? LoadMethod.MAPPED.toMethod() : LoadMethod.STANDARD.toMethod(), CharSequence.class, ProgressLogger.class).invoke(null, basename, pl);
			else if (spec)  graph = ObjectParser.fromSpec(basename, ImmutableGraph.class, GraphClassParser.PACKAGE);
			else graph = mapped ? ImmutableGraph.loadMapped(basename, pl) : ImmutableGraph.load(basename, pl);

			samples = graph.numNodes();

			System.err.println("Accessing links sequentially using ImmutableGraph.successorArray()...");

			for(int k = WARMUP + REPEAT; k-- != 0;) {
				long time = -System.nanoTime();
				final NodeIterator nodeIterator = graph.nodeIterator();
				totLinks = 0;
				for(long i = samples; i-- != 0;) {
					nodeIterator.nextLong();
					totLinks += nodeIterator.outdegree();
					nodeIterator.successorBigArray();
				}
				time += System.nanoTime();

				if (k < REPEAT) cumulativeTime += time;
				System.err.printf("Intermediate time: %3fs nodes: %d; arcs %d; nodes/s: %.3f arcs/s: %.3f ns/node: %3f, ns/link: %.3f\n",
						time / 1E9, samples, totLinks, (samples * 1E9) / time, (totLinks * 1E9) / time, time / (double)samples, time / (double)totLinks);
			}
		}

		final double averageTime = cumulativeTime / (double)REPEAT;
		System.out.printf("Time: %.3fs nodes: %d; arcs %d; nodes/s: %.3f arcs/s: %.3f ns/node: %3f, ns/link: %.3f\n",
				averageTime / 1E9, samples, totLinks, (samples * 1E9) / averageTime, (totLinks * 1E9) / averageTime, averageTime / samples, averageTime / totLinks);

		if (z == 0) System.err.println((char)0);
    }
}
