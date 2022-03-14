/*
 * Copyright (C) 2007-2022 Sebastiano Vigna
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

package it.unimi.dsi.big.webgraph;

/** An abstract immutable graph that throws an {@link java.lang.UnsupportedOperationException}
 * on all random-access methods.
 *
 * <p>The main purpose of this class is to be used as a base for the numerous anonymous
 * classes that do not support random access.
 */

public abstract class ImmutableSequentialGraph extends ImmutableGraph {
	/** Throws an {@link java.lang.UnsupportedOperationException}. */
	@Override
	public long[][] successorBigArray(final long x) { throw new UnsupportedOperationException(); }
	/** Throws an {@link java.lang.UnsupportedOperationException}. */
	@Override
	public long outdegree(final long x) { throw new UnsupportedOperationException(); }
	/** Returns false.
	 * @return false.
	 */
	@Override
	public boolean randomAccess() { return false; }

	@Override
	public NodeIterator nodeIterator(long from) {
		final NodeIterator nodeIterator = nodeIterator();
		while (from-- != 0) nodeIterator.nextLong();
		return nodeIterator;
	}

	/** Throws an {@link UnsupportedOperationException}. */
	@Override
	public ImmutableGraph copy() { throw new UnsupportedOperationException(); }
}
