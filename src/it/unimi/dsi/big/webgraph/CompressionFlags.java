/*
 * Copyright (C) 2006-2022 Paolo Boldi and Sebastiano Vigna
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

/** This interface provides constants to be used as compression flags. */


public interface CompressionFlags {

	/** &delta; coding (see {@link it.unimi.dsi.io.OutputBitStream#writeDelta(int)}). */
	public static final int DELTA = 1;

	/** &gamma; coding (see {@link it.unimi.dsi.io.OutputBitStream#writeGamma(int)}). */
	public static final int GAMMA = 2;

	/** Golomb coding (see {@link it.unimi.dsi.io.OutputBitStream#writeGolomb(int,int)}). */
	public static final int GOLOMB = 3;

	/** Skewed Golomb coding (see {@link it.unimi.dsi.io.OutputBitStream#writeSkewedGolomb(int,int)}). */
	public static final int SKEWED_GOLOMB = 4;

	/** Unary coding (see {@link it.unimi.dsi.io.OutputBitStream#writeUnary(int)}). */
	public static final int UNARY = 5;

	/** &zeta;<sub><var>k</var></sub> coding (see {@link it.unimi.dsi.io.OutputBitStream#writeZeta(int,int)}). */
	public static final int ZETA = 6;

	/** Variable-length nibble coding (see {@link it.unimi.dsi.io.OutputBitStream#writeNibble(int)}). */
	public static final int NIBBLE = 7;

	public static final String[] CODING_NAME = { "DEFAULT", "DELTA", "GAMMA", "GOLOMB", "SKEWED_GOLOMB", "UNARY", "ZETA", "NIBBLE" };

}
