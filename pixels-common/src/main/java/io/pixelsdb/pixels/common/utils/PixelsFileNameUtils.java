/*
 * Copyright 2026 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.common.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Single source of truth for all {@code .pxl} file name construction and parsing.
 *
 * <h3>Unified format</h3>
 * <pre>
 *   &lt;hostName&gt;_&lt;yyyyMMddHHmmss&gt;_&lt;count&gt;_&lt;virtualNodeId&gt;_&lt;type&gt;.pxl
 * </pre>
 * <ul>
 *   <li>{@code hostName}      — writer node hostname; guarantees cross-node uniqueness.</li>
 *   <li>{@code yyyyMMddHHmmss_count} — from {@link DateUtil#getCurTime()}; guarantees
 *       sub-second uniqueness within the same JVM.</li>
 *   <li>{@code virtualNodeId} — consistent-hash virtual node ID used by Storage GC to
 *       group files for rewrite; {@link #VNODE_ID_NONE} ({@code -1}) for {@code single}
 *       files which carry no vnode affinity.</li>
 *   <li>{@code type}          — {@link PxlFileType} label; distinguishes the write path
 *       and determines GC eligibility.</li>
 * </ul>
 *
 * <h3>File types</h3>
 * <table border="1">
 *   <tr><th>Type</th><th>Writer</th><th>GC eligible</th></tr>
 *   <tr><td>ordered</td><td>{@code FileWriterManager} (CDC real-time path) /
 *       {@code IndexedPixelsConsumer} (indexed batch load)</td><td>yes</td></tr>
 *   <tr><td>compact</td><td>{@code CompactExecutor}</td><td>yes</td></tr>
 *   <tr><td>single</td><td>{@code SimplePixelsConsumer} (non-indexed batch load)</td><td>no</td></tr>
 *   <tr><td>copy</td><td>{@code CopyExecutor} (test/benchmark data amplification)</td><td>no</td></tr>
 * </table>
 *
 * <p>The {@code \d{14}} timestamp constraint is the parsing anchor: it is long enough that
 * no reasonable hostname component will accidentally match it, so group 1 (hostName, which
 * may itself contain underscores) resolves unambiguously via regex backtracking.
 *
 * <p>Copy-rename operations use {@link #buildCopyFileName}: it parses the source file's
 * {@code hostName} and {@code virtualNodeId}, generates a fresh timestamp via
 * {@link DateUtil#getCurTime()}, and sets the type to {@link PxlFileType#COPY}.  This
 * keeps the resulting name fully compliant with the unified format while guaranteeing
 * uniqueness across multiple copy iterations ({@code n > 1}).
 */
public final class PixelsFileNameUtils
{
    public static final String PXL_EXTENSION = ".pxl";

    /**
     * Sentinel virtualNodeId for {@link PxlFileType#SINGLE} files that have no
     * vnode affinity. Using {@code -1} avoids collision with any real virtualNodeId
     * (which is always a non-negative integer produced by consistent hashing).
     */
    public static final int VNODE_ID_NONE = -1;

    // -------------------------------------------------------------------------
    // File type
    // -------------------------------------------------------------------------

    /**
     * Enumerates the five recognised {@code .pxl} file types.
     */
    public enum PxlFileType
    {
        ORDERED("ordered"),
        COMPACT("compact"),
        SINGLE("single"),
        /** Test/benchmark copies produced by {@code CopyExecutor}; not GC-eligible. */
        COPY("copy");

        private final String label;

        PxlFileType(String label)
        {
            this.label = label;
        }

        public String getLabel()
        {
            return label;
        }

        /**
         * Returns the {@link PxlFileType} whose label equals {@code label},
         * or {@code null} if no match is found.
         */
        public static PxlFileType fromLabel(String label)
        {
            for (PxlFileType t : values())
            {
                if (t.label.equals(label))
                {
                    return t;
                }
            }
            return null;
        }
    }

    // -------------------------------------------------------------------------
    // Pattern
    // -------------------------------------------------------------------------

    /**
     * Compiled pattern matching the unified {@code .pxl} file name format.
     * <pre>
     *   &lt;hostName&gt;_&lt;yyyyMMddHHmmss&gt;_&lt;count&gt;_&lt;virtualNodeId&gt;_&lt;type&gt;.pxl
     * </pre>
     * Capture groups:
     * <ol>
     *   <li>hostName (may contain underscores)</li>
     *   <li>timestamp — exactly 14 digits (yyyyMMddHHmmss)</li>
     *   <li>atomicCount</li>
     *   <li>virtualNodeId — non-negative integer, or {@code -1} for single files</li>
     *   <li>type label — one of {@code ordered|compact|single|copy}</li>
     * </ol>
     */
    private static final Pattern PXL_PATTERN = Pattern.compile(
            "(?:.*/)?(.+)_(\\d{14})_(\\d+)_(-?\\d+)_(ordered|compact|single|copy)\\.pxl$");

    private PixelsFileNameUtils() {}

    // -------------------------------------------------------------------------
    // Builder — unified entry point
    // -------------------------------------------------------------------------

    /**
     * Builds a {@code .pxl} file name in the unified format.
     *
     * @param hostName      writer node hostname (may contain underscores)
     * @param virtualNodeId vnode ID, or {@link #VNODE_ID_NONE} for {@link PxlFileType#SINGLE}
     * @param type          file type
     * @return the constructed file name
     */
    public static String buildPxlFileName(String hostName, int virtualNodeId, PxlFileType type)
    {
        return hostName + "_" + DateUtil.getCurTime() + "_" + virtualNodeId + "_" + type.getLabel() + PXL_EXTENSION;
    }

    // -------------------------------------------------------------------------
    // Builder — semantic wrappers
    // -------------------------------------------------------------------------

    /**
     * Builds an <b>Ordered</b> file name (CDC real-time write path and indexed batch load).
     * <p>Format: {@code <hostName>_<yyyyMMddHHmmss>_<count>_<virtualNodeId>_ordered.pxl}
     */
    public static String buildOrderedFileName(String hostName, int virtualNodeId)
    {
        return buildPxlFileName(hostName, virtualNodeId, PxlFileType.ORDERED);
    }

    /**
     * Builds a <b>Compact</b> file name.
     * <p>Format: {@code <hostName>_<yyyyMMddHHmmss>_<count>_<virtualNodeId>_compact.pxl}
     * <p>The {@code virtualNodeId} must match the virtualNodeId of all source ordered files
     * in the compaction batch.
     */
    public static String buildCompactFileName(String hostName, int virtualNodeId)
    {
        return buildPxlFileName(hostName, virtualNodeId, PxlFileType.COMPACT);
    }

    /**
     * Builds a <b>Single</b> file name (non-indexed batch load, excluded from Storage GC).
     * <p>Format: {@code <hostName>_<yyyyMMddHHmmss>_<count>_-1_single.pxl}
     * <p>virtualNodeId is always {@link #VNODE_ID_NONE} ({@code -1}) because single files
     * are not routed through consistent hashing.
     */
    public static String buildSingleFileName(String hostName)
    {
        return buildPxlFileName(hostName, VNODE_ID_NONE, PxlFileType.SINGLE);
    }

    /**
     * Builds a copy file name derived from an existing {@code .pxl} file.
     *
     * <p>The resulting name conforms to the unified five-segment format:
     * <pre>
     *   &lt;hostName&gt;_&lt;newTimestamp&gt;_&lt;newCount&gt;_&lt;virtualNodeId&gt;_copy.pxl
     * </pre>
     * {@code hostName} and {@code virtualNodeId} are inherited from {@code originalPath};
     * a fresh {@link DateUtil#getCurTime()} call guarantees uniqueness across multiple
     * copy iterations ({@code n > 1}) of the same source file.
     *
     * @param originalPath original file name or absolute path (must match the unified format)
     * @return the constructed copy file name
     * @throws IllegalArgumentException if {@code originalPath} does not match the unified format
     */
    public static String buildCopyFileName(String originalPath)
    {
        String hostName = extractHostName(originalPath);
        if (hostName == null)
        {
            throw new IllegalArgumentException(
                    "Cannot build copy name: source file does not match the unified .pxl format: "
                    + originalPath);
        }
        int virtualNodeId = extractVirtualNodeId(originalPath);
        return buildPxlFileName(hostName, virtualNodeId, PxlFileType.COPY);
    }

    // -------------------------------------------------------------------------
    // Parsing
    // -------------------------------------------------------------------------

    /**
     * Extracts the {@code virtualNodeId} from a {@code .pxl} file path.
     *
     * @param path absolute or relative file path
     * @return the parsed virtualNodeId ({@link #VNODE_ID_NONE} for single-type files),
     *         or {@link #VNODE_ID_NONE} if the path does not match the unified format
     */
    public static int extractVirtualNodeId(String path)
    {
        Matcher m = match(path);
        if (m == null)
        {
            return VNODE_ID_NONE;
        }
        try
        {
            return Integer.parseInt(m.group(4));
        } catch (NumberFormatException e)
        {
            return VNODE_ID_NONE;
        }
    }

    /**
     * Extracts the {@code hostName} from a {@code .pxl} file path.
     *
     * @param path absolute or relative file path
     * @return the hostName, or {@code null} if the path does not match the unified format
     */
    public static String extractHostName(String path)
    {
        Matcher m = match(path);
        return (m != null) ? m.group(1) : null;
    }

    /**
     * Extracts the {@link PxlFileType} from a {@code .pxl} file path.
     *
     * @param path absolute or relative file path
     * @return the file type, or {@code null} if the path does not match the unified format
     */
    public static PxlFileType extractFileType(String path)
    {
        Matcher m = match(path);
        return (m != null) ? PxlFileType.fromLabel(m.group(5)) : null;
    }

    /**
     * Returns {@code true} if the file at {@code path} is eligible for Storage GC,
     * i.e. its type is one of {@link PxlFileType#ORDERED} or {@link PxlFileType#COMPACT}.
     *
     * <p>{@link PxlFileType#SINGLE} and {@link PxlFileType#COPY} files, as well as
     * unrecognised paths, return {@code false}.
     */
    public static boolean isGcEligible(String path)
    {
        PxlFileType type = extractFileType(path);
        return type == PxlFileType.ORDERED || type == PxlFileType.COMPACT;
    }

    // -------------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------------

    private static Matcher match(String path)
    {
        if (path == null || path.isEmpty())
        {
            return null;
        }
        Matcher m = PXL_PATTERN.matcher(path);
        return m.matches() ? m : null;
    }
}
