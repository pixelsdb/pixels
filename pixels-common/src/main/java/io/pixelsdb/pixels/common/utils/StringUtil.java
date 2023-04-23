/*
 * Copyright 2019-2022 PixelsDB.
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

/**
 * @author guodong
 * @author hank
 */
public class StringUtil
{
    private StringUtil()
    {
    }

    public static String replaceAll(String text, String searchString, String replacement)
    {
        if (text.isEmpty() || searchString.isEmpty() || replacement.isEmpty())
        {
            return text;
        }
        int start = 0;
        int end = text.indexOf(searchString, start);
        if (end == -1)
        {
            return text;
        }
        int repLen = searchString.length();
        StringBuilder buf = new StringBuilder();
        while (end != -1)
        {
            buf.append(text, start, end).append(replacement);
            start = end + repLen;
            end = text.indexOf(searchString, start);
        }
        buf.append(text.substring(start));
        return buf.toString();
    }

    /**
     * Derived from org.apache.curator.utils.PathUtils.
     * @param path
     * @return
     * @throws IllegalArgumentException
     */
    public static String validatePath(String path) throws IllegalArgumentException
    {
        if (path == null)
        {
            throw new IllegalArgumentException("Path cannot be null");
        } else if (path.length() == 0)
        {
            throw new IllegalArgumentException("Path length must be > 0");
        } else if (path.charAt(0) != '/')
        {
            throw new IllegalArgumentException("Path must start with / character");
        } else if (path.length() == 1)
        {
            return path;
        } else if (path.charAt(path.length() - 1) == '/')
        {
            throw new IllegalArgumentException("Path must not end with / character");
        } else
        {
            String reason = null;
            char lastc = '/';
            char[] chars = path.toCharArray();

            for (int i = 1; i < chars.length; ++i)
            {
                char c = chars[i];
                if (c == 0)
                {
                    reason = "null character not allowed @" + i;
                    break;
                }

                if (c == '/' && lastc == '/')
                {
                    reason = "empty node name specified @" + i;
                    break;
                }

                if (c == '.' && lastc == '.')
                {
                    if (chars[i - 2] == '/' && (i + 1 == chars.length || chars[i + 1] == '/'))
                    {
                        reason = "relative paths not allowed @" + i;
                        break;
                    }
                } else if (c == '.')
                {
                    if (chars[i - 1] == '/' && (i + 1 == chars.length || chars[i + 1] == '/'))
                    {
                        reason = "relative paths not allowed @" + i;
                        break;
                    }
                } else if (c > 0 && c < 31 || c > 127 && c < 159 || c > '\ud800' && c < '\uf8ff' || c > '\ufff0' && c < '\uffff')
                {
                    reason = "invalid charater @" + i;
                    break;
                }

                lastc = chars[i];
            }

            if (reason != null)
            {
                throw new IllegalArgumentException("Invalid path string \"" + path + "\" caused by " + reason);
            } else
            {
                return path;
            }
        }
    }

    /**
     * Derived from org.apache.curator.utils.PathUtils.
     * @param parent
     * @param child
     * @return
     */
    public static String makePath(String parent, String child)
    {
        int maxPathLength = nullableStringLength(parent) + nullableStringLength(child) + 2;
        StringBuilder path = new StringBuilder(maxPathLength);
        joinPath(path, parent, child);
        return path.toString();
    }

    /**
     * Derived from org.apache.curator.utils.PathUtils.
     * @param s
     * @return
     */
    private static int nullableStringLength(String s)
    {
        return s != null ? s.length() : 0;
    }

    /**
     * Derived from org.apache.curator.utils.PathUtils.
     * @param path
     * @param parent
     * @param child
     */
    private static void joinPath(StringBuilder path, String parent, String child)
    {
        if (parent != null && parent.length() > 0)
        {
            if (parent.charAt(0) != '/')
            {
                path.append('/');
            }

            if (parent.charAt(parent.length() - 1) == '/')
            {
                path.append(parent, 0, parent.length() - 1);
            } else
            {
                path.append(parent);
            }
        }

        if (child == null || child.length() == 0 || child.length() == 1 && child.charAt(0) == '/')
        {
            if (path.length() == 0)
            {
                path.append('/');
            }

        } else
        {
            path.append('/');
            byte childAppendBeginIndex;
            if (child.charAt(0) == '/')
            {
                childAppendBeginIndex = 1;
            } else
            {
                childAppendBeginIndex = 0;
            }

            int childAppendEndIndex;
            if (child.charAt(child.length() - 1) == '/')
            {
                childAppendEndIndex = child.length() - 1;
            } else
            {
                childAppendEndIndex = child.length();
            }

            path.append(child, childAppendBeginIndex, childAppendEndIndex);
        }
    }
}
