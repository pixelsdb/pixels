package cn.edu.ruc.iir.pixels.cache;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * pixels
 *
 * @author guodong
 */
public class PixelsRadix
{
    private final DynamicArray<RadixNode> nodes;    // tree nodes.

    public PixelsRadix()
    {
        this.nodes = new DynamicArray<>();
        RadixNode root = new RadixNode();
        root.setEdge(new byte[0]);
        nodes.add(root);                 // add root node
    }

    public void put(PixelsCacheKey cacheKey, PixelsCacheIdx cacheItem)
    {
        putInternal(cacheKey, cacheItem, true);
    }

    public void putIfAbsent(PixelsCacheKey cacheKey, PixelsCacheIdx cacheItem)
    {
        putInternal(cacheKey, cacheItem, false);
    }

    /**
     * Take a radix tree containing 'team' and 'test' as example.
     * The original tree is:
     *   |root| -> |te|
     *                  -> |am|
     *                  -> |st|
     * 1. If we put <test, new_value> into the original tree, it's a EXACT_MATCH with the |st| node.
     * Then we add the new_value into the node or replace the node.
     *
     * 2. If we put <tea, new_value> into the original tree, it's a KEY_ENDS_AT_MID_EDGE with the |am| node.
     * Then we split the |am| node into two.
     *   |root| -> |te|
     *                  -> |a|
     *                         -> |m|
     *                  -> |st|
     *
     * 3. If we put <teak, new_value> into the original tree, it's a MATCH_END_AT_MID_EDGE with the |am| node.
     * Then we split the |am| node into three.
     *   |root| -> |te|
     *                  -> |a|
     *                         -> |m|
     *                         -> |k|
     *                  -> |st|
     *
     * 4. If we put <teamster, new_value> into the original tree, it's a MATCH_END_AT_END_EDGE with the |am| node.
     * Then we add a new node containing the trailing bytes from the key, and append it to the |am| node.
     *   |root| -> |te|
     *                  -> |am|
     *                         -> |ster|
     *                  -> |st|
     * */
    private void putInternal(PixelsCacheKey cacheKey, PixelsCacheIdx cacheIdx, boolean overwrite)
    {
        checkArgument(cacheKey != null, "cache key is null");
        checkArgument(cacheIdx != null, "cache index item is null");
        SearchResult searchResult = searchInternal(cacheKey.getBytes());
        SearchResult.Type matchingType = searchResult.matchType;
        RadixNode nodeFound = searchResult.nodeFound;

        switch (matchingType) {
            // an exact match for all edges leading to this node.
            // -> add or update the value in the node found.
            case EXACT_MATCH:
            {
                // check if found node has value.
                // if overwrite is not allowed and the node has a value, then return early.
                PixelsCacheIdx existingValue = nodeFound.getValue();
                if (!overwrite && existingValue != null)
                {
                    return;
                }
                // if overwrite is allowed, then replace existing node
                nodeFound.setValue(cacheIdx);
                return;
            }

            // key run out of bytes while match in the middle of an edge in the node.
            // -> split the node into two:
            //    (1) a new parent node storing the new value with key matched bytes with current node
            //    (2) a new node holding original value and edges with edge trailing bytes of current node
            case KEY_ENDS_AT_MID_EDGE:
            {
                byte[] commonPrefix = Arrays.copyOfRange(nodeFound.getEdge(),
                        0, searchResult.bytesMatchedInNodeFound);
                byte[] edgeSuffix = Arrays.copyOfRange(nodeFound.getEdge(),
                        searchResult.bytesMatchedInNodeFound,
                        nodeFound.getEdge().length);

                RadixNode newParent = new RadixNode();
                RadixNode newChild = new RadixNode();
                newParent.setEdge(commonPrefix);
                newParent.setValue(cacheIdx);
                newParent.addChild(newChild, true);
                newChild.setEdge(edgeSuffix);
                newChild.setValue(nodeFound.getValue());
                newChild.setChildren(nodeFound.getChildren());

                searchResult.parentNode.addChild(newParent, true);
                return;
            }

            // edge run out of bytes while match in the middle of key
            // -> add a new child to the node, containing the trailing bytes from the key.
            case MATCH_END_AT_END_EDGE:
            {
                byte[] keySuffix = Arrays.copyOfRange(cacheKey.getBytes(),
                        searchResult.bytesMatched, cacheKey.getBytes().length);
                RadixNode newNode = new RadixNode();
                newNode.setEdge(keySuffix);
                newNode.setValue(cacheIdx);
                nodeFound.addChild(newNode, true);
                return;
            }

            // difference is spotted between the key and the bytes in the middle of the edge,
            // and they still have trailing unmatched characters.
            // -> split the node into three:
            //    (1) a new parent node containing matched bytes from the key and the edge of current node.
            //    (2) a new node containing the unmatched bytes from the rest of the key, and the value supplied.
            //    (3) a new node containing the unmatched bytes from the rest of the edge, and the original edges and value.
            case MATCH_END_AT_MID_EDGE:
            {
                byte[] commonPrefix = Arrays.copyOfRange(nodeFound.getEdge(),
                        0, searchResult.bytesMatchedInNodeFound);
                byte[] keySuffix = Arrays.copyOfRange(cacheKey.getBytes(),
                        searchResult.bytesMatched, cacheKey.getBytes().length);
                byte[] edgeSuffix = Arrays.copyOfRange(nodeFound.getEdge(),
                        searchResult.bytesMatchedInNodeFound, nodeFound.getEdge().length);
                RadixNode parentNode = new RadixNode();
                RadixNode childNode1 = new RadixNode();
                RadixNode childNode2 = new RadixNode();
                parentNode.setEdge(commonPrefix);
                childNode1.setEdge(keySuffix);
                childNode1.setValue(cacheIdx);
                childNode2.setEdge(edgeSuffix);
                childNode2.setValue(nodeFound.getValue());
                childNode2.setChildren(nodeFound.getChildren());
                parentNode.addChild(childNode1, true);
                parentNode.addChild(childNode2, true);

                searchResult.parentNode.addChild(parentNode, true);
                return;
            }

            default:
                throw new IllegalStateException("Unexpected matching type for search result: " + searchResult);
        }
    }

    public PixelsCacheIdx get(PixelsCacheKey cacheKey)
    {
        RadixNode root = nodes.get(0);
        // if tree is empty, return null
        if (root.getChildren().isEmpty()) {
            return null;
        }
        SearchResult searchResult = searchInternal(cacheKey.getBytes());
        if (searchResult.matchType.equals(SearchResult.Type.EXACT_MATCH)) {
            return searchResult.nodeFound.getValue();
        }
        return null;
    }

    public boolean remove(PixelsCacheKey cacheKey)
    {
        checkArgument(cacheKey != null, "cache key is null");
        SearchResult searchResult = searchInternal(cacheKey.getBytes());
        SearchResult.Type matchType = searchResult.matchType;
        RadixNode nodeFound = searchResult.nodeFound;
        switch (matchType) {
            case EXACT_MATCH: {
                // if node has no value, then no need to delete it
                if (nodeFound.getValue() == null) {
                    return false;
                }
                int childrenNum = nodeFound.getChildren().size();
                // if node has more than one child, just delete the associated value, and keep the node
                if (childrenNum > 1) {
                    nodeFound.setValue(null);
                }
                // if node has exactly one child, create a new node to merge this node and its child.
                // the new node has concatenated bytes from this node and its child node,
                // and the new node has the children of the child node and the value from the child node.
                if (childrenNum == 1) {
                    RadixNode node = new RadixNode();
                    byte[] currentNodeEdge = nodeFound.getEdge();
                    RadixNode childNode = nodeFound.getChildren().values().iterator().next();
                    byte[] childNodeEdge = childNode.getEdge();
                    byte[] edge = new byte[currentNodeEdge.length + childNodeEdge.length];
                    System.arraycopy(currentNodeEdge, 0, edge, 0, currentNodeEdge.length);
                    System.arraycopy(childNodeEdge, 0, edge, currentNodeEdge.length, childNodeEdge.length);
                    node.setEdge(edge);
                    node.setChildren(childNode.getChildren());
                    node.setValue(childNode.getValue());
                    searchResult.parentNode.addChild(node, true);
                }
                // if node has no children, delete this node from parent.
                // and if after deletion, the parent itself with only one child left and has no value,
                // then we need also merge the parent and its remaining child.
                else {
                    Iterator<RadixNode> parentChildrenIterator = searchResult.parentNode.getChildren().values().iterator();
                    List<RadixNode> parentChildrenNodes = new ArrayList<>();
                    while (parentChildrenIterator.hasNext()) {
                        RadixNode node = parentChildrenIterator.next();
                        if (node != nodeFound) {
                            parentChildrenNodes.add(parentChildrenIterator.next());
                        }
                    }

                    RadixNode newNode = new RadixNode();
                    // if parent has only one child left and has no value, then we can merge parent as a new node
                    // the new node has concatenated edge, children and value from the remaining child node
                    if (parentChildrenNodes.size() == 1 && searchResult.parentNode.getValue() == null) {
                        RadixNode remainingChild = parentChildrenNodes.get(0);
                        // concatenate parent and remaining child edge
                        byte[] parentEdge = searchResult.parentNode.getEdge();
                        byte[] childEdge = remainingChild.getEdge();
                        byte[] edge = new byte[parentEdge.length + childEdge.length];
                        System.arraycopy(parentEdge, 0, edge, 0, parentEdge.length);
                        System.arraycopy(childEdge, 0, edge, parentEdge.length, childEdge.length);
                        newNode.setEdge(edge);
                        newNode.setChildren(remainingChild.getChildren());
                        newNode.setValue(remainingChild.getValue());
                        searchResult.grandParentNode.addChild(newNode, true);
                    }
                    // parent cannot be merged with remaining children
                    // then just delete the node
                    else {
                        searchResult.parentNode.removeChild(nodeFound);
                    }
                }
                return true;
            }
            default: {
                return false;
            }
        }
    }

    private SearchResult searchInternal(byte[] key)
    {
        RadixNode currentNode = nodes.get(0);
        RadixNode parentNode = null;
        RadixNode grandParentNode = null;
        int bytesMatched = 0, bytesMatchedInNodeFound = 0;

        final int keyLen = key.length;
        outer_loop: while (bytesMatched < keyLen) {
            RadixNode nextNode = currentNode.getChild(key[bytesMatched]);
            if (nextNode == null) {
                break;
            }

            grandParentNode = parentNode;
            parentNode = currentNode;
            currentNode = nextNode;
            bytesMatchedInNodeFound = 0;
            byte[] currentNodeEdge = currentNode.getEdge();
            for (int i = 0, numEdgeBytes = currentNodeEdge.length; i < numEdgeBytes && bytesMatched < keyLen; i++) {
                if (currentNodeEdge[i] != key[bytesMatched]) {
                    break outer_loop;
                }
                bytesMatched++;
                bytesMatchedInNodeFound++;
            }
        }

        return new SearchResult(key, currentNode, bytesMatched, bytesMatchedInNodeFound, parentNode, grandParentNode);
    }

    static class SearchResult
    {
        final byte[] key;
        final RadixNode nodeFound;
        final int bytesMatched;
        final int bytesMatchedInNodeFound;
        final RadixNode parentNode;
        final RadixNode grandParentNode;
        final Type matchType;

        enum Type {
            EXACT_MATCH,            // key exactly matches the node
            KEY_ENDS_AT_MID_EDGE,   // match end before reaching edge end, because key hits end already
            MATCH_END_AT_END_EDGE,  // match end before reaching key end, because edge hits end already
            MATCH_END_AT_MID_EDGE   // match end before reaching key end, because the matching hits end already
        }

        SearchResult(byte[] key, RadixNode nodeFound, int bytesMatched, int bytesMatchedInNodeFound,
                     RadixNode parentNode, RadixNode grandParentNode)
        {
            this.key = key;
            this.nodeFound = nodeFound;
            this.bytesMatched = bytesMatched;
            this.bytesMatchedInNodeFound = bytesMatchedInNodeFound;
            this.parentNode = parentNode;
            this.grandParentNode = grandParentNode;
            this.matchType = match(key, nodeFound, bytesMatched, bytesMatchedInNodeFound);
        }

        private Type match(byte[] key, RadixNode nodeFound, int bytesMatched, int bytesMatchedInNodeFound)
        {
            if (bytesMatched == key.length) {
                if (bytesMatchedInNodeFound == nodeFound.getEdge().length) {
                    return Type.EXACT_MATCH;
                }
                else if (bytesMatchedInNodeFound < nodeFound.getEdge().length) {
                    return Type.KEY_ENDS_AT_MID_EDGE;
                }
            }
            else if (bytesMatched < key.length) {
                if (bytesMatchedInNodeFound == nodeFound.getEdge().length) {
                    return Type.MATCH_END_AT_END_EDGE;
                }
                else if (bytesMatchedInNodeFound < nodeFound.getEdge().length) {
                    return Type.MATCH_END_AT_MID_EDGE;
                }
            }
            throw new IllegalStateException("Unexpected matching type for SearchResult");
        }

        public byte[] getKey()
        {
            return key;
        }

        public RadixNode getNodeFound()
        {
            return nodeFound;
        }

        public int getBytesMatched()
        {
            return bytesMatched;
        }

        public int getBytesMatchedInNodeFound()
        {
            return bytesMatchedInNodeFound;
        }

        public RadixNode getParentNode()
        {
            return parentNode;
        }

        public RadixNode getGrandParentNode()
        {
            return grandParentNode;
        }

        public Type getMatchType()
        {
            return matchType;
        }
    }
}
