# B+ Tree

Package `bptree` implements an on-disk B+ tree. This implementation of B+ tree can
store keys of variable size (but limited by configured maxKeySize) and `uint64` values.
Since this implementation is meant to act as an indexing scheme for the Kiwi store,
the `uint64` value here is meant to store the offset/record id of the actual data stored
in a data-file.

## Implementation

Each node in the tree is mapped to exactly one page/block (size configurable as multiple
of 4096) in the underlying file. Degree of the tree is computed based on the page size and
key size. Degree might vary slightly between internal nodes and leaf nodes since internal
nodes store only keys and pointers to child nodes whereas leaf nodes store key, value and
pointers to right and left siblings for range scans.

### Page Layouts

* Leaf Node:

    ```plaintext
    --- header section ---
    flags  (1 byte)   - flags to indicate leaf/internal etc.
    count  (2 bytes)  - number of entries in this node
    next   (4 bytes)  - pointer to right sibling
    prev   (4 bytes)  - pointer to left sibling
    ---- header ends ----
    value1 (8 bytes)  - value associated with the first key
    key1Sz (2 bytes)  - size of the first key
    key1   (variable) - first key itself
    value2 (8 bytes)  - value associated with the second key
    key2Sz (2 bytes)  - size of the second key
    key2   (variable) - second key itself
    ...
    ```

* Internal Node:

    ```plaintext
    --- header section ---
    flags  (1 byte)   - flags to indicate leaf/internal etc.
    count  (2 bytes)  - number of entries in this node
    ---- header ends ----
    P0     (4 bytes)  - pointer to the 0th child
    key1Sz (2 bytes)  - size of key 1
    key1   (variable) - key 1 itself
    P1     (4 bytes)  - pointer to the 1st child
    key2Sz (2 bytes)  - size of key 2
    key2   (variable) - key 2 itself
    ...
    ```
