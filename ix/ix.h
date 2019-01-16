#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string.h>

#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan

class IX_ScanIterator;
class IXFileHandle;

struct LeafNodeHeader{
    bool isLeaf;
    short freespace;
    short nextLeafId;
    short parentNodeId;
    short startOffset; //record where to insert new record
    short slotNum; //number of slot
};


struct leafSlot{
    short offset;//the offset of record
    short rLeng;//record length
};


struct NonLeafNodeHeader{
    bool isLeaf;
    short freespace;
    short ParentNodeId;
    short startOffset;//record where to insert new key
    short slotNum;
};

struct nonleafSlot{
    short offset;//the offset of stored key
    short klength;//the length of key
};

class IndexManager {

    public:
        static IndexManager* instance();

        // Create an index file.
        RC createFile(const string &fileName);

        // Delete an index file.
        RC destroyFile(const string &fileName);

        // Open an index and return an ixfileHandle.
        RC openFile(const string &fileName, IXFileHandle &ixfileHandle);

        // Close an ixfileHandle for an index.
        RC closeFile(IXFileHandle &ixfileHandle);

        // Insert an entry into the given index that is indicated by the given ixfileHandle.
        RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Delete an entry from the given index that is indicated by the given ixfileHandle.
        RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Initialize and IX_ScanIterator to support a range search
        RC scan(IXFileHandle &ixfileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive,
                IX_ScanIterator &ix_ScanIterator);

        // Print the B+ tree in pre-order (in a JSON record format)
        void printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const;
    protected:
        IndexManager();
        ~IndexManager();

    private:
        static IndexManager *_index_manager;
        PagedFileManager * pfm;
};




class IXFileHandle {
    public:

    // variables to keep counter for each operation
    unsigned ixReadPageCounter;
    unsigned ixWritePageCounter;
    unsigned ixAppendPageCounter;

    // Constructor
    IXFileHandle();

    // Destructor
    ~IXFileHandle();

	// Put the current counter values of associated PF FileHandles into variables
	RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

    FileHandle indexFileHandle;

    int searchNode(const Attribute &attribute, const void *key);

    int appendLeafNode();
    RC appendNonLeafNode();

    RC insertIndexToLeaf(int pageNum, const Attribute &attribute, const void *key, const RID &rid);
    RC insertIndexToNonLeaf(int pageNum, int sPageId, int lPageId, const Attribute &attribute, const void *key);

    RC splitNonLeafNode(int pageNum, int sPageId, int lPageId, const Attribute &attribute, const void *key, void* buffer);

    int getRootPageNum();

    RC setRootPageNum(int rootPageNum);

    bool getPageType(int pageNum);

    leafSlot* inLeafNodeInsertSearch(bool inclusive, void* buffer, LeafNodeHeader* header, Attribute attribute, 
                                  short &slotCount, void* insertKeyValue, int insertKeyLength);

    leafSlot* inLeafNodeIndexSearch(void* buffer, leafSlot* tempSlot, LeafNodeHeader* header, 
                             short &slotCount, const RID &rid);

    RC splitLeafNode(void* buffer, void* pairData, int pairDataLength,
                     LeafNodeHeader* header, void* insertKeyValue, int insertKeyLength, int slotCount, int oldPageNum, Attribute attributeconst, const void *key, const RID &rid);
    
    void updateNonleafNodeChilds(int pageNum);

    RC deleteIndexOfLeaf(int pageNum, const Attribute &attribute, const void *key, const RID &rid);


};

class IX_ScanIterator {
    public:

		// Constructor
        IX_ScanIterator();

        // Destructor
        ~IX_ScanIterator();

        // Get next matching entry
        RC getNextEntry(RID &rid, void *key);

        // Terminate index scan
        RC close();

        RC getCondition(Attribute attribute, const void *lowKey, 
                        const void *highKey, bool lowKeyInclusive, bool highKeyInclusive, IXFileHandle &ixFileHandle);

        RC setStartEnd(int pageStart, int pageEnd, int startSlot, int endSlot);
        
        Attribute attribute;

        const void *lowKey;
        const void *highKey;
        bool lowKeyInclusive;
        bool highKeyInclusive;
        int startPage;
        int startSlot;
        int endPage;
        int endSlot;

        int curPage;
        int curSlot;
        int curNumOfSlot;
        IXFileHandle* ixFileHandle;
};


#endif
