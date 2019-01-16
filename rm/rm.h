
#ifndef _rm_h_
#define _rm_h_

#include <string.h>
#include <vector>
#include "../rbf/rbfm.h"
#include "../ix/ix.h"

using namespace std;

# define RM_EOF (-1)  // end of a scan operator

// RM_ScanIterator is an iterator to go through tuples
class RM_ScanIterator {
public:
  RM_ScanIterator() {};
  ~RM_ScanIterator() {};

  // "data" follows the same format as RelationManager::insertTuple()
  RC getNextTuple(RID &rid, void *data) 
  {
    if(_rbfmsi->getNextRecord(rid, data) != RBFM_EOF)
    {
        return 0;
    } 
    else
    {
        return RM_EOF;
    }
  };

  RC close() {     
    if(_rbfmsi != NULL)
    {
        _rbfmsi->close();
        delete _rbfmsi;
        _rbfmsi = 0;
    }
    return 0; 
  };

  RC setRBFMSI(RBFM_ScanIterator* _rbfmsi)
  {
    this->_rbfmsi = _rbfmsi;
    return 0;
  };
private:
  RBFM_ScanIterator* _rbfmsi;
};

class RM_IndexScanIterator {
  public:
    RM_IndexScanIterator() {};  	// Constructor
    ~RM_IndexScanIterator() {}; 	// Destructor
    // "key" follows the same format as in IndexManager::insertEntry()
    RC getNextEntry(RID &rid, void *key) 
    {
      if(_ixmsi->getNextEntry(rid, key) != IX_EOF)
      {
        return 0;
      }
      else
      {
        return IX_EOF;
      }

    };  	// Get next matching entry
    RC close() {
      _ixmsi->close();
    return 0; };             			// Terminate index scan
    RC setIXMSI(IX_ScanIterator* _ixmsi)
    {

      this->_ixmsi = _ixmsi;
      //cout << "this->_ixmsi " << this->_ixmsi->ixFileHandle->indexFileHandle.getFileName() << endl;

      return 0;
    }
  private:
    IX_ScanIterator* _ixmsi;
};

struct attrIndexPair
{
  string attrName;
  string indexFileName;
};

struct attrRidPair
{
  string attrName;
  RID rid;
};

// Relation Manager
class RelationManager
{
public:
  static RelationManager* instance();

  RC createCatalog();

  RC deleteCatalog();

  RC createTable(const string &tableName, const vector<Attribute> &attrs);

  RC deleteTable(const string &tableName);

  RC getAttributes(const string &tableName, vector<Attribute> &attrs);

  RC insertTuple(const string &tableName, const void *data, RID &rid);

  RC updateIndexesInsert(const string tableName, const void* data, RID &rid);

  RC deleteTuple(const string &tableName, const RID &rid);

  RC updateTuple(const string &tableName, const void *data, const RID &rid);

  RC readTuple(const string &tableName, const RID &rid, void *data);

  // Print a tuple that is passed to this utility method.
  // The format is the same as printRecord().
  RC printTuple(const vector<Attribute> &attrs, const void *data);

  RC readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data);

  // Scan returns an iterator to allow the caller to go through the results one by one.
  // Do not store entire results in the scan iterator.
  RC scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparison type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RM_ScanIterator &rm_ScanIterator);

  RC createIndex(const string &tableName, const string &attributeName);

  RC destroyIndex(const string &tableName, const string &attributeName);

  // indexScan returns an iterator to allow the caller to go through qualified entries in index
  RC indexScan(const string &tableName,
                        const string &attributeName,
                        const void *lowKey,
                        const void *highKey,
                        bool lowKeyInclusive,
                        bool highKeyInclusive,
                        RM_IndexScanIterator &rm_IndexScanIterator);


// Extra credit work (10 points)
public:
  RC addAttribute(const string &tableName, const Attribute &attr);

  RC dropAttribute(const string &tableName, const string &attributeName);


protected:
  RelationManager();
  ~RelationManager();

private: 
  string curFileName;
  vector<Attribute> curDescriptor;
  IndexManager *indexManager;
  RecordBasedFileManager * rbfm;
  const string tableTableName = "Tables";
  const string columnTableName = "Columns";
  const string indexCatelogName = "indexCatelog";
  int findTableIdByName(const string tableName);

  int getActualByteForNullsIndicator(int fieldCount);

  void createIndexRecordDescriptor(vector<Attribute> &recordDescriptor);
  void prepareIndexRecord(int fieldCount, unsigned char *nullFieldsIndicator, const int tableNameLength, 
                          const string tableName, const int attributeNameLength, const string attributeName, 
                          const int indexFileNameLength, const string indexFileName, void *buffer, int *recordSize);
  RC insertIndexRecordTuple(FileHandle &fileHandle, int tableNameLength, 
                          string tableName, int attributeNameLength, string attributeName, 
                           int indexFileNameLength, string indexFileName);
  RC findIndexFileName(const string &tableName,  const string &attribute, string &indexFileName);
  RC analyzeScanIndexRecord(const vector<Attribute> &recordDescriptor, const void *data, vector<attrIndexPair> &pairs);
  RC analyzeScanIndexRecordInDelete(const vector<Attribute> &recordDescriptor, const void *data, vector<attrRidPair> &pairs, RID rid);


  void createTableRecordDescriptor(vector<Attribute> &recordDescriptor);
  void prepareTableRecord(int fieldCount, unsigned char *nullFieldsIndicator, const int tableId, 
                          const int tableNameLength, const string tableName, const int fileNameLength, 
                          const string fileName, void *buffer, int *recordSize);
  RC insertTableTableTuple(FileHandle &fileHandle, int tableId, int tableNameLength, string table, int fileNameLength, string fileName);

  void createColumnRecordDescriptor(vector<Attribute> &recordDescriptor);
  void prepareColumnRecord(int fieldCount, unsigned char *nullFieldsIndicator, const int tableId, 
                          const int columnNameLength, const string columnName, const int columnType, 
                          const int columnLength, const int columnPostion, void *buffer, int *recordSize);
  RC insertColumnTableTuple(FileHandle &fileHandle, int tableId, int columnNameLength, string columnName, int columnType, int columnLength, int columnPosition);
  RC contructRecordDescriptor(vector<Attribute> &recordDescriptor, const string tableName);
  RC analyzeScanColumnRecord(const vector<Attribute> &recordDescriptor, const void *data, vector<Attribute> &attributes);
  RC updateCurAttribute(string tableName);

private:
  static RelationManager *_rm;
};

#endif
