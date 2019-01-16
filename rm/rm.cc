#include "rm.h"
#define FAILURE -1
#define SUCCESS 0

RC findAttribute(const string &tableName, const string &attributeName, vector<Attribute> &attrs, Attribute &attr);


RelationManager* RelationManager::_rm = 0;
RelationManager* RelationManager::instance()
{
    if(!_rm)
        _rm = new RelationManager();
    
    return _rm;
}

RelationManager::RelationManager()
{
    rbfm = RecordBasedFileManager::instance();
    indexManager = IndexManager::instance();
}

RelationManager::~RelationManager()
{
}

// Calculate actual bytes for nulls-indicator for the given field counts
int RelationManager::getActualByteForNullsIndicator(int fieldCount) {

    return ceil((double) fieldCount / CHAR_BIT);
}

void RelationManager::createIndexRecordDescriptor(vector<Attribute> &recordDescriptor)
{
    Attribute attr;

    attr.name = "table-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)50;
    recordDescriptor.push_back(attr);

    attr.name = "column-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)50;
    recordDescriptor.push_back(attr);

    attr.name = "indexFile-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)50;
    recordDescriptor.push_back(attr);
}


void RelationManager::createColumnRecordDescriptor(vector<Attribute> &recordDescriptor)
{
    Attribute attr;
    attr.name = "table-id";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    recordDescriptor.push_back(attr);

    attr.name = "column-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)50;
    recordDescriptor.push_back(attr);

    attr.name = "column-type";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    recordDescriptor.push_back(attr);

    attr.name = "column-length";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    recordDescriptor.push_back(attr);

    attr.name = "column-postion";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    recordDescriptor.push_back(attr);
}

void RelationManager::prepareIndexRecord(int fieldCount, unsigned char *nullFieldsIndicator, const int tableNameLength, 
                          const string tableName, const int attributeNameLength, const string attributeName, 
                          const int indexFileNameLength, const string indexFileName, void *buffer, int *recordSize)
{
    int offset = 0;
    bool nullBit = false;
    int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(fieldCount);
    memcpy((char *)buffer + offset, nullFieldsIndicator, nullFieldsIndicatorActualSize);
    offset += nullFieldsIndicatorActualSize;

    // set tableName
    nullBit = nullFieldsIndicator[0] & (1 << 7);
    if (!nullBit) 
    {
        memcpy((char *)buffer + offset, &tableNameLength, sizeof(int));
        offset += sizeof(int);
        memcpy((char *)buffer + offset, tableName.c_str(), tableNameLength);
        offset += tableNameLength;
    }

    // set attributeName
    nullBit = nullFieldsIndicator[0] & (1 << 6);
    if (!nullBit) 
    {
        memcpy((char *)buffer + offset, &attributeNameLength, sizeof(int));
        offset += sizeof(int);
        memcpy((char *)buffer + offset, attributeName.c_str(), attributeNameLength);
        offset += attributeNameLength;
    }

    // set indexFileName
    nullBit = nullFieldsIndicator[0] & (1 << 5);
    if (!nullBit) 
    {
        memcpy((char *)buffer + offset, &indexFileNameLength, sizeof(int));
        offset += sizeof(int);
        memcpy((char *)buffer + offset, indexFileName.c_str(), indexFileNameLength);
        offset += indexFileNameLength;
    }

    *recordSize = offset;
    
}

RC RelationManager::insertIndexRecordTuple(FileHandle &fileHandle, int tableNameLength, 
                          string tableName, int attributeNameLength, string attributeName, 
                          int indexFileNameLength, string indexFileName)
{
    int recordSize = 0;
    void *record = malloc(200);
    memset(record, 0, 200);
    int rc;
    RID rid; 
    vector<Attribute> recordDescriptor;
    createIndexRecordDescriptor(recordDescriptor);
    // Initialize a NULL field indicator
    int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(recordDescriptor.size());
    unsigned char *nullsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
    memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);

    // Insert a record into a file and print the record
    prepareIndexRecord(recordDescriptor.size(), nullsIndicator, tableNameLength, tableName, attributeNameLength, attributeName, indexFileNameLength, indexFileName, record, &recordSize);
    // rbfm->printRecord(recordDescriptor, record);
    rc = rbfm->insertRecord(fileHandle, recordDescriptor, record, rid);
    free(record);
    return SUCCESS;
}

RC RelationManager::createIndex(const string &tableName, const string &attributeName)
{
    /***
     * 1. create index file, fileName = "tableName.index"
     * 2. insert the indexRecord of table.column.indexFileName in indexCatelog to tableName.index
     */

    // 1. construct the indexFileName
    string indexFileName;
    string nameEnd = ".index";
    indexFileName = tableName + attributeName + nameEnd;
    if(indexManager->createFile(indexFileName))
    {
        return -1;
    }

    // 2. insert the indexRecord
    FileHandle fileHandle;
    rbfm->openFile(indexCatelogName, fileHandle);
    insertIndexRecordTuple(fileHandle, tableName.size(), tableName, 
                           attributeName.size(), attributeName, 
                           indexFileName.size(), indexFileName);
    rbfm->closeFile(fileHandle);

    // 3. load the records into B+ Tree
    vector<string> attrs;
    RM_ScanIterator rmsi;

    attrs.push_back(attributeName);
    RC rc = scan(tableName, "", NO_OP, NULL, attrs, rmsi); 
    if(rc)
    {
        return FAILURE;
    }
    void *returnedData = malloc(PAGE_SIZE);
    RID rid;

    IXFileHandle ixfileHandle;
    rc = indexManager->openFile(indexFileName, ixfileHandle);

    vector<Attribute> v_attrs;
    Attribute attr;
    getAttributes(tableName, v_attrs);
    findAttribute(tableName, attributeName, v_attrs, attr);

    int counter = 0;
    while(rmsi.getNextTuple(rid, returnedData) != RM_EOF)
    {
        counter++;
        //cout<<"returned data: "<<*(float*)returnedData<<endl;
        // cout<<"insert number: "<<counter<<endl;
        indexManager->insertEntry(ixfileHandle, attr, (byte*)returnedData+1, rid);
    }
    indexManager->closeFile(ixfileHandle);
    rmsi.close();    
    
	return 0;
}
 RC RelationManager::destroyIndex(const string &tableName, const string &attributeName)
{
    // 1. construct the indexFileName
    string indexFileName;
    string nameEnd = ".index";
    indexFileName = tableName + attributeName + nameEnd;
    if(indexManager->destroyFile(indexFileName))
    {
        return -1;
    }

    // 2. delete the records in the indexCatelog
    // 2.1. create indexTable descriptor
    vector<Attribute> recordDescriptor; 
    createIndexRecordDescriptor(recordDescriptor);

    // 2.2. open indexCatelog by handle
    FileHandle handle; 
    if(rbfm->openFile(indexCatelogName, handle))
    {
        return FAILURE;
    }
    // 2.3. construct the tableName key to scan: add the length to the void*
    int tableNameLength = tableName.size();
    void* value = malloc(tableNameLength+sizeof(int)+1); 
    memset(value,0,tableNameLength+sizeof(int)+1);
    memcpy(value,&tableNameLength,sizeof(int));
    memcpy((char*)value+sizeof(int), (char*)tableName.data(), tableNameLength);

    vector<string> selectAttrs; 
    // 2.4. construct the selected attrs [attribute, IndexFileNames]

    selectAttrs.push_back("column-name");

    // 2.5. scan the <attribute, indexFileName> pairs into pairs
    RBFM_ScanIterator rbfm_scan; 
    rbfm->scan(handle, recordDescriptor, "table-name", EQ_OP, value, selectAttrs, rbfm_scan);

    RID rid;
    void * record = malloc(PAGE_SIZE);
    memset(record, 0, sizeof(PAGE_SIZE));
    vector<attrRidPair> pairs;

    while(rbfm_scan.getNextRecord(rid, record) != RBFM_EOF)
    {
        analyzeScanIndexRecordInDelete(recordDescriptor, record, pairs, rid);
        memset(record, 0, sizeof(PAGE_SIZE));
    }

    rbfm_scan.close();
    handle.closeFile();
    free(record);

    // 6. find and delete the record in the pairs
    for(int i = 0; i < pairs.size(); i++)
    {
        if(pairs[i].attrName ==attributeName)
        {
            deleteTuple(tableName, pairs[i].rid);
            return SUCCESS;
        }
    }

    return FAILURE;
}

/***
 * This function will find the indexFileName in the indexCatelogTable given tableName and attributeName.
 * 1. find the records of given tableName, get the records list.
 * 2. locate the record of given attributeName and extract the indexFileName
 */
RC RelationManager::findIndexFileName(const string &tableName, const string &attribute, string &indexFileName)
{

    // 1. create indexTable descriptor
    vector<Attribute> recordDescriptor; 
    createIndexRecordDescriptor(recordDescriptor);

    // 2. open indexCatelog by handle
    FileHandle handle; 
    if(rbfm->openFile(indexCatelogName, handle))
    {
        return FAILURE;
    }

    // 3. construct the tableName key to scan: add the length to the void*
    int tableNameLength = tableName.size();
    void* value = malloc(tableNameLength+sizeof(int)+1); 
    memset(value,0,tableNameLength+sizeof(int)+1);
    memcpy(value,&tableNameLength,sizeof(int));
    memcpy((char*)value+sizeof(int), (char*)tableName.data(), tableNameLength);

    vector<string> selectAttrs; 
    // 4. construct the selected attrs [attribute, IndexFileNames]
    for(int i = 1; i < recordDescriptor.size(); i++)
    {
        selectAttrs.push_back(recordDescriptor[i].name);
    }

    // 5. scan the <attribute, indexFileName> pairs into pairs
    RBFM_ScanIterator rbfm_scan; 
    rbfm->scan(handle, recordDescriptor, "table-name", EQ_OP, value, selectAttrs, rbfm_scan);

    RID rid;
    void * record = malloc(PAGE_SIZE);
    memset(record, 0, sizeof(PAGE_SIZE));
    vector<attrIndexPair> pairs;

    while(rbfm_scan.getNextRecord(rid, record) != RBFM_EOF)
    {
        analyzeScanIndexRecord(recordDescriptor, record, pairs);
        memset(record, 0, sizeof(PAGE_SIZE));
    }
    rbfm_scan.close();
    handle.closeFile();
    free(record);

    // 6. find the indexFileName in the pairs
    for(int i = 0; i < pairs.size(); i++)
    {
        if(pairs[i].attrName == attribute)
        {
            indexFileName = pairs[i].indexFileName;
            return SUCCESS;
        }
    }

    return FAILURE;
}

RC findAttribute(const string &tableName, const string &attributeName, vector<Attribute> &attrs, Attribute &attr)
{
    // cout << "findAttribute" << endl;
    // cout << "attrs.size(): "<< attrs.size() << endl;
    for(int i = 0; i < attrs.size(); i++)
    {
        // cout << "i: " << i << " " << attrs[i].name << endl;
        if(attrs[i].name == attributeName)
        {
            attr = attrs[i];
            return SUCCESS;
        }
    }
    return FAILURE;
}

 RC RelationManager::indexScan(const string &tableName,
                      const string &attributeName,
                      const void *lowKey,
                      const void *highKey,
                      bool lowKeyInclusive,
                      bool highKeyInclusive,
                      RM_IndexScanIterator &rm_IndexScanIterator)
{
    string indexFileName;
    vector<Attribute> attrs;
    Attribute attr;
    IX_ScanIterator* ix_ScanIterator = new IX_ScanIterator();

    getAttributes(tableName, attrs);
    // cout << "after getAttributes"<< endl;
    findIndexFileName(tableName, attributeName, indexFileName);
    // cout << "after findIndexFileName: " << indexFileName << endl;
    findAttribute(tableName, attributeName, attrs, attr);
    // cout << "indexFileName" << indexFileName << endl;
    IXFileHandle* ixFileHandle = new IXFileHandle();
    RC rc = indexManager->openFile(indexFileName, *ixFileHandle);
    // cout << "begin open" <<endl;

    rc = indexManager->scan(*ixFileHandle, attr, lowKey, highKey, 
                            lowKeyInclusive, highKeyInclusive, *ix_ScanIterator);

    if(rc)
    {
        ixFileHandle->indexFileHandle.closeFile();
        return FAILURE;
    }
    rm_IndexScanIterator.setIXMSI(ix_ScanIterator);

    
    return SUCCESS;
}


void RelationManager::prepareColumnRecord(int fieldCount, unsigned char *nullFieldsIndicator, const int tableId, 
                                        const int columnNameLength, const string columnName, const int columnType, 
                                        const int columnLength, const int columnPostion, void *buffer, int *recordSize)
{
    int offset = 0;

    bool nullBit = false;
    int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(fieldCount);

    memcpy((char *)buffer + offset, nullFieldsIndicator, nullFieldsIndicatorActualSize);
    offset += nullFieldsIndicatorActualSize;

    // set tableId
    nullBit = nullFieldsIndicator[0] & (1 << 7);
    if (!nullBit) 
    {
        memcpy((char *)buffer + offset, &tableId, sizeof(int));
        offset += sizeof(int);
    }

    // set columnName
    nullBit = nullFieldsIndicator[0] & (1 << 6);
    if (!nullBit) 
    {
        memcpy((char *)buffer + offset, &columnNameLength, sizeof(int));
        offset += sizeof(int);
        memcpy((char *)buffer + offset, columnName.c_str(), columnNameLength);
        offset += columnNameLength;
    }

    // set columnType
    nullBit = nullFieldsIndicator[0] & (1 << 5);
    if (!nullBit) 
    {
        memcpy((char *)buffer + offset, &columnType, sizeof(int));
        offset += sizeof(int);
    }

    // set columnLength
    nullBit = nullFieldsIndicator[0] & (1 << 4);
    if (!nullBit) 
    {
        memcpy((char *)buffer + offset, &columnLength, sizeof(int));
        offset += sizeof(int);
    }

    // set columnPostion
    nullBit = nullFieldsIndicator[0] & (1 << 3);
    if (!nullBit) 
    {
        memcpy((char *)buffer + offset, &columnPostion, sizeof(int));
        offset += sizeof(int);
    }

    *recordSize = offset;
}
RC RelationManager::insertColumnTableTuple(FileHandle &fileHandle, int tableId, int columnNameLength, 
                                        string columnName, int columnType, int columnLength, int columnPosition)
{
    int recordSize = 0;
    void *record = malloc(100);
    memset(record, 0, 100);
    int rc;
    RID rid; 
    vector<Attribute> recordDescriptor;
    createColumnRecordDescriptor(recordDescriptor);
    // Initialize a NULL field indicator
    int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(recordDescriptor.size());
    unsigned char *nullsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
    memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);

    // Insert a record into a file and print the record
    prepareColumnRecord(recordDescriptor.size(), nullsIndicator, tableId, columnNameLength, columnName, columnType, columnLength, columnPosition, record, &recordSize);
    // rbfm->printRecord(recordDescriptor, record);
    rc = rbfm->insertRecord(fileHandle, recordDescriptor, record, rid);
    free(record);
    return SUCCESS;
}
  

void RelationManager::createTableRecordDescriptor(vector<Attribute> &recordDescriptor)
{
    Attribute attr;
    attr.name = "table-id";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    recordDescriptor.push_back(attr);

    attr.name = "table-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)50;
    recordDescriptor.push_back(attr);

    attr.name = "file-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)50;
    recordDescriptor.push_back(attr);
}

// Function to prepare the data in the correct form to be inserted/read
void RelationManager::prepareTableRecord(int fieldCount, unsigned char *nullFieldsIndicator, const int tableId, 
                        const int tableNameLength, const string tableName, const int fileNameLength, 
                        const string fileName, void *buffer, int *recordSize)
{
    int offset = 0;

    bool nullBit = false;
    int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(fieldCount);

    memcpy((char *)buffer + offset, nullFieldsIndicator, nullFieldsIndicatorActualSize);
    offset += nullFieldsIndicatorActualSize;

    nullBit = nullFieldsIndicator[0] & (1 << 7);
    if (!nullBit) 
    {
        memcpy((char *)buffer + offset, &tableId, sizeof(int));
        offset += sizeof(int);
    }
    nullBit = nullFieldsIndicator[0] & (1 << 6);
    if (!nullBit) 
    {
        memcpy((char *)buffer + offset, &tableNameLength, sizeof(int));
        offset += sizeof(int);
        memcpy((char *)buffer + offset, tableName.c_str(), tableNameLength);
        offset += tableNameLength;
    }
    nullBit = nullFieldsIndicator[0] & (1 << 5);
    if (!nullBit) 
    {
        memcpy((char *)buffer + offset, &fileNameLength, sizeof(int));
        offset += sizeof(int);
        memcpy((char *)buffer + offset, fileName.c_str(), fileNameLength);
        offset += fileNameLength;
    }
    // cout << "recordSize: " << offset << endl;
    *recordSize = offset;
}

RC RelationManager::insertTableTableTuple(FileHandle &fileHandle, int tableId, int tableNameLength, string tableName, int fileNameLength, string fileName)
{
    int recordSize = 0;
    void *record = malloc(200);
    int rc;
    RID rid; 
    vector<Attribute> recordDescriptor;
    createTableRecordDescriptor(recordDescriptor);
    // Initialize a NULL field indicator
    int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(recordDescriptor.size());
    unsigned char *nullsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
    memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);

    // Insert a record into a file and print the record
    prepareTableRecord(recordDescriptor.size(), nullsIndicator, tableId, tableNameLength, tableName, fileNameLength, fileName,record, &recordSize);
    // rbfm->printRecord(recordDescriptor, record);
    rc = rbfm->insertRecord(fileHandle, recordDescriptor, record, rid);
    free(record);
    return SUCCESS;
}

RC RelationManager::analyzeScanIndexRecordInDelete(const vector<Attribute> &recordDescriptor, const void *data, vector<attrRidPair> &pairs, RID rid) 
{
    // cout << "delete Index" << endl;
    int offset = 0;
    attrRidPair p;

    //read the nullIndicator
    byte* nullIndicator = (byte*)data + offset;
    int nullNum = ceil((double)recordDescriptor.size()/8);
    offset += nullNum;

    //check the null pointer
    byte* nTemp = nullIndicator;
    bool nBit = *nTemp & (1<<7);
    if(nBit){
        // print info
        // cout<< recordDescriptor[i].name<<":"<<"NULL"<<"\t";
        return FAILURE; // ???
    }
    int* charlength = (int*)((char*)data+offset);
    offset += 4;

    //output the varchar
    char* charvalue = (char*)data + offset;
    char* outputstr = (char*)malloc(*charlength+1);
    memset(outputstr, 0, *charlength+1);
    memcpy(outputstr,charvalue,*charlength);

    p.attrName = outputstr;
    p.rid = rid;
    // cout << "p.attriName: " << p.attrName << endl;

    free(outputstr);

    pairs.push_back(p);

    return 0;
}

RC RelationManager::analyzeScanIndexRecord(const vector<Attribute> &recordDescriptor, const void *data, vector<attrIndexPair> &pairs) 
{
    int offset = 0;
    attrIndexPair p;

    //read the nullIndicator
    byte* nullIndicator = (byte*)data + offset;
    int nullNum = ceil((double)recordDescriptor.size()/8);
    offset += nullNum;
    for(int i = 1; i<recordDescriptor.size(); i++)
    {
        // cout << recordDescriptor[i].name << endl;
        //check the null pointer
        byte* nTemp = nullIndicator + (i/8);
        bool nBit = *nTemp & (1<<(7-i%8));
        if(nBit){
            // print info
            // cout<< recordDescriptor[i].name<<":"<<"NULL"<<"\t";
            continue;
        }
        int* charlength = (int*)((char*)data+offset);
        offset += 4;
        //output the varchar
        // cout << *charlength << endl;
        char* charvalue = (char*)data + offset;
        char* outputstr = (char*)malloc(*charlength+1);
        memset(outputstr, 0, *charlength+1);
        memcpy(outputstr,charvalue,*charlength);
        // cout << outputstr << endl;
        if(i == 1)
        {
            // cout << "outputstr: " << outputstr << endl;
            p.attrName = outputstr;
            // cout << "p.attriName: " << p.attrName << endl;
        }
        if(i == 2)
        {
            // cout << "outputstr: " << outputstr << endl;
            p.indexFileName = outputstr;
            // cout << "p.indexFileName: " << p.indexFileName << endl;

        }   
        free(outputstr);
        offset += *charlength;
    }
    pairs.push_back(p);

    return 0;
}

RC RelationManager::analyzeScanColumnRecord(const vector<Attribute> &recordDescriptor, const void *data, vector<Attribute> &attributes) 
{
    int offset = 0;
    Attribute newAttr;

    //read the nullIndicator
    byte* nullIndicator = (byte*)data + offset;
    int nullNum = ceil((double)recordDescriptor.size()/8);
    offset += nullNum;

    for(int i = 0; i<recordDescriptor.size(); i++)
    {
        //check the null pointer
        byte* nTemp = nullIndicator + (i/8);
        bool nBit = *nTemp & (1<<(7-i%8));
        if(nBit){
            // print info
            // cout<< recordDescriptor[i].name<<":"<<"NULL"<<"\t";
            continue;
        }
        if(recordDescriptor[i].type == TypeInt){
            int* intvalue = (int*)((char*)data + offset);

            if(i == 2)
            {
                newAttr.type = AttrType(*intvalue);
            }
            if(i == 3)
            {
                newAttr.length = *intvalue;
            }
            // cout<<recordDescriptor[i].name<<":"<<*intvalue<<"\t";
            //move the offset
            offset += recordDescriptor[i].length;
        }
        else if(recordDescriptor[i].type == TypeReal){
            float* floatvalue = (float*)((char*)data + offset);
            // cout<<recordDescriptor[i].name<<":"<<*floatvalue<<"\t";
            //move the offset
            offset += recordDescriptor[i].length;
        }
        else if(recordDescriptor[i].type == TypeVarChar){
            //check the varchar length
            int* charlength = (int*)((char*)data+offset);
            offset += 4;

            //output the varchar
            char* charvalue = (char*)data + offset;
            char* outputstr = (char*)malloc(*charlength+1);
            memset(outputstr, 0, *charlength+1);
            memcpy(outputstr,charvalue,*charlength);

            if(i == 1)
            {
                newAttr.name = outputstr;
            }
            
            free(outputstr);
            offset += *charlength;
        }

    }
    attributes.push_back(newAttr);

    return 0;
}

RC RelationManager::updateCurAttribute(string tableName)
{
    if(tableName != curFileName)
    {
        curFileName = tableName;
        vector<Attribute>().swap(curDescriptor);
        if(getAttributes(tableName, curDescriptor))
        {
            // cout << "getAttribute fail" << endl;
            return FAILURE;
        }
        return SUCCESS;
    }
    return SUCCESS;
    
}

RC RelationManager::createCatalog()
{
    int tableTableId;
    int columnTableId;
    int indexTableId;
    // 1. create tableTable columnTable indexCatelog
    if(rbfm->createFile(tableTableName))
    {
        // cout << "fail to create tableTable" << endl;
        return FAILURE;
    }
    if(rbfm->createFile(columnTableName))
    {
        // cout << "fail to create tableTable" << endl;
        return FAILURE;
    }
    if(rbfm->createFile(indexCatelogName))
    {
        return FAILURE;
    }
    // 2. insert tableRecord into tableTable
    FileHandle fileHandle;
    rbfm->openFile(tableTableName, fileHandle);
    tableTableId = rbfm->getWriteAppendCounterValue(fileHandle) + 1;
    insertTableTableTuple(fileHandle, tableTableId, 6, "Tables", 6, "Tables");
    columnTableId = rbfm->getWriteAppendCounterValue(fileHandle) + 1;
    insertTableTableTuple(fileHandle, columnTableId, 7, "Columns", 7, "Columns");
    indexTableId = rbfm->getWriteAppendCounterValue(fileHandle) + 1;
    insertTableTableTuple(fileHandle, indexTableId, 12 ,"indexCatelog", 12, "indexCatelog");
    rbfm->closeFile(fileHandle);

    // 3. insert columnRecord into columnTable
    FileHandle fileHandle2;
    rbfm->openFile(columnTableName, fileHandle2);
    insertColumnTableTuple(fileHandle2, tableTableId, 8, "table-id", TypeInt, 4, 1);
    insertColumnTableTuple(fileHandle2, tableTableId, 10, "table-name", TypeVarChar, 50, 2);
    insertColumnTableTuple(fileHandle2, tableTableId, 9, "file-name", TypeVarChar, 50, 3);
    insertColumnTableTuple(fileHandle2, columnTableId, 8, "table-id", TypeInt, 4, 1);
    insertColumnTableTuple(fileHandle2, columnTableId, 11, "column-name",  TypeVarChar, 50, 2);
    insertColumnTableTuple(fileHandle2, columnTableId, 11, "column-type", TypeInt, 4, 3);
    insertColumnTableTuple(fileHandle2, columnTableId, 13, "column-length", TypeInt, 4, 4);
    insertColumnTableTuple(fileHandle2, columnTableId, 15, "column-position", TypeInt, 4, 5);
    rbfm->closeFile(fileHandle2);

    return SUCCESS;
}

RC RelationManager::deleteCatalog()
{
    if(rbfm->destroyFile(tableTableName))
    {
        return FAILURE;
    }
    if(rbfm->destroyFile(columnTableName))
    {
        return FAILURE;
    }
    if(rbfm->destroyFile(indexCatelogName))
    {
        return FAILURE;
    }

    return SUCCESS;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
    int newTableId;
    // 1. create the table RBF file
    if(rbfm->createFile(tableName))
    {
        // cout << "fail to create table: "<< tableName << endl;
        return FAILURE;
    }
    // 2. insert tableRecord into tableTable
    FileHandle fileHandle;
    rbfm->openFile(tableTableName, fileHandle);
    newTableId = rbfm->getWriteAppendCounterValue(fileHandle) + 1;
    // cout << "new table id: " << newTableId << endl;
    
    insertTableTableTuple(fileHandle, newTableId, tableName.length(), tableName, tableName.length(), tableName);
    rbfm->closeFile(fileHandle);

    FileHandle fileHandle2;
    rbfm->openFile(columnTableName, fileHandle2);
    for(int i = 0; i < attrs.size(); i++)
    {
        insertColumnTableTuple(fileHandle2, newTableId, attrs[i].name.length() , attrs[i].name, attrs[i].type, attrs[i].length, i+1);
    }
    rbfm->closeFile(fileHandle2);


    return SUCCESS;
}

RC RelationManager::deleteTable(const string &tableName)
{
    if(tableName == "Tables" || tableName == "Columns")
    {
        return FAILURE;
    }
    //delete the tableTableRecord
    // just delete the actual file, not tableTable record
    if(rbfm->destroyFile(tableName))
    {
        return FAILURE;
    }

    return SUCCESS;
}

// untested ???
int RelationManager:: findTableIdByName(const string tableName)
{
    vector<Attribute> recordDescriptor; // table record descriptor
    createTableRecordDescriptor(recordDescriptor);

    FileHandle handle; // tableTable file handle
    rbfm->openFile(tableTableName, handle);

    RBFM_ScanIterator rbfm_scan; // scanIterator

    void* value = malloc(tableName.size()+5); // construct the value
    memset(value,0,tableName.size()+5);
    int length = tableName.size();
    memcpy(value,&length,4);
    memcpy((char*)value+4, (char*)tableName.data(), tableName.size()); // untested ???

    vector<string> selectAttrs; // construct the selected attrs
    string strId = "table-id";
    selectAttrs.push_back(strId);

    rbfm->scan(handle, recordDescriptor, "table-name", EQ_OP, value, selectAttrs, rbfm_scan);

    RID rid;
    void *idData = malloc(sizeof(int));
    int tableId;
    if(rbfm_scan.getNextRecord(rid, idData) == RBFM_EOF)
    {
        // cout << "can not find the id" << endl;
        return -1;
    } // ???
    memcpy(&tableId, (char*)idData+1, sizeof(int)); // untested ???
    // cout << "get id: " << tableId << endl;
    rbfm_scan.close();

    free(value);
    free(idData);
    handle.closeFile();

    return tableId;
}



RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
    // 1. find table id
    // cout << "GetAttributetableName: " << tableName << endl;
    

    int tableId = findTableIdByName(tableName);
    if(tableId == -1)
    {
        return FAILURE;
    }

    // 2. find corresponding attributes
    vector<Attribute> recordDescriptor; // table record descriptor
    createColumnRecordDescriptor(recordDescriptor);

    FileHandle handle; // tableTable file handle
    if(rbfm->openFile(columnTableName, handle))
    {
        return FAILURE;
    }
    // cout << "tableId: " << tableId << endl;

    RBFM_ScanIterator rbfm_scan; // scanIterator

    void* value = malloc(sizeof(int)); // construct the key value: table id
    memcpy(value, &tableId, sizeof(int)); 

    vector<string> selectAttrs; // construct the selected attrs
    for(int i = 0; i < recordDescriptor.size(); i++)
    {
        selectAttrs.push_back(recordDescriptor[i].name);
    }
    rbfm->scan(handle, recordDescriptor, "table-id", EQ_OP, value, selectAttrs, rbfm_scan);
    RID rid;
    void * record = malloc(PAGE_SIZE);
    memset(record, 0, sizeof(PAGE_SIZE));
    

    Attribute attr;
    // untested ??
    while(rbfm_scan.getNextRecord(rid, record) != RBFM_EOF)
    {
        analyzeScanColumnRecord(recordDescriptor, record, attrs);
        memset(record, 0, sizeof(PAGE_SIZE));
    }
    rbfm_scan.close();
    handle.closeFile();
    free(record);


    return SUCCESS;
}

/***
 * 1. find all the attributes of current table which has indexes
 * 2. insert the entry into these B+ trees
 */
RC RelationManager::updateIndexesInsert(const string tableName, const void* data, RID &rid)
{
    //cout << "updateIndexesInsert" << endl;
    string indexFileName;
    IXFileHandle IXFileHandle;
    
    int rNum = curDescriptor.size();
    int nullbytenum = ceil((double)rNum/8);
    char* nullindicator = (char*)data;
    //dataptr points to the real data
    char* dataptr = (char*)data + nullbytenum; 
    
    for(int i = 0; i < rNum; i++)
    {
        char* nTemp = nullindicator + (i/8);
        bool nBit = *nTemp & (1<<(7-i%8));
        if(nBit)
        {
            continue;
        }
        void* key = NULL;

        if(curDescriptor[i].type == TypeVarChar)
        {
            int varCharLength;
            memcpy(&varCharLength, dataptr, sizeof(int));
            key = malloc(varCharLength);
            dataptr += sizeof(int);
            memcpy(key, dataptr, varCharLength);
            dataptr += varCharLength;
        }
        else
        {
            key = malloc(sizeof(int));
            memcpy(key, dataptr, sizeof(int));
            dataptr += sizeof(int);
        }
        if(!findIndexFileName(tableName, curDescriptor[i].name, indexFileName))
        {
            // cout<<"index file name: "<<i<<"\t"<<indexFileName<<endl;
            
            if(indexManager->openFile(indexFileName, IXFileHandle))
            {
                // cout << "cannot open file: " << indexFileName << endl;
                return FAILURE;
            }
            if(indexManager->insertEntry(IXFileHandle, curDescriptor[i], key, rid))
            {
                // cout << "insertFailure" << endl;

                return FAILURE;
            }
            IXFileHandle.indexFileHandle.closeFile();
        }
        free(key);
    }
    return SUCCESS;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
    if(tableName == "Tables" || tableName == "Columns")
    {
        // cout << "do not have access to system " << endl;
        return FAILURE;
    }
    FileHandle handle;
    if(rbfm->openFile(tableName, handle))
    {
        // cout << "open file fail" << endl;
        perror("error");
        return FAILURE;
    }
    updateCurAttribute(tableName);

    if(rbfm->insertRecord(handle, curDescriptor, data, rid))
    {
        // cout << "insertRecord fail"<< endl;
        handle.closeFile();
        return FAILURE;
    }
    if(updateIndexesInsert(tableName, data, rid)){
        // cout<<"insert index error"<<endl;
        return -1;
    }
    handle.closeFile();


    return SUCCESS;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
    if(tableName == "Tables" || tableName == "Columns")
    {
        return FAILURE;
    }
    FileHandle handle;
    if(rbfm->openFile(tableName, handle))
    {
        return FAILURE;
    }
    updateCurAttribute(tableName);    

    if(rbfm->deleteRecord(handle, curDescriptor, rid))
    {
        handle.closeFile();
        return FAILURE;
    }
    if(handle.closeFile())
        return FAILURE;

    return SUCCESS;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
    if(tableName == "Tables" || tableName == "Columns")
    {
        return FAILURE;
    }
    FileHandle handle;
    if(rbfm->openFile(tableName, handle))
    {
        return FAILURE;
    }

    updateCurAttribute(tableName);

    if(rbfm->updateRecord(handle, curDescriptor, data, rid))
    {
        handle.closeFile();
        return FAILURE;
    }
    if(handle.closeFile())
        return FAILURE;

    return SUCCESS;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
    if(tableName == "Tables" || tableName == "Columns")
    {
        return FAILURE;
    }
    FileHandle handle;
    if(rbfm->openFile(tableName, handle))
    {
        return FAILURE;
    }

    updateCurAttribute(tableName);
    // cout<<"reading rid: "<<rid.pageNum<<" "<<rid.slotNum<<endl;
    if(rbfm->readRecord(handle, curDescriptor, rid, data))
    {
        handle.closeFile();
        return FAILURE;
    }
    
    handle.closeFile();

    return SUCCESS;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
    rbfm->printRecord(attrs, data);
	return SUCCESS;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
    FileHandle handle;
    if(rbfm->openFile(tableName, handle))
    {
        return FAILURE;
    }
    vector<Attribute> recordDescriptor;
    if(getAttributes(tableName, recordDescriptor))
    {
        return FAILURE;
    }
    if(rbfm->readAttribute(handle, recordDescriptor, rid, attributeName, data))
    {
        handle.closeFile();
        return FAILURE;
    }
    handle.closeFile();

    return SUCCESS;
    
}

RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  
      const void *value,                    
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
    FileHandle fileHandle;
    if(rbfm->openFile(tableName, fileHandle) != 0)
    {
        return FAILURE;
    }
    RBFM_ScanIterator* _rbfmsi = new RBFM_ScanIterator();
    updateCurAttribute(tableName);
    // cout<<"within rm scan"<<endl;
    if(rbfm->scan(fileHandle, curDescriptor, conditionAttribute, compOp, value, attributeNames, *_rbfmsi) != 0)
    {
        fileHandle.closeFile();
        return FAILURE;
    }
    rm_ScanIterator.setRBFMSI(_rbfmsi);
    return SUCCESS;
}

// Extra credit work
RC RelationManager::dropAttribute(const string &tableName, const string &attributeName)
{
    return -1;
}

// Extra credit work
RC RelationManager::addAttribute(const string &tableName, const Attribute &attr)
{
    return -1;
}



