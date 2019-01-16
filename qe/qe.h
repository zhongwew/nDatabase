#ifndef _qe_h_
#define _qe_h_

#include <vector>

#include "../rbf/rbfm.h"
#include "../rm/rm.h"
#include "../ix/ix.h"
#include <map>

#define QE_EOF (-1)  // end of the index scan

using namespace std;

typedef enum{ MIN=0, MAX, COUNT, SUM, AVG } AggregateOp;
const unsigned recordMax = 200;

// The following functions use the following
// format for the passed data.
//    For INT and REAL: use 4 bytes
//    For VARCHAR: use 4 bytes for the length followed by the characters

struct Value {
    AttrType type;          // type of value
    void     *data;         // value
};


struct Condition {
    string  lhsAttr;        // left-hand side attribute
    CompOp  op;             // comparison operator
    bool    bRhsIsAttr;     // TRUE if right-hand side is an attribute and not a value; FALSE, otherwise.
    string  rhsAttr;        // right-hand side attribute if bRhsIsAttr = TRUE
    Value   rhsValue;       // right-hand side value if bRhsIsAttr = FALSE
};


class Iterator {
    // All the relational operators and access methods are iterators.
    public:
        virtual RC getNextTuple(void *data) = 0;
        virtual void getAttributes(vector<Attribute> &attrs) const = 0;
        virtual ~Iterator() {};
};


class TableScan : public Iterator
{
    // A wrapper inheriting Iterator over RM_ScanIterator
    public:
        RelationManager &rm;
        RM_ScanIterator *iter;
        string tableName;
        vector<Attribute> attrs;
        vector<string> attrNames;
        RID rid;

        TableScan(RelationManager &rm, const string &tableName, const char *alias = NULL):rm(rm)
        {
        	//Set members
        	this->tableName = tableName;

            // Get Attributes from RM
            rm.getAttributes(tableName, attrs);

            // Get Attribute Names from RM
            unsigned i;
            for(i = 0; i < attrs.size(); ++i)
            {
                // convert to char *
                attrNames.push_back(attrs.at(i).name);
            }

            // Call RM scan to get an iterator
            iter = new RM_ScanIterator();
            rm.scan(tableName, "", NO_OP, NULL, attrNames, *iter);

            // Set alias
            if(alias) this->tableName = alias;
        };

        // Start a new iterator given the new compOp and value
        void setIterator()
        {
            iter->close();
            delete iter;
            iter = new RM_ScanIterator();
            rm.scan(tableName, "", NO_OP, NULL, attrNames, *iter);
        };

        RC getNextTuple(void *data)
        {
            // cout << "get Next table scan" << endl;
            return iter->getNextTuple(rid, data);
        };

        void getAttributes(vector<Attribute> &attrs) const
        {
            attrs.clear();
            attrs = this->attrs;
            unsigned i;

            // For attribute in vector<Attribute>, name it as rel.attr
            for(i = 0; i < attrs.size(); ++i)
            {
                string tmp = tableName;
                tmp += ".";
                tmp += attrs.at(i).name;
                attrs.at(i).name = tmp;
            }
        };

        ~TableScan()
        {
        	iter->close();
        };
};


class IndexScan : public Iterator
{
    // A wrapper inheriting Iterator over IX_IndexScan
    public:
        RelationManager &rm;
        RM_IndexScanIterator *iter;
        string tableName;
        string attrName;
        vector<Attribute> attrs;
        char key[PAGE_SIZE];
        RID rid;

        IndexScan(RelationManager &rm, const string &tableName, const string &attrName, const char *alias = NULL):rm(rm)
        {
        	// Set members
        	this->tableName = tableName;
        	this->attrName = attrName;

            // Get Attributes from RM
            rm.getAttributes(tableName, attrs);


            // Call rm indexScan to get iterator
            iter = new RM_IndexScanIterator();
            
            rm.indexScan(tableName, attrName, NULL, NULL, true, true, *iter);
            

            // Set alias
            if(alias) this->tableName = alias;
        };

        // Start a new iterator given the new key range
        RC setIterator(void* lowKey,
                         void* highKey,
                         bool lowKeyInclusive,
                         bool highKeyInclusive)
        {
            iter->close();
            delete iter;
            iter = new RM_IndexScanIterator();
            if(rm.indexScan(tableName, attrName, lowKey, highKey, lowKeyInclusive,
                           highKeyInclusive, *iter))
                return -1;
        };

        RC getNextTuple(void *data)
        {
            // cout << "tableName: " << tableName << endl;
            // cout << "indexScanNext Tuple"<< endl;
            int rc = iter->getNextEntry(rid, key);
            if(rc == 0)
            {
                rc = rm.readTuple(tableName.c_str(), rid, data);
            }
            return rc;
        };

        void getAttributes(vector<Attribute> &attrs) const
        {
            attrs.clear();
            attrs = this->attrs;
            unsigned i;

            // For attribute in vector<Attribute>, name it as rel.attr
            for(i = 0; i < attrs.size(); ++i)
            {
                string tmp = tableName;
                tmp += ".";
                tmp += attrs.at(i).name;
                attrs.at(i).name = tmp;
            }
        };

        ~IndexScan()
        {
            iter->close();
        };
};


class Filter : public Iterator {
    // Filter operator
    public:
        Iterator* input;
        Condition cond;
        vector<Attribute> attrs;
        int lIndex;
        int rIndex;
    public:
        Filter(Iterator *input,               // Iterator of input R
               const Condition &condition     // Selection condition
        );
        ~Filter(){};

        RC getNextTuple(void *data);
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const;
        bool condJudge(Value lvalue, Value rvalue, int lIndex, int rIndex);
        int compareValue(Value lvalue, Value rvalue, int lIndex, int rIndex);
};


class Project : public Iterator {
    // Projection operator
    public:
        Iterator* input;
        vector<string> attrNames;
        Project(Iterator *input,                    // Iterator of input R
              const vector<string> &attrNames){
                  this->input = input;
                  this->attrNames = attrNames;
              };   // vector containing attribute names
        ~Project(){};

        RC getNextTuple(void *data);
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const{
            attrs.clear();
            vector<Attribute> pAttrs;
            input->getAttributes(pAttrs);
            for(int i = 0; i<attrNames.size(); i++){
                for(int j = 0; j<pAttrs.size(); j++)
                    if(pAttrs[j].name == attrNames[i]){
                        attrs.push_back(pAttrs[j]);
                        continue;
                    }
            }
            
        };
};

class BNLJoin : public Iterator {
    // Block nested-loop join operator
    public:
        BNLJoin(Iterator *leftIn,            // Iterator of input R
               TableScan *rightIn,           // TableScan Iterator of input S
               const Condition &condition,   // Join condition
               const unsigned numPages       // # of pages that can be loaded into memory,
			                                 //   i.e., memory block size (decided by the optimizer)
        );
        ~BNLJoin();

        RC getNextTuple(void *data);
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const;
    private:
        Iterator *leftInput;
        TableScan *rightInput;
        Condition condition;
        unsigned numOfPages;
        unsigned maxBytes;
        // map<string, vector<string>> blockMap;
        map<string, void*> blockMapS;
        map<int, void*> blockMapI;
        map<float, void*> blockMapF;


        unsigned blockFillBytes;
        bool isFirst;

        void* lRecord;
        void* rRecord;
        int lIndex;
        int rIndex;
        void* combinedRecord;
        Attribute curAttr;
        Attribute curRAttr;
        int curKeyLength;
        int curRKeyLength;
        // vector<string> curMappedRecords;
        void* curFoundRecord;
        AttrType joinKeyType;


        vector<Attribute> leftTableAttributes;
        vector<Attribute> rightTableAttributes;

        RC loadNextBlock();
        RC clearCurBlock();
        void combineRecords(void* data);
        int isValid();
        void* getRLengthAndKey(int& rLength, void* key);
        RC addBlockMap(void* key, int rLength);
        void* showBlockMap();

};


class INLJoin : public Iterator {
    // Index nested-loop join operator
    public:
        Iterator* leftIn;
        IndexScan* rightIn;
        Condition cond;
        int lIndex;
        int rIndex;
        INLJoin(Iterator *leftIn,           // Iterator of input R
               IndexScan *rightIn,          // IndexScan Iterator of input S
               const Condition &condition   // Join condition
        ){
            this->leftIn = leftIn;
            this->rightIn = rightIn;
            this->cond = condition;
            if(!cond.bRhsIsAttr) cout<<"error condition"<<endl;
            //find the position and offset of compared attribute
            vector<Attribute> lattrs;
            vector<Attribute> rattrs;
            leftIn->getAttributes(lattrs);
            rightIn->getAttributes(rattrs);
            for(int i = 0; i<lattrs.size(); i++)
                if(lattrs[i].name == cond.lhsAttr)
                    lIndex = i;
            for(int i = 0; i<rattrs.size(); i++)
                if(rattrs[i].name == cond.rhsAttr)
                    rIndex = i;
        };
        ~INLJoin(){};

        RC getNextTuple(void *data);
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const{
            vector<Attribute> lattrs;
            vector<Attribute> rattrs;
            leftIn->getAttributes(lattrs);
            rightIn->getAttributes(rattrs);
            for(int i = 0; i<lattrs.size(); i++)
                attrs.push_back(lattrs[i]);
            
            for(int i = 0; i<lattrs.size(); i++)
                attrs.push_back(rattrs[i]);
        };
};

// Optional for everyone. 10 extra-credit points
class GHJoin : public Iterator {
    // Grace hash join operator
    public:
      GHJoin(Iterator *leftIn,               // Iterator of input R
            Iterator *rightIn,               // Iterator of input S
            const Condition &condition,      // Join condition (CompOp is always EQ)
            const unsigned numPartitions     // # of partitions for each relation (decided by the optimizer)
      ){};
      ~GHJoin(){};

      RC getNextTuple(void *data){return QE_EOF;};
      // For attribute in vector<Attribute>, name it as rel.attr
      void getAttributes(vector<Attribute> &attrs) const{};
};

class Aggregate : public Iterator {
    // Aggregation operator
    public:
        Iterator* input;
        vector<Attribute> attrs;
        int aggIndex;
        Attribute aggAttr;
        AggregateOp op;
        bool resultFlag;
        // Mandatory
        // Basic aggregation
        Aggregate(Iterator *input,          // Iterator of input R
                  Attribute aggAttr,        // The attribute over which we are computing an aggregate
                  AggregateOp op            // Aggregate operation
        ){
            this->input = input;
            this->aggAttr = aggAttr;
            this->op = op;
            resultFlag = false;
            input->getAttributes(attrs);
            for(int i = 0; i<attrs.size(); i++)
                if(attrs[i].name == aggAttr.name)
                    aggIndex = i;
        };

        // Optional for everyone: 5 extra-credit points
        // Group-based hash aggregation
        Aggregate(Iterator *input,             // Iterator of input R
                  Attribute aggAttr,           // The attribute over which we are computing an aggregate
                  Attribute groupAttr,         // The attribute over which we are grouping the tuples
                  AggregateOp op              // Aggregate operation
        ){};
        ~Aggregate(){};

        RC getNextTuple(void *data);
        // Please name the output attribute as aggregateOp(aggAttr)
        // E.g. Relation=rel, attribute=attr, aggregateOp=MAX
        // output attrname = "MAX(rel.attr)"
        void getAttributes(vector<Attribute> &attrs) const{
            string opName;
            switch(op){
                case MAX:
                    opName = "MAX";
                    break;
                case MIN:
                    opName = "MIN";
                    break;
                case COUNT:
                    opName = "COUNT";
                    break;
                case AVG:
                    opName = "AVG";
                    break;
                case SUM:
                    opName = "SUM";
                    break;
            }
        };
};

#endif
