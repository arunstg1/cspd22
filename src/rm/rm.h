
#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>
#include <ostream>

#include "../pf/pf.h"

using namespace std;

// Return code
typedef int RC;

// Record ID
typedef struct
{
	unsigned pageNum;
	unsigned slotNum;
} RID;

// Attribute
typedef enum { TypeInt = 0, TypeReal, TypeVarChar } AttrType;

typedef unsigned AttrLength;

struct Attribute {
	string   name;     // attribute name
	AttrType type;     // attribute type
	AttrLength length; // attribute length
};

// Catalog Struct
typedef struct
{
	short schemaId;
	char columnName[50];
	char tableName[50];
	AttrType columnType;
	AttrLength columnLength;
} Catalog;

// Control Information
typedef struct CITuple
{
	int beg, end;

	bool operator < (const CITuple& tuple) const
	{
		return ((end-beg) < (tuple.end - tuple.beg));
	}
} CITuple;

// Comparison Operator
typedef enum { EQ_OP = 0,  // =
	LT_OP,      // <
	GT_OP,      // >
	LE_OP,      // <=
	GE_OP,      // >=
	NE_OP,      // !=
	NO_OP       // no condition
} CompOp;


# define RM_EOF (-1)  // end of a scan operator

typedef struct DIRecord {
	int startPos;
	short version, length;
} DIRecord;

// RM_ScanIterator is an iteratr to go through records
// The way to use it is like the following:
//  RM_ScanIterator rmScanIterator;
//  rm.open(..., rmScanIterator);
//  while (rmScanIterator(rid, data) != RM_EOF) {
//    process the data;
//  }
//  rmScanIterator.close();

class RM_ScanIterator {
private:
	RC rdVectorCntr;
	vector<void *> returnedData;
	//	vector<RMSIStruct> ignoreDI;
	vector<RID> ridVector; // Remove
	bool isSet; // Remove

	string tableName, conditionAttribute;
	CompOp compOp;
	const void *value, *pageBuffer;
	vector<string> attributeNames;
	int page, slotNumber, currentDIPos;
	short diOffset;

public:
	RM_ScanIterator() {
		isSet = false;
		pageBuffer = malloc(PF_PAGE_SIZE);
		page = 0, slotNumber = currentDIPos = PF_PAGE_SIZE;
	};
	~RM_ScanIterator() {
		returnedData.clear();
		//	  ignoreDI.clear();
	};

	RC init(const string tableName,
			const string conditionAttribute,
			const CompOp compOp,                  // comparision type such as "<" and "="
			const void *value,                    // used in the comparison
			const vector<string> &);

	// "data" follows the same format as RM::insertTuple()
	RC getNextTuple(RID &rid, void *data);

	int getCurrentDiPos() const {
		return currentDIPos;
	}

	void setCurrentDiPos(int currentDiPos) {
		currentDIPos = currentDiPos;
	}

	short getDiOffset() const {
		return diOffset;
	}

	void setDiOffset(short diOffset) {
		this->diOffset = diOffset;
	}

	const void* getValue() const {
		return value;
	}

	void setValue(const void *value) {
		this->value = value;
	}

	const void* getPageBuffer() const {
		return pageBuffer;
	}

	void setPageBuffer(const void *pageBuffer) {
		this->pageBuffer = pageBuffer;
	}

	int getSlotNumber() const {
		return slotNumber;
	}

	void setSlotNumber(int slotNumber) {
		this->slotNumber = slotNumber;
	}

	int getPage() const {
		return page;
	}

	void setPage(int page) {
		this->page = page;
	}

	vector<string> getAttributeNames() const {
		return attributeNames;
	}

	void setAttributeNames(vector<string> attributeNames) {
		this->attributeNames = attributeNames;
	}

	CompOp getCompOp() const {
		return compOp;
	}

	void setCompOp(CompOp compOp) {
		this->compOp = compOp;
	}

	string getConditionAttribute() const {
		return conditionAttribute;
	}

	void setConditionAttribute(string conditionAttribute) {
		this->conditionAttribute = conditionAttribute;
	}

	string getTableName() const {
		return tableName;
	}

	void setTableName(string tableName) {
		this->tableName = tableName;
	}

	RID getRid(int counter) const {
		return ridVector.at(counter);
	}

	void setRidVector(RID rid) {
		this->ridVector.push_back(rid);
	}

	bool isIsSet() const {
		return isSet;
	}

	void setIsSet(bool isSet) {
		this->isSet = isSet;
	}

	RC close() { return -1; };

	void pushBackRetDat(void* retDat) {
		(this->returnedData).push_back(retDat);
	}

	void* retDataAtPos(int pos) {
		return (this->returnedData).at(pos);
	}

	/*
	void pushBackRMSIDI(RMSIStruct ignoreDI) {
	  (this->ignoreDI).push_back(ignoreDI);
	}
	 */

	RC getRetDatSize() {
		return returnedData.size();
	}

	RC getRdVectorCntr() const {
		return rdVectorCntr;
	}

	void setRdVectorCntr(RC rdVectorCntr) {
		this->rdVectorCntr = rdVectorCntr;
	}
};

// Record Manager
class RM
{
public:
	static RM* Instance();

	RC createTable(const string tableName, const vector<Attribute> &attrs); // Done

	RC deleteTable(const string tableName); // Done

	RC getAttributes(const string tableName, vector<Attribute> &attrs); // Done

	//  Format of the data passed into the function is the following:
	//  1) data is a concatenation of values of the attributes
	//  2) For int and real: use 4 bytes to store the value;
	//     For varchar: use 4 bytes to store the length of characters, then store the actual characters.
	//  !!!The same format is used for updateTuple(), the returned data of readTuple(), and readAttribute()
	RC insertTuple(const string tableName, const void *data, RID &rid); // Done - modifications needed.

	RC deleteTuples(const string tableName); // Done

	RC deleteTuple(const string tableName, const RID &rid);

	// Assume the rid does not change after update
	RC updateTuple(const string tableName, const void *data, const RID &rid);

	RC readTuple(const string tableName, const RID &rid, void *data); // Done

	RC readAttribute(const string tableName, const RID &rid, const string attributeName, void *data); // Done

	RC reorganizePage(const string tableName, const unsigned pageNumber);

	// scan returns an iterator to allow the caller to go through the results one by one.
	RC scan(const string tableName,
			const string conditionAttribute,
			const CompOp compOp,                  // comparision type such as "<" and "="
			const void *value,                    // used in the comparison
			const vector<string> &attributeNames, // a list of projected attributes
			RM_ScanIterator &rm_ScanIterator);

	// Additional Methods
	RC dropAttribute(const string tableName, const string attributeName);

	RC addAttribute(const string tableName, const Attribute attr);

	RC reorganizeTable(const string tableName);

	// User Defined Methods
	void insertCatInfo(Catalog &catalog, unsigned int pos);

	RC createDatTable(const string tableName);

	RC initDatTable(const string tableName, PF_Manager *pf);

	short getDIOffset(void *page);

	RC calcDataLength(const string tableName, const void *data);

	RC generateCI(void *data, vector<CITuple> &CI);
	RC setDIOffset(void *page, short DIOffset);
	RC setDIRecord(void *page, int offset, int startPos, short length, short version);
	RC getLatestVersion(string);

	void* parseTuple(void *data, const string tableName, int &tupleLength);
	void* unParseTuple(void *data, const string tableName);
	RC stringInTuple(vector<Attribute> attrs);


protected:
	RM();
	~RM();

private:
	static RM *_rm;
};

#endif
