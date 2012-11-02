#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

#include <fstream>
#include <iostream>
#include <cassert>

#include "rm.h"

using namespace std;

RM *rm = RM::Instance();
const int success = 0;

// Function to prepare the data in the correct form to be inserted/read/updated
void prepareTuple(const int name_length, const string name, const int age, const float height, const int salary, void *buffer, int *tuple_size)
{
    int offset = 0;

    memcpy((char *)buffer + offset, &name_length, sizeof(int));
    offset += sizeof(int);
    memcpy((char *)buffer + offset, name.c_str(), name_length);
    offset += name_length;

    memcpy((char *)buffer + offset, &age, sizeof(int));
    offset += sizeof(int);

    memcpy((char *)buffer + offset, &height, sizeof(float));
    offset += sizeof(float);

    memcpy((char *)buffer + offset, &salary, sizeof(int));
    offset += sizeof(int);

    *tuple_size = offset;
}


// Function to parse the data in buffer and print each field
void printTuple(const void *buffer, const int tuple_size)
{
    int offset = 0;
    cout << "****Printing Buffer: Start****" << endl;

    int name_length = 0;
    memcpy(&name_length, (char *)buffer+offset, sizeof(int));
    offset += sizeof(int);
    cout << "name_length: " << name_length << endl;

    char *name = (char *)malloc(100);
    memcpy(name, (char *)buffer+offset, name_length);
    name[name_length] = '\0';
    offset += name_length;
    cout << "name: " << name << endl;

    int age = 0;
    memcpy(&age, (char *)buffer+offset, sizeof(int));
    offset += sizeof(int);
    cout << "age: " << age << endl;

    float height = 0.0;
    memcpy(&height, (char *)buffer+offset, sizeof(float));
    offset += sizeof(float);
    cout << "height: " << height << endl;

    int salary = 0;
    memcpy(&salary, (char *)buffer+offset, sizeof(int));
    offset += sizeof(int);
    cout << "salary: " << salary << endl;

    cout << "****Printing Buffer: End****" << endl << endl;
}


// Create an employee table
void createTable(const string tablename)
{
    cout << "****Create Table " << tablename << " ****" << endl;

    // 1. Create Table ** -- made separate now.
    vector<Attribute> attrs;

    Attribute attr;
    attr.name = "EmpName";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)30;
    attrs.push_back(attr);

    attr.name = "Age";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    attrs.push_back(attr);

    attr.name = "Height";
    attr.type = TypeReal;
    attr.length = (AttrLength)4;
    attrs.push_back(attr);

    attr.name = "Salary";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    attrs.push_back(attr);

    RC rc = rm->createTable(tablename, attrs);
    assert(rc == success);
    cout << "****Table Created: " << tablename << " ****" << endl << endl;
}


void secA_0(const string tablename)
{
    // Functions Tested
    // 1. Get Attributes
    cout << "****In Test Case 0****" << endl;

    // GetAttributes
    vector<Attribute> attrs;
    RC rc = rm->getAttributes(tablename, attrs);
    assert(rc == success);

    for(unsigned i = 0; i < attrs.size(); i++)
    {
        cout << "Attribute Name: " << attrs[i].name << endl;
        cout << "Attribute Type: " << (AttrType)attrs[i].type << endl;
        cout << "Attribute Length: " << attrs[i].length << endl << endl;
    }
    return;
}

void secA_1(const string tablename, const int name_length, const string name, const int age, const float height, const int salary)
{
    // Functions tested
    // 1. Create Table ** -- made separate now.
    // 2. Insert Tuple **
    // 3. Read Tuple **
    // NOTE: "**" signifies the new functions being tested in this test case.
    cout << "****In Test Case 1****" << endl;

    RID rid;
    int tuple_size = 0;
    void *tuple = malloc(100);
    void *data_returned = malloc(100);

    // Insert a tuple into a table
    prepareTuple(name_length, name, age, height, salary, tuple, &tuple_size);
    cout << "Insert Data:" << endl;
    printTuple(tuple, tuple_size);
    RC rc = rm->insertTuple(tablename, tuple, rid);
    assert(rc == success);

    // Given the rid, read the tuple from table
    rc = rm->readTuple(tablename, rid, data_returned);
    assert(rc == success);

    cout << "Returned Data:" << endl;
    printTuple(data_returned, tuple_size);

    // Compare whether the two memory blocks are the same
    if(memcmp(tuple, data_returned, tuple_size) == 0)
    {
        cout << "****Test case 1 passed****" << endl << endl;
    }
    else
    {
        cout << "****Test case 1 failed****" << endl << endl;
    }

    free(tuple);
    free(data_returned);
    return;
}


void secA_2(const string tablename, const int name_length, const string name, const int age, const float height, const int salary)
{
    // Functions Tested
    // 1. Insert tuple
    // 2. Delete Tuple **
    // 3. Read Tuple
    cout << "****In Test Case 2****" << endl;

    RID rid;
    int tuple_size = 0;
    void *tuple = malloc(100);
    void *data_returned = malloc(100);

    // Test Insert the Tuple
    prepareTuple(name_length, name, age, height, salary, tuple, &tuple_size);
    cout << "Insert Data:" << endl;
    printTuple(tuple, tuple_size);
    RC rc = rm->insertTuple(tablename, tuple, rid);
    assert(rc == success);

    // Test Delete Tuple
    rc = rm->deleteTuple(tablename, rid);
    assert(rc == success);

    // Test Read Tuple
    memset(data_returned, 0, 100);
    rc = rm->readTuple(tablename, rid, data_returned);
    assert(rc != success);

    cout << "After Deletion." << endl;

    // Compare the two memory blocks to see whether they are different
    if (memcmp(tuple, data_returned, tuple_size) != 0)
    {
        cout << "****Test case 2 passed****" << endl << endl;
    }
    else
    {
        cout << "****Test case 2 failed****" << endl << endl;
    }

    free(tuple);
    free(data_returned);
    return;
}


void secA_3(const string tablename, const int name_length, const string name, const int age, const float height, const int salary)
{
    // Functions Tested
    // 1. Insert Tuple
    // 2. Update Tuple **
    // 3. Read Tuple
    cout << "****In Test Case 3****" << endl;

    RID rid;
    int tuple_size = 0;
    int tuple_size_updated = 0;
    void *tuple = malloc(100);
    void *tuple_updated = malloc(100);
    void *data_returned = malloc(100);

    // Test Insert Tuple
    prepareTuple(name_length, name, age, height, salary, tuple, &tuple_size);
    RC rc = rm->insertTuple(tablename, tuple, rid);
    assert(rc == success);
    cout << "Original RID slot = " << rid.slotNum << endl;

    // Test Update Tuple
    prepareTuple(6, "Newman", age, height, 100, tuple_updated, &tuple_size_updated);
    rc = rm->updateTuple(tablename, tuple_updated, rid);
    assert(rc == success);
    cout << "Updated RID slot = " << rid.slotNum << endl;

    // Test Read Tuple
    rc = rm->readTuple(tablename, rid, data_returned);
    assert(rc == success);
    cout << "Read RID slot = " << rid.slotNum << endl;

    // Print the tuples
    cout << "Insert Data:" << endl;
    printTuple(tuple, tuple_size);

    cout << "Updated data:" << endl;
    printTuple(tuple_updated, tuple_size_updated);

    cout << "Returned Data:" << endl;
    printTuple(data_returned, tuple_size_updated);

    if (memcmp(tuple_updated, data_returned, tuple_size_updated) == 0)
    {
        cout << "****Test case 3 passed****" << endl << endl;
    }
    else
    {
        cout << "****Test case 3 failed****" << endl << endl;
    }

    free(tuple);
    free(tuple_updated);
    free(data_returned);
    return;
}


void secA_4(const string tablename, const int name_length, const string name, const int age, const float height, const int salary)
{
    // Functions Tested
    // 1. Insert tuple
    // 2. Read Attributes **
    cout << "****In Test Case 4****" << endl;

    RID rid;
    int tuple_size = 0;
    void *tuple = malloc(100);
    void *data_returned = malloc(100);

    // Test Insert Tuple
    prepareTuple(name_length, name, age, height, salary, tuple, &tuple_size);
    RC rc = rm->insertTuple(tablename, tuple, rid);
    assert(rc == success);

    // Test Read Attribute
    rc = rm->readAttribute(tablename, rid, "Salary", data_returned);
    assert(rc == success);

    cout << "Salary: " << *(int *)data_returned << endl;
    if (memcmp((char *)data_returned, (char *)tuple+18, 4) != 0)
    {
        cout << "****Test case 4 failed" << endl << endl;
    }
    else
    {
        cout << "****Test case 4 passed" << endl << endl;
    }

    free(tuple);
    free(data_returned);
    return;
}


void secA_5(const string tablename, const int name_length, const string name, const int age, const float height, const int salary)
{
    // Functions Tested
    // 0. Insert tuple;
    // 1. Read Tuple
    // 2. Delete Tuples **
    // 3. Read Tuple
    cout << "****In Test Case 5****" << endl;

    RID rid;
    int tuple_size = 0;
    void *tuple = malloc(100);
    void *data_returned = malloc(100);
    void *data_returned1 = malloc(100);

    // Test Insert Tuple
    prepareTuple(name_length, name, age, height, salary, tuple, &tuple_size);
    RC rc = rm->insertTuple(tablename, tuple, rid);
    assert(rc == success);

    // Test Read Tuple
    rc = rm->readTuple(tablename, rid, data_returned);
    assert(rc == success);
    printTuple(data_returned, tuple_size);

    cout << "Now Deleting..." << endl;

    // Test Delete Tuples
    rc = rm->deleteTuples(tablename);
    assert(rc == success);

    // Test Read Tuple
    memset((char*)data_returned1, 0, 100);
    rc = rm->readTuple(tablename, rid, data_returned1);
    assert(rc != success);
    printTuple(data_returned1, tuple_size);

    if(memcmp(tuple, data_returned1, tuple_size) != 0)
    {
        cout << "****Test case 5 passed****" << endl << endl;
    }
    else
    {
        cout << "****Test case 5 failed****" << endl << endl;
    }

    free(tuple);
    free(data_returned);
    free(data_returned1);
    return;
}

void secA_6(const string tablename)
{
    // Functions Tested
    // 1. Simple scan **
    cout << "****In Test Case 6****" << endl;

    RID rid;
    int tuple_size = 0;
    int num_records = 5;
    void *tuple;
    void *data_returned = malloc(100);

    RID rids[num_records];
    vector<char *> tuples;

    RC rc = 0;
    for(int i = 0; i < num_records; i++)
    {
        tuple = malloc(100);

        // Insert Tuple
        float height = (float)i;
        prepareTuple(6, "Tester", 20+i, height, 123, tuple, &tuple_size);
        rc = rm->insertTuple(tablename, tuple, rid);
        assert(rc == success);

        tuples.push_back((char *)tuple);
        rids[i] = rid;
    }
    cout << "After Insertion!" << endl;

    // Set up the iterator
    RM_ScanIterator rmsi;
    string attr = "Age";
    vector<string> attributes;
    attributes.push_back(attr);
    rc = rm->scan(tablename, "", NO_OP, NULL, attributes, rmsi);
    assert(rc == success);

    cout << "Scanned Data:" << endl;

    while(rmsi.getNextTuple(rid, data_returned) != RM_EOF)
    {
        cout << "Age: " << *(int *)data_returned << endl;
    }
    rmsi.close();

    // Deleta Table
    rc = rm->deleteTable(tablename);
    assert(rc == success);

    free(data_returned);
    for(int i = 0; i < num_records; i++)
    {
        free(tuples[i]);
    }

    return;
}

void Tests()
{
    // GetAttributes
    secA_0("tbl_employee");

    // Insert/Read Tuple
    secA_1("tbl_employee", 6, "Peters", 24, 170.1, 5000);

    // Delete Tuple
    secA_2("tbl_employee", 6, "Victor", 22, 180.2, 6000);

    // Update Tuple
    secA_3("tbl_employee", 6, "Thomas", 28, 187.3, 4000);

    // Read Attributes
    secA_4("tbl_employee", 6, "Veekay", 27, 171.4, 9000);

    // Delete Tuples
    secA_5("tbl_employee", 6, "Dillon", 29, 172.5, 7000);

    // Simple Scan
    createTable("tbl_employee3");
    secA_6("tbl_employee3");

    return;
}


void UpdateTestCase1()
{
	string tableName = "tbl_players";
	createTable(tableName);
    cout << "****In Update Test Case 1****" << endl;
    cout << "Peters"<<endl;
    secA_1(tableName, 6, "Peters", 25, 200.2, 5000);
    cout << "Sachin"<<endl;
    secA_1(tableName, 6, "Sachin", 39, 201.3, 6600);
    cout << "Dhoni"<<endl;
    secA_1(tableName, 20, "Mahendra Singh Dhoni", 27, 199.4, 7770);
    cout << "Dravid"<<endl;
    secA_1(tableName, 12, "Rahul Dravid", 37, 203.5, 8888);
    cout << "Federer"<<endl;
    secA_1(tableName, 13, "Roger Federer", 35, 45.6, 9999);

    RID rid;
    int tuple_size = 0;
    int tuple_size_updated = 0;
    void *tuple_updated = malloc(100);
    void *data_returned = malloc(100);

    // Test Update Tuple
    rid.pageNum = 1; rid.slotNum = PF_PAGE_SIZE-sizeof(DIRecord);
    cout << "Original RID page = " << rid.pageNum << " slot = " << rid.slotNum << endl;
    prepareTuple(8, "MS Dhoni", 28, 203.4, 7220, tuple_updated, &tuple_size_updated);
    int rc = rm->updateTuple(tableName, tuple_updated, rid);
    assert(rc == success);
    cout << "Updated RID page = " << rid.pageNum << " slot = " << rid.slotNum << endl;

    rc = rm->readTuple(tableName, rid, data_returned);		// Test Read Tuple
    assert(rc == success);
    cout << "Read RID slot = " << rid.slotNum << endl;

    cout << "Returned Data:" << endl;
    printTuple(data_returned, tuple_size);

    if (memcmp(tuple_updated, data_returned, tuple_size_updated) == 0)
    {
        cout << "****PASSED Update Shrink record Test Case****" << endl << endl;
    }
    else
    {
        cout << "****FAILED : Update Shrink record Test Case****" << endl << endl;
    }

    RM_ScanIterator rmsi;
    vector<string> attributes;
    string attr = "EmpName";attributes.push_back(attr);
    attr = "Age";			attributes.push_back(attr);
    attr = "Height";		attributes.push_back(attr);
    attr = "Salary";		attributes.push_back(attr);

    int age = 9999;
    rc = rm->scan(tableName, "Salary", EQ_OP, &age, attributes, rmsi);
    assert(rc == success);

    cout << "Scanned Data:" << endl;

    while(rmsi.getNextTuple(rid, data_returned) != RM_EOF)
        printTuple(data_returned, rm->calcDataLength(tableName, data_returned));

    rmsi.close();

    rc = rm->deleteTable(tableName);
    assert(rc == success);
    rc = rm->readTuple(tableName, rid, data_returned);		// Test Read Tuple
    assert(rc != success);

    free(tuple_updated);
    free(data_returned);
}

void UpdateTestCase2()
{
	string tablename = "tbl_UpdateTestCase2";
	createTable(tablename);
    cout << "****In Update Test Case 2****" << endl;
    cout << "Peters"<<endl;
    secA_1(tablename, 6, "Peters", 25, 200.2, 5000);
    cout << "Sachin"<<endl;
    secA_1(tablename, 6, "Sachin", 26, 201.3, 6600);
    cout << "Dhoni"<<endl;
    secA_1(tablename, 20, "Mahendra Singh Dhoni", 27, 202.4, 7770);
    cout << "Dravid"<<endl;
    secA_1(tablename, 12, "Rahul Dravid", 28, 203.5, 8888);
    cout << "Federer"<<endl;
    secA_1(tablename, 13, "Roger Federer", 29, 204.6, 9999);

    RID rid;
    int tuple_size = 0;
    int tuple_size_updated = 0;
    void *tuple_updated = malloc(100);
    void *data_returned = malloc(100);

    // Test Update Tuple
    rid.pageNum = 1; rid.slotNum = PF_PAGE_SIZE-sizeof(DIRecord);
    cout << "Original RID page = " << rid.pageNum << " slot = " << rid.slotNum << endl;
    prepareTuple(29, "Mahendra Singh Dhoni(captain)", 28, 203.4, 7220, tuple_updated, &tuple_size_updated);
    int rc = rm->updateTuple(tablename, tuple_updated, rid);
    assert(rc == success);
    cout << "Updated RID page = " << rid.pageNum << " slot = " << rid.slotNum << endl;

    rc = rm->readTuple(tablename, rid, data_returned);		// Test Read Tuple
    assert(rc == success);
    cout << "Read RID slot = " << rid.slotNum << endl;

    cout << "Returned Data:" << endl;
    printTuple(data_returned, tuple_size);

    if (memcmp(tuple_updated, data_returned, tuple_size_updated) == 0)
    {
        cout << "****PASSED Update Expand data Test Case  ****" << endl << endl;
    }
    else
    {
        cout << "****FAILED : Update Expand data Test Case ****" << endl << endl;
    }

    // Test Update Tuple
    rid.pageNum = 1; rid.slotNum = PF_PAGE_SIZE-sizeof(DIRecord);
    cout << "Original RID page = " << rid.pageNum << " slot = " << rid.slotNum << endl;
    prepareTuple(37, "Mahendra Singh Dhoni(current captain)", 28, 203.4, 7220, tuple_updated, &tuple_size_updated);
    rc = rm->updateTuple(tablename, tuple_updated, rid);
    assert(rc == success);
    cout << "Updated RID page = " << rid.pageNum << " slot = " << rid.slotNum << endl;

    rc = rm->readTuple(tablename, rid, data_returned);		// Test Read Tuple
    assert(rc == success);
    cout << "Read RID slot = " << rid.slotNum << endl;

    cout << "Returned Data:" << endl;
    printTuple(data_returned, tuple_size);

    if (memcmp(tuple_updated, data_returned, tuple_size_updated) == 0)
    {
        cout << "****PASSED Update Expand data new page Test Case  ****" << endl << endl;
    }
    else
    {
        cout << "****FAILED : Update Expand data new page Test Case ****" << endl << endl;
    }

    rc = rm->deleteTable(tablename);
    assert(rc == success);
    rc = rm->readTuple(tablename, rid, data_returned);		// Test Read Tuple
    assert(rc != success);

    free(tuple_updated);
    free(data_returned);
}

void TestReorganizePage()
{
	string tablename = "tbl_ReorganizePageTest";
	createTable(tablename);
    cout << "****In Reorganize Page Test Case 1****" << endl;
    cout << "Peters"<<endl;
    secA_1(tablename, 6, "Peters", 25, 200.2, 5000);
    cout << "Sachin"<<endl;
    secA_1(tablename, 6, "Sachin", 26, 201.3, -6600);
    cout << "Dhoni"<<endl;
    secA_1(tablename, 20, "Mahendra Singh Dhoni", 27, 202.4, 7770);
    cout << "Dravid"<<endl;
    secA_1(tablename, 12, "Rahul Dravid", 28, 203.5, 8888);
    cout << "Federer"<<endl;
    secA_1(tablename, 13, "Roger Federer", 29, 204.6, 9999);

    RID rid;
    int tuple_size = 0;
    void *data_returned = malloc(100);

    // Test delete Tuple
    rid.pageNum = 0; rid.slotNum = PF_PAGE_SIZE-(3*sizeof(DIRecord));
    RC rc = rm->deleteTuple(tablename, rid);
    assert(rc == success);

    rid.pageNum = 0; rid.slotNum = PF_PAGE_SIZE-(4*sizeof(DIRecord));
    rc = rm->deleteTuple(tablename, rid);
    assert(rc == success);

    // test read tuple
    rc = rm->readTuple(tablename, rid, data_returned);		// Test Read Tuple
    assert(rc != success);

    rc = rm->reorganizePage(tablename, 0);

    rc = rm->readTuple(tablename, rid, data_returned);		// Test Read Tuple
    assert(rc != success);

    PF_Manager *pf = PF_Manager::Instance();
    PF_FileHandle *fileHandle = new PF_FileHandle();
    string dataFile = tablename + ".dat";
	if( pf->OpenFile(dataFile.c_str(), *fileHandle) == 0 )
	{
		if( fileHandle->ReadPage(0, data_returned) == 0 )
		{
			void *tuple = malloc(100);
			memcpy(tuple, (char *) data_returned + 46, 29);
			pf->CloseFile(*fileHandle);
			printTuple(tuple, 29);
		}
	}

}

void printParsedTuple(const void *buffer, const int tuple_size)
{
	int offset = 0;
	cout << "****Printing Buffer: Start****" << endl;

	short s1Len = 0;
	memcpy(&s1Len, (char *)buffer+offset, sizeof(short));
	offset += sizeof(short);
	cout << "String 1 Length: " << s1Len << endl;
	int name_length = 0;
	memcpy(&name_length, (char *)buffer+offset, sizeof(int));
	offset += sizeof(int);
	cout << "name_length: " << name_length << endl;

	char *name = (char *)malloc(100);
	try {
		memcpy(name, (char *)buffer+offset, name_length);
	} catch(int e) {
		cout << "Trying to read non-existing record.";
	}
	name[name_length] = '\0';
	offset += name_length;
	cout << "name: " << name << endl;

	int age = 0;
	memcpy(&age, (char *)buffer+offset, sizeof(int));
	offset += sizeof(int);
	cout << "age: " << age << endl;

	float height = 0.0;
	memcpy(&height, (char *)buffer+offset, sizeof(float));
	offset += sizeof(float);
	cout << "height: " << height << endl;

	int salary = 0;
	memcpy(&salary, (char *)buffer+offset, sizeof(int));
	offset += sizeof(int);
	cout << "salary: " << salary << endl;

	cout << "****Printing Buffer: End****" << endl << endl;
}

void testParse_UnParse()
{
	int tuple_size = 0;
	void *tuple = malloc(100);
	void *parsedTuple = malloc(100);

	// Insert a tuple into a table
	int tupleLength = 0;
	prepareTuple(6, "arvind", 22, 5.7, 50000, tuple, &tuple_size);
	parsedTuple = rm->parseTuple(tuple, "tbl_employee", tupleLength);

	printParsedTuple(parsedTuple, 24);
	cout << "Tuple Length: " << tupleLength << endl;

	void *tupleOne = malloc(100);
	tupleOne = rm->unParseTuple(parsedTuple, "tbl_employee");
	printTuple(tupleOne, 22);
}

void scanTest()
{
	void *data_returned = malloc(100);
	RID rid;

	RM_ScanIterator rmsi;
	string attr = "Height";
	vector<string> attributes;
	attributes.push_back(attr);
	attr = "Salary";
	attributes.push_back(attr);
	//RC rc = rm->scan("tbl_employee", "Age", LE_OP, buffer, attributes, rmsi);
	RC rc = rm->scan("tbl_employee", "", NO_OP, NULL, attributes, rmsi);

	while(rmsi.getNextTuple(rid, data_returned) != RM_EOF)
	{
		int age; /*memcpy(&age, (char *)data_returned, sizeof(int));
		char* efg = (char*) malloc(50);efg[age] = '\0';
		memcpy(efg, (char *)data_returned + sizeof(int), age);
		cout << "Name: " << efg << endl;*/
		float flt = 0.0;
		memcpy(&flt, (char *)data_returned, sizeof(float));
		cout << "Height: " << flt << endl;
		int ht = 0;
		memcpy(&ht, (char *)data_returned+sizeof(float), sizeof(int));
		cout << "Salary: " << ht << endl;
		memcpy(&age, (char *)data_returned+8, sizeof(int));
	}
}

void TestReorganizeTable(const string tablename)
{
    cout << "****In Re organise table test case ****" << endl;

	createTable(tablename);
    RID rid;
    int tuple_size = 0;
    int num_records = 5000, delCnt = 0;
    void *tuple;
    int afterInsert, afterDeletion;

    int totalDels = 2500;
    RID rids[totalDels];

    RC rc = 0;
    for(int i = 0; i < num_records; i++)
    {
        tuple = malloc(50);
        float height = (float)i;
        prepareTuple(6, "TejasP", 20+i, height, 123, tuple, &tuple_size);
        rc = rm->insertTuple(tablename, tuple, rid);
        assert(rc == success);

        rc = rm->readTuple(tablename, rid, tuple);
        assert(rc == success);

        if(delCnt < totalDels) {
        	rids[delCnt] = rid;
        	delCnt++;
        }
    }

	string fileName = tablename + ".dat";
	PF_Manager *pf = PF_Manager::Instance();
	PF_FileHandle *DBHandle = new PF_FileHandle();
	pf->OpenFile(fileName.c_str(), *DBHandle);
    afterInsert = DBHandle->GetNumberOfPages();
    pf->CloseFile(*DBHandle);

    for(int i = 0; i < delCnt; i++)
    {
        rc = rm->deleteTuple(tablename, rids[i]);
        assert(rc == success);
    }

	pf->OpenFile(fileName.c_str(), *DBHandle);
	afterDeletion = DBHandle->GetNumberOfPages();
    pf->CloseFile(*DBHandle);

    rc = rm->reorganizeTable(tablename);
    assert (rc == success);

    cout << "After insertion page count : " << afterInsert << endl;
    cout << "After deletion page count : " << afterDeletion << endl;
	pf->OpenFile(fileName.c_str(), *DBHandle);
    cout << "After re-organisation page count : " << DBHandle->GetNumberOfPages() << endl;
    pf->CloseFile(*DBHandle);

    free(tuple);
}

int main()
{
    // Basic Functions
    cout << endl << "Test Basic Functions..." << endl;

    // Create Table

    createTable("tbl_employee");
    Tests();

//    UpdateTestCase1();  WORK ONLY WITH PF_PAGE_SIZE 4096
//    UpdateTestCase2();
//    scanTest();
    TestReorganizePage();
    TestReorganizeTable("reorganizeTableTest");

    cout << "O.K.";
    return 0;
}
