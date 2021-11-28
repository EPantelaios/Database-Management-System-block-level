#ifndef PRIMARY_HASH_FILE_H
#define PRIMARY_HASH_FILE_H

#define NUMBER_OF_INDEXES (BLOCK_SIZE/4)


typedef struct Record{
	int id;
	char name[15];
	char surname[25];
	char address[50];
}Record;


typedef struct {
    int fileDesc;
    char attrType;
    char attrName[8];
    int attrLength;
    long int numBuckets;
}HT_info;


int HT_CreateIndex(char *fileName, char attrType, char* attrName, int attrLength, int buckets);

HT_info* HT_OpenIndex(char *fileName);

int HT_CloseIndex(HT_info* header_info);

int HT_InsertEntry(HT_info header_info, Record record);

int HT_DeleteEntry(HT_info header_info, void *value);

int HT_GetAllEntries(HT_info header_info, void *value);


#endif //PRIMARY_HASH_FILE_H