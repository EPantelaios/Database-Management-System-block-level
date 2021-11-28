#ifndef SECONDARY_HASH_FILE_H
#define SECONDARY_HASH_FILE_H

#define NUMBER_OF_INDEXES (BLOCK_SIZE/4)

#include "../../primary_hashtable/include/primary_ht.h"

typedef struct SecondaryRecord{

    char surname[25];
    int blockId;
}SecondaryRecord;


typedef struct {
    int fileDesc;  
    char attrName[8];
    int attrLength; 
    long int numBuckets;
    char fileName[30];
}SHT_info;


int SHT_CreateSecondaryIndex(char *sfileName, char* attrName, int attrLength, int buckets, char *fileName);

SHT_info* SHT_OpenSecondaryIndex(char *sfileName);

int SHT_CloseSecondaryIndex(SHT_info* header_info);

int SHT_SecondaryInsertEntry(SHT_info header_info, SecondaryRecord record);

int SHT_SecondaryGetAllEntries(SHT_info header_info_sht, HT_info header_info_ht, void *value);


#endif //SECONDARY_HASH_FILE_H