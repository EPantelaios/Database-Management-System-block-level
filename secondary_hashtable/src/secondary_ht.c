#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <limits.h>

#include "../include/secondary_ht_functions.h"


int SHT_CreateSecondaryIndex(char *sfileName, char* attrName, int attrLength, int buckets, char *fileName){

    int fileDesc;
    void *block;
    int block_counter=0;
    int init=0;
    char check_primary_ht[18] = "Primary_Hashtable";
    char metadata[20] = "Secondary_Hashtable";

    //check if attrName is equal to 'surname'. The only acceptable name.
    if(strcmp(attrName, "surname")){
        printf("Error. Create the secondary index with the correct attribute name (='surname')\n");
        return -1;
    }

    //check if there is already a primary hashtable
    if((fileDesc = BF_OpenFile(fileName)) < 0){
        BF_PrintError("Can't open File");
        return -1;
    }
    //Read from first block
    if(BF_ReadBlock(fileDesc, 0, &block) < 0){
        BF_PrintError("Can't read block");
        return -1;
    }
    //check if the first block has the correct information for hash file
    if(memcmp(check_primary_ht, block, strlen(check_primary_ht)+1)){

        printf("Error! Τhe filename that you import does not correspond to a primary hash file...");
        return -1;
    }
    if(BF_CloseFile(fileDesc) < 0){
        BF_PrintError("Can't close file");
        return -1;
    }



    //Creathe the secondary hashtable
    SHT_info sht_info;

    if(BF_CreateFile(sfileName) < 0){
        BF_PrintError("Error Creating File");
        return -1;
    }

    if((fileDesc = BF_OpenFile(sfileName)) < 0){
        BF_PrintError("Can't open File");
        return -1;
    }

    //Allocate first block
    if(BF_AllocateBlock(fileDesc) < 0){
        BF_PrintError("Can't allocate block");
        return -1;
    }

    block_counter = BF_GetBlockCounter(fileDesc) - 1;
    assert(block_counter==0);


    if(BF_ReadBlock(fileDesc, block_counter, &block) < 0){
        BF_PrintError("Can't read block");
        return -1;
    }
    
    //the information to know if file is actually heap file
    //is a string 'HeapFile' at the first block.
    memcpy(block, metadata, strlen(metadata)+1);
    
    //copy the info from arguments to the hp_info struct
    sht_info.fileDesc = fileDesc;
    strncpy(sht_info.attrName, attrName, strlen(attrName)+1);
    sht_info.attrLength = attrLength;
    sht_info.numBuckets = buckets;
    strncpy(sht_info.fileName, fileName, strlen(fileName)+1);
    //copy information at first block
    memcpy(block+strlen(metadata)+1, &sht_info, sizeof(SHT_info));

    if(BF_WriteBlock(fileDesc, block_counter) < 0){
        BF_PrintError("Can't write block");
        return -1;
    }

    
    //Create exactly 'num_of_blocks' blocks for the index 
    //Each index is an integer, so has size 4 bytes.
    //If have '1-128' indexes want only 1 bucket.
    //If have '129-256' indexes want 2 buckets ... etc
    int num_of_blocks = (((buckets * sizeof(int)) - 1) / BLOCK_SIZE) + 1;
    int indexes_each_block=0;

    //Making the index
    for(int i=0; i<num_of_blocks; i++){

        BF_AllocateBlock(fileDesc);
        block_counter = BF_GetBlockCounter(fileDesc) - 1;
        BF_ReadBlock(fileDesc, block_counter, &block);

        if(buckets < NUMBER_OF_INDEXES) //buckets < 128
            indexes_each_block = buckets;
        else
            indexes_each_block = NUMBER_OF_INDEXES;

        for(int j=0; j<indexes_each_block; j++){
            
            init=0;
            memcpy(block + j * sizeof(int), &init, sizeof(int));
        }   
        
        buckets = buckets - BLOCK_SIZE/sizeof(int);

        //save information at block
        if(BF_WriteBlock(fileDesc, block_counter) < 0){
            BF_PrintError("Can't write block");
            return -1;
        }
    }


    if(BF_CloseFile(fileDesc) < 0){
        BF_PrintError("Can't close file");
        return -1;
    }

    return 0;  
}







SHT_info* SHT_OpenSecondaryIndex(char *sfileName){

    int fileDesc;
    void *block;
    char metadata[20] = "Secondary_Hashtable";
    SHT_info *sht_info = (SHT_info *)malloc(sizeof(SHT_info));   
      

    if((fileDesc = BF_OpenFile(sfileName)) < 0){
        BF_PrintError("Can't open File");
        return NULL;
    }

    //Read from first block
    if(BF_ReadBlock(fileDesc, 0, &block) < 0){
        BF_PrintError("Can't read block");
        return NULL;
    }

    //check if the first block has the correct information for hash file
    if(memcmp(metadata, block, strlen(metadata)+1)){

        printf("Error! Τhe filename that you import does not correspond to a secondary heap file...");
        return NULL;
    }
   
    //copy hp_info struct from first block
    memcpy(sht_info, (block)+strlen(metadata)+1, sizeof(SHT_info));


    return sht_info;
}







int SHT_CloseSecondaryIndex(SHT_info* header_info){

    int fileDesc = header_info->fileDesc;
    if(BF_CloseFile(fileDesc) < 0){
        BF_PrintError("Can't close file");
        return -1;
    }

    free(header_info);

    return 0;
}







int SHT_SecondaryInsertEntry(SHT_info header_info, SecondaryRecord record){

    void *block;
    int stored_number_of_next_block=0;
    int number_next_block=0;
    int cnt_of_records=0;
    int flg=0;
    int block_counter=0;
    int store_next_block=-100;
    int cnt=0;
    int hash_index=0;
    int ret_val=0;


    //check for the attribute type and name.
    //call the hash function to find the right bucket at index.
    if(!strcmp(header_info.attrName, "surname")){
        hash_index = str_hash(record.surname, header_info.numBuckets);
    }
    else{
        printf("Error. Insert records only with key 'surname'. Exit");
        assert(1==0);
    }


    //find specific block and index in the block
    int block_number = (hash_index / NUMBER_OF_INDEXES)  + 1;
    int block_index = hash_index % NUMBER_OF_INDEXES;

    BF_ReadBlock(header_info.fileDesc, block_number, &block);
    memcpy(&stored_number_of_next_block, block + block_index*sizeof(int), sizeof(int));

    //when the first record is inserted then it must create a new block
    if(stored_number_of_next_block==0){
        
        const int cnt_of_records = 1;
        const int number_next_block = -10;

        //store in the corresponding position of index the value of the current block
        block_counter=0;

        block_counter = BF_GetBlockCounter(header_info.fileDesc);
        block_counter = block_counter + 1;
        memcpy(block + block_index*sizeof(int), &block_counter, sizeof(int));

        assert(!BF_WriteBlock(header_info.fileDesc, block_number));


        //create new block and store the record
        BF_AllocateBlock(header_info.fileDesc);
        block_counter = BF_GetBlockCounter(header_info.fileDesc) - 1;
        BF_ReadBlock(header_info.fileDesc, block_counter, &block);

        memcpy(block, &number_next_block, sizeof(int));
        memcpy(block+4, &cnt_of_records, sizeof(int));
        memcpy(block+8, &record, sizeof(SecondaryRecord));

        assert(!BF_WriteBlock(header_info.fileDesc, block_counter));
        
        return 0;
    }

    //Check for duplicate records
    if(!strcmp(header_info.attrName, "surname")){
        ret_val = check_if_record_is_already_at_file_SHT(header_info, (char *)record.surname, record.blockId);
    }
    
    
    if(ret_val==-1){

        printf("The record with key surname = %s and blockId = %d "
               "is already in the file. Not allowed to insert duplicate records.\n",
                record.surname, record.blockId);

        return 0;
    }
    

    BF_ReadBlock(header_info.fileDesc, stored_number_of_next_block-1, &block);
    memcpy(&number_next_block, block, sizeof(int));
    memcpy(&cnt_of_records, block+4, sizeof(int));
    
    
    //find the first block that is not full
    while(cnt_of_records >= (BLOCK_SIZE/sizeof(SecondaryRecord)-1)){   //run only if block is full (has already 15 records).
        
        flg=1;
        cnt++;

        //If all blocks are full, then create new block     
        if(number_next_block==-10 && cnt_of_records >= (BLOCK_SIZE/sizeof(SecondaryRecord)-1)){

            const int tmp_cnt_of_records = 1;
            const int tmp_number_next_block = -10;
            
            //complete the previous block's variable 'number_next_block' with the new value
            block_counter=BF_GetBlockCounter(header_info.fileDesc);
            block_counter = block_counter + 1;
            memcpy(block, &block_counter, sizeof(int));

            if(cnt!=1){
                assert(!BF_WriteBlock(header_info.fileDesc, store_next_block-1));
            }
            else{
                assert(!BF_WriteBlock(header_info.fileDesc, stored_number_of_next_block-1));
            }


            //create new block and store the record
            BF_AllocateBlock(header_info.fileDesc);
            block_counter=BF_GetBlockCounter(header_info.fileDesc) - 1;
            BF_ReadBlock(header_info.fileDesc, block_counter, &block);

            memcpy(block, &tmp_number_next_block, sizeof(int));
            memcpy(block+4, &tmp_cnt_of_records, sizeof(int));
            memcpy(block+8, &record, sizeof(SecondaryRecord));

            assert(!BF_WriteBlock(header_info.fileDesc, block_counter));

            return 0;
        }


        store_next_block = number_next_block;
        BF_ReadBlock(header_info.fileDesc, number_next_block-1, &block);
        memcpy(&number_next_block, block, sizeof(int));
        memcpy(&cnt_of_records, block+4, sizeof(int));
       
    }


    if(flg==0){

        memcpy(block + 8 + (cnt_of_records)*sizeof(SecondaryRecord), &record, sizeof(SecondaryRecord));
        cnt_of_records = cnt_of_records + 1;
        memcpy(block + 4, &cnt_of_records, sizeof(int));

        assert(!BF_WriteBlock(header_info.fileDesc, stored_number_of_next_block-1));

        return 0;
    }
    else{

        BF_ReadBlock(header_info.fileDesc, number_next_block-1, &block);
        memcpy(block + 8 + (cnt_of_records)*sizeof(SecondaryRecord), &record, sizeof(SecondaryRecord));
        cnt_of_records = cnt_of_records + 1;
        memcpy(block + 4, &cnt_of_records, sizeof(int));

        assert(!BF_WriteBlock(header_info.fileDesc, store_next_block-1));

        return 0;
    }
}






int SHT_SecondaryGetAllEntries(SHT_info header_info_sht, HT_info header_info_ht, void *value){

    void *block;
    void *block_ht;
    int stored_number_of_next_block=0, number_next_block=0;
    int cnt_of_records=0, hash_index=0;
    int cnt=1, flg=0, cnt_of_records_ht=0;
    Record record_ht;
    SecondaryRecord *temp_record = (SecondaryRecord *)malloc(sizeof(SecondaryRecord));
    

    //check for the attribute type and name.
    //call the hash function to find the right bucket at index.
    if(!strcmp(header_info_sht.attrName, "surname")){
        hash_index = str_hash(value, header_info_sht.numBuckets);
    }
    else{
        printf("Error. Insert records only with key 'surname'. Exit");
        assert(1==0);
    }

    //find specific block and index in the block
    int block_number = (hash_index / NUMBER_OF_INDEXES)  + 1;
    int block_index = hash_index % NUMBER_OF_INDEXES;


    BF_ReadBlock(header_info_sht.fileDesc, block_number, &block);
    memcpy(&stored_number_of_next_block, block + block_index*sizeof(int), sizeof(int));


    //if the current index is empty without blocks
    if(stored_number_of_next_block==0){

        printf("No record at this index of bucket.\n"); 
        free(temp_record);         
        return -1;
    }
    
    BF_ReadBlock(header_info_sht.fileDesc, stored_number_of_next_block-1, &block);
    memcpy(&number_next_block, block, sizeof(int));
    memcpy(&cnt_of_records, block+4, sizeof(int));
            

    //search all blocks except last 
    while(number_next_block != -10){
        
        for(int i=0; i<cnt_of_records; i++){

            memcpy(temp_record, block + 8 + i*sizeof(SecondaryRecord), sizeof(SecondaryRecord));

            if(!strcmp(value, temp_record->surname)){

                printf("\nEntry found!\nSurname = '%s' and blockId of Primary Hashtable is = %d\n", 
                    temp_record->surname, temp_record->blockId);
              
        
                //Read block at primary hashtable
                if(BF_ReadBlock(header_info_ht.fileDesc, temp_record->blockId, &block_ht)){

                    BF_PrintError("Can't read block");
                    free(temp_record);         
                    return -1;
                }

                memcpy(&cnt_of_records_ht, block_ht+4, sizeof(int));  

                for(int i=0; i<cnt_of_records_ht; i++){

                    memcpy(&record_ht, block_ht + 8 + i*sizeof(Record), sizeof(Record));
                    print_record_by_attr_name_SHT(header_info_sht, record_ht, value);
                }

                flg=1;
            }

        }

        BF_ReadBlock(header_info_sht.fileDesc, number_next_block-1, &block);
        memcpy(&number_next_block, block, sizeof(int));
        memcpy(&cnt_of_records, block+4, sizeof(int));    

        cnt++;
    }
    

    //search last block
    for(int i=0; i<cnt_of_records; i++){

        memcpy(temp_record, block + 8 + i*sizeof(SecondaryRecord), sizeof(SecondaryRecord));

        if(!strcmp(value, temp_record->surname)){

            printf("\nEntry found!\nSurname = '%s' and blockId of Primary Hashtable is = %d\n", 
                    temp_record->surname, temp_record->blockId);
            
            
            //Read block at primary hashtable
            if(BF_ReadBlock(header_info_ht.fileDesc, temp_record->blockId, &block_ht)){

                BF_PrintError("Can't read block");
                free(temp_record);         
                return -1;
            }

            memcpy(&cnt_of_records_ht, block_ht+4, sizeof(int));  

            for(int i=0; i<cnt_of_records_ht; i++){

                memcpy(&record_ht, block_ht + 8 + i*sizeof(Record), sizeof(Record));
                print_record_by_attr_name_SHT(header_info_sht, record_ht, value);
            }
            
            flg=1;
        }

    }

    cnt++;

    //in case of not found the record
    if(flg==0){

        printf("The record with value = '%s' is not in the secondary hash file\n", (char *)value);
    }


    free(temp_record);  
    return cnt;
}