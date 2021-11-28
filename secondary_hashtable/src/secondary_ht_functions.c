#include "../include/secondary_ht_functions.h"


int str_hash(char *str, int number_buckets){

    int i=0;
    long int hash_value=0;

    for(i=0; i<strlen(str); i++){

        hash_value = (hash_value*34 + str[i])%715823;
    }
    
    return hash_value % number_buckets;
}




int print_record_by_attr_name_SHT(SHT_info header_info, Record temp_record, void *value){
   
   if(!strcmp(header_info.attrName, "surname")){
    
        if(!strcmp(value, temp_record.surname)){

            printf("id  = %d, name = %s, surname = %s, address = %s\n", 
                temp_record.id, temp_record.name, temp_record.surname, temp_record.address);

            return 1;
        }
    }
    
    return 0;
}




int check_if_record_is_already_at_file_SHT(SHT_info header_info, void *value, int blockId){

    void *block;
    int stored_number_of_next_block=0;
    int number_next_block=0;
    int cnt_of_records=0;
    int hash_index=0;
    SecondaryRecord *temp_record = (SecondaryRecord *)malloc(sizeof(SecondaryRecord));


    //call hash function to find the index
    if(!strcmp(header_info.attrName, "surname")){
        hash_index = str_hash(value, header_info.numBuckets);
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


    //if the current index is empty without blocks
    if(stored_number_of_next_block==0){

        printf("No record at this index of bucket.\n"); 
        free(temp_record);         
        return -1;
    }
    
    BF_ReadBlock(header_info.fileDesc, stored_number_of_next_block-1, &block);
    memcpy(&number_next_block, block, sizeof(int));
    memcpy(&cnt_of_records, block+4, sizeof(int));

    //search all blocks except last 
    while(number_next_block != -10){
        

        for(int i=0; i<cnt_of_records; i++){

            memcpy(temp_record, block + 8 + i*sizeof(SecondaryRecord), sizeof(SecondaryRecord));
            
            if(!strcmp((char *)value, temp_record->surname) && temp_record->blockId==blockId){           
                free(temp_record);
                return -1;
            }
        }

        BF_ReadBlock(header_info.fileDesc, number_next_block-1, &block);
        memcpy(&number_next_block, block, sizeof(int));
        memcpy(&cnt_of_records, block+4, sizeof(int));    
    }
    

    //search last block
    for(int i=0; i<cnt_of_records; i++){

        memcpy(temp_record, block + 8 + i*sizeof(SecondaryRecord), sizeof(SecondaryRecord));

        if(!strcmp((char *)value, temp_record->surname) && temp_record->blockId==blockId){  
            free(temp_record);
            return -1;
        }
    }

    free(temp_record);  
    return 0;
}