#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <time.h>
#include "../primary_hashtable/include/primary_ht_functions.h"
#include "../secondary_hashtable/include/secondary_ht_functions.h"

#define RECORDS_NUM 15000
#define MAX_OPEN_FILES 5


typedef struct openfiles{

    char fileName[100];
    HT_info *ht_info;
    char sfileName[100];
    SHT_info *sht_info;

}openfiles;


int main(){

    Record record;
    SecondaryRecord sht_record;
    int ret_val=0, i=0, cnt_blocks=0, num_buckets=0;
    openfiles file_manager[MAX_OPEN_FILES];
    char current_line[100];
    char* token;
    FILE* records_from_file;


    Record **saved_records = malloc(MAX_OPEN_FILES*sizeof(Record *));
    if(saved_records==NULL){

		printf("Sorry, cannot allocate memory\n");
		return -1;
	}	

    for(i=0;i<MAX_OPEN_FILES;i++){

        saved_records[i]=malloc(RECORDS_NUM*sizeof(Record));

        if(saved_records[i]==NULL){

            printf("Sorry, cannot allocate memory\n");
            return -1;  
        }
    }


    srand(8549813);
    BF_Init();


    for(i=0; i<MAX_OPEN_FILES; i++){
        
        num_buckets = rand() % 301 + 50;    //50 to 350 buckets
        sprintf(file_manager[i].fileName, "primary_hash_file%d", i+1);
        assert(!HT_CreateIndex(file_manager[i].fileName, 'i', "id", 4, num_buckets));
        file_manager[i].ht_info=HT_OpenIndex(file_manager[i].fileName);     


        num_buckets = rand() % 301 + 50;    //50 to 350 buckets
        sprintf(file_manager[i].sfileName, "secondary_hash_file%d", i+1);
        assert(!SHT_CreateSecondaryIndex(file_manager[i].sfileName, "surname", 25, num_buckets, file_manager[i].fileName));
        file_manager[i].sht_info=SHT_OpenSecondaryIndex(file_manager[i].sfileName);   
    }


    for(i=0; i<MAX_OPEN_FILES; i++){

        records_from_file = fopen("record_examples/records15K.txt", "r");
        if(records_from_file == NULL){
            printf("Cannot open file\n");
            return -1;
        }

        printf("\n--------------------------------------------------------------------------------\n");
        printf("File %d - Insert Entries...\n\n", i+1);
        for (int j=0; j < RECORDS_NUM; j++) {
          
            token = NULL;

            //Read a line from the file
            fgets(current_line, 100, records_from_file);

            if(feof(records_from_file)){

                printf("Error. File should have at least %d records to read...\n", RECORDS_NUM);
                assert(1==0);
            }

            //Tokenize each line to get the information
            token = strtok(current_line,"\",");
            sscanf(token,"{%d",&record.id);

            token = strtok(NULL,"\",");
            sscanf(token,"%s",record.name);

            token = strtok(NULL,"\",");
            sscanf(token,"%s",record.surname);

            token = strtok(NULL,"\",");
            sscanf(token,"%s}",record.address);   
            

            ret_val = HT_InsertEntry(*file_manager[i].ht_info, record); 
            if(ret_val<2){

                printf("Error at Insert Entry.\n");
                assert(1==0);
            }    
            
            //Store record at a 2d-array for testing later. 
            saved_records[i][j]=record;


            sht_record.blockId = ret_val;
            strcpy(sht_record.surname, record.surname);

            assert(!SHT_SecondaryInsertEntry(*file_manager[i].sht_info, sht_record)); 
        }



        //For primary hashtable do some tests
        int random_number = rand() % RECORDS_NUM;

        printf("Get Entry with id = %d\n", saved_records[i][random_number].id);
        cnt_blocks = HT_GetAllEntries(*file_manager[i].ht_info, (void*)(long int)saved_records[i][random_number].id);
        printf(">> %d blocks was read until the record was found.\n\n", cnt_blocks);

        printf("\n");
        HT_DeleteEntry(*file_manager[i].ht_info, (void*)(long int)saved_records[i][random_number].id);

        printf("\n\nGet Entry with id = %d\n", saved_records[i][random_number].id);
        cnt_blocks = HT_GetAllEntries(*file_manager[i].ht_info, (void*)(long int)saved_records[i][random_number].id);    
        printf(">> %d blocks was read until the record was found.\n\n", cnt_blocks);


        printf("\nInsert Entry with id = %d\n\n", saved_records[i][random_number].id);
        ret_val = HT_InsertEntry(*file_manager[i].ht_info, saved_records[i][random_number]);   
        if(ret_val<2){

            printf("Error at Insert Entry.\n");
            assert(1==0);
        }   


        printf("\nGet Entry with id = %d\n", saved_records[i][random_number].id);
        cnt_blocks = HT_GetAllEntries(*file_manager[i].ht_info, (void*)(long int)saved_records[i][random_number].id);    
        printf(">> %d blocks was read until the record was found.\n\n", cnt_blocks);


        printf("\nInsert Entry with id = %d\n", saved_records[i][random_number].id);

        ret_val = HT_InsertEntry(*file_manager[i].ht_info, saved_records[i][random_number]);   
        if(ret_val<2){

            printf("Error at Insert Entry.\n");
            assert(1==0);
        }



        //For secondary hashtable do some tests
        char surname_str[20];
        int x=rand()%3000+100;
      
        sprintf(surname_str, "surname_%d", x);

        printf("\n\n\n\nGet Entry with surname = '%s'\n", surname_str);

        cnt_blocks = SHT_SecondaryGetAllEntries(*file_manager[i].sht_info, *file_manager[i].ht_info, (char *)surname_str);
        printf(">> %d blocks was read until the record was found.\n\n\n\n", cnt_blocks);   


        assert(!fclose(records_from_file));
        printf("\n\n\n");
    }


    //Close HT files and remove files
    for(i=0; i<MAX_OPEN_FILES; i++){

        assert(!HT_CloseIndex(file_manager[i].ht_info));
        assert(!SHT_CloseSecondaryIndex(file_manager[i].sht_info));

        HashStatistics(file_manager[i].fileName);
        HashStatistics(file_manager[i].sfileName);
    }

   
    for(i=0; i<MAX_OPEN_FILES; i++){
      
        remove(file_manager[i].fileName);  
        remove(file_manager[i].sfileName);
    }


    for(i=0;i<MAX_OPEN_FILES;i++){

        free(saved_records[i]);
    }
    free(saved_records);
}