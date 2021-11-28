hashtable:
	@echo " Compile hashtables ...";
	gcc -Wall -I ./include/ ./main_folder/main_hash.c ./primary_hashtable/src/primary_ht.c ./primary_hashtable/src/primary_ht_functions.c ./secondary_hashtable/src/secondary_ht.c ./secondary_hashtable/src/secondary_ht_functions.c BF_lib/BF_64.a -no-pie -o ./build/runner
