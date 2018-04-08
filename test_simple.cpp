#include "OptimisticLockCoupling/Tree.h"



const char key_dat [] [10] = { {3, 'A', 'R', 'T'},\
							 {2, 'A', 'B'},\
							 {2, 'A', 'C'},\
							 {2, 'A', 'C'}};
							 /*{4, 'A', 'B', 'B', 'A'},\
							 {3, 'S', 'T', 'O'},\
							 {9, 'S', 'T', 'O', 'H', 'A', 'S', 'T', 'I', 'C'},\
							 {5, 'S', 'T', 'O', 'C', 'K'},\
							 {5, 'S', 'T', 'E', 'A', 'K'}};
*/


void loadKey(TID tid, Key &key) {
      // Store the key of the tuple into the key vector
      // Implementation is database specific
      printf("LOAD KEY IS CALLED for %lu, size: %u!\n", tid, (unsigned)key_dat[tid][0]);
	  key.set(key_dat[tid]+1, (unsigned)key_dat[tid][0]);
}


int main(){
	ART_OLC::Tree tree(loadKey);
	auto t = tree.getThreadInfo();
	Key key;
	for (TID tid=0; tid<sizeof(key_dat) / sizeof(key_dat[0]); tid++){
		key.set(key_dat[tid]+1, (unsigned)key_dat[tid][0]);
		tree.insert(key, tid, t);
	}

	return 0;
}




