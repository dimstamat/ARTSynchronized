//
// Created by florian on 18.11.15.
//

#ifndef ART_OPTIMISTICLOCK_COUPLING_N_H
#define ART_OPTIMISTICLOCK_COUPLING_N_H

 // Dimos: This is used for the tree size metric, needed by merge
#define MEASURE_TREE_SIZE 1
#define N_THREADS 20

#include "N.h"

#include <string>

using namespace ART;

#define MEASURE_ART_NODE_ACCESSES 0

namespace ART_OLC {

using node_vers_t = std::tuple<N*, uint64_t>;

	// Dim STO
	typedef struct trans_info {
		bool w_unlock_obsolete;     // whether to unlock with writeUnlockObsolete()
        N* cur_node;                // the node where the record should live (inserted or looked up)
		uint8_t keyslice;           // the keyslice to insert
		N* l_node;                  // the locked node			(inserts and updates only)
		N* l_parent_node;           // the locked parent node	(inserts only)
		node_vers_t updated_node1;  // the first node we change and thus we should update its AVN (Node*, AVN)
        node_vers_t updated_node2;  // the second node we change and thus we should update its AVN (Node*, AVN)
        bool check_key;			    // the caller must call checkKey because the default ART loadKey does not cast from rec* to TID!
        TID prevVal;                // the existing tid before the update
        TID updatedVal;             // if > 0, then it is an update
        #if MEASURE_ART_NODE_ACCESSES == 1
        uint8_t accessed_nodes;     // measure the number of accessed ART nodes for statistical purposes
        #endif
        #if MEASURE_TREE_SIZE == 1
        unsigned addedSize;              // the change in the tree's size after an insert or delete operation, required by merge. (negative number is caused 
                                    // by delete)
        #endif
        bool shouldAbort;           // whether the transaction should abort due to encountering of an 
                                    // obsolete node during ABSENT_VALIDATION
        //ThreadInfo& threadInfo; // the tree thread info (needed for actual delete on STO cleanup)
	} trans_info;


    class Tree {
    
    N *const root;

    public:
        using LoadKeyFunction = void (*)(TID tid, Key &key);

        #if MEASURE_TREE_SIZE == 1
        uint64_t tree_sz[N_THREADS];
        #endif

        //Dim: Testing
        // It should be public, so that to be used in HybridTART when merging
        LoadKeyFunction loadKey;

    //private:

        //N *const root;

	// Dim: Testing
	//protected:
		//LoadKeyFunction loadKey;
	
	private:
		

        TID checkKey(const TID tid, const Key &k) const;

        Epoche epoche{256};

    public:

        enum class CheckPrefixResult : uint8_t {
            Match,
            NoMatch,
            OptimisticMatch
        };

        enum class CheckPrefixPessimisticResult : uint8_t {
            Match,
            NoMatch,
        };

        enum class PCCompareResults : uint8_t {
            Smaller,
            Equal,
            Bigger,
        };
        enum class PCEqualsResults : uint8_t {
            BothMatch,
            Contained,
            NoMatch
        };

		// Dim: for debugging:
		static std::string keyToStr(const Key &k) {
            std::string res="";
            for(unsigned i=0; i<k.getKeyLen(); i++){
                res += (char)k[i];
            }
            return res;
        }

        static CheckPrefixResult checkPrefix(N* n, const Key &k, uint32_t &level);

        static CheckPrefixPessimisticResult checkPrefixPessimistic(N *n, const Key &k, uint32_t &level,
                                                                   uint8_t &nonMatchingKey,
                                                                   Prefix &nonMatchingPrefix,
                                                                   LoadKeyFunction loadKey, bool &needRestart);

        static PCCompareResults checkPrefixCompare(const N* n, const Key &k, uint8_t fillKey, uint32_t &level, LoadKeyFunction loadKey, bool &needRestart);

        static PCEqualsResults checkPrefixEquals(const N* n, uint32_t &level, const Key &start, const Key &end, LoadKeyFunction loadKey, bool &needRestart);

    public:

        Tree(LoadKeyFunction loadKey);

        Tree(const Tree &) = delete;

        Tree(Tree &&t) : root(t.root), loadKey(t.loadKey) { }

        ~Tree();

        ThreadInfo getThreadInfo();

        #if MEASURE_TREE_SIZE == 1
        uint64_t getTreeSize(void);
        #endif

        TID lookup(const Key &k, ThreadInfo &threadEpocheInfo) const;
		// Dim: For transactional ART
		TID lookup(const Key &k, ThreadInfo &threadEpocheInfo, trans_info* t_info) const;
        TID lookup(const Key &k, ThreadInfo &threadEpocheInfo, trans_info* t_info, N* startNode) const;

        bool lookupRange(const Key &start, const Key &end, Key &continueKey, TID result[], std::size_t resultLen,
                         std::size_t &resultCount, ThreadInfo &threadEpocheInfo) const;

        void insert(const Key &k, TID tid, ThreadInfo &epocheInfo);

		// Dim STO: insert function to provide transactinal information for STO
		void insert(const Key &k, TID tid, ThreadInfo &epocheInfo, trans_info* t_info);

        void remove(const Key &k, TID tid, ThreadInfo &epocheInfo);

        // Dim STO: insert function to provide transactinal information for STO
        void remove(const Key &k, TID tid, ThreadInfo &epocheInfo, trans_info* t_info);
    };
}
#endif //ART_OPTIMISTICLOCK_COUPLING_N_H
