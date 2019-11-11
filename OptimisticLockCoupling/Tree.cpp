#include <assert.h>
#include <algorithm>
#include "Tree.h"
#include "N.cpp"
#include "../Epoche.cpp"
#include "../Key.h"

#include <sstream>

using namespace std;
#include <iostream>

#define DIM_DEBUG 0
#if DIM_DEBUG == 1
	#define PRINT_DEBUG(...) printf(__VA_ARGS__);
#else
	#define PRINT_DEBUG(...)  
#endif

namespace ART_OLC {

    Tree::Tree(LoadKeyFunction loadKey) : root(new N256( nullptr, 0)), loadKey(loadKey) {
        #if MEASURE_TREE_SIZE == 1
            memset(tree_sz, 0, N_THREADS * sizeof(uint64_t));
        #endif
    }

    Tree::~Tree() {
        N::deleteChildren(root);
        N::deleteNode(root);
    }

    ThreadInfo Tree::getThreadInfo() {
        return ThreadInfo(this->epoche);
    }

    #if MEASURE_TREE_SIZE == 1
    uint64_t Tree::getTreeSize(){
        uint64_t size=0;
        for(uint8_t i=0; i<N_THREADS; i++){
            size += tree_sz[i];
        }
        return size;
    }
    #endif

	TID Tree::lookup(const Key &k, ThreadInfo &threadEpocheInfo) const {
		return lookup(k, threadEpocheInfo, nullptr);
	}

    TID Tree::lookup(const Key &k, ThreadInfo &threadEpocheInfo, trans_info_t* t_info) const {
        return lookup(k, threadEpocheInfo, t_info, nullptr);
    }

    TID Tree::lookup(const Key &k, ThreadInfo &threadEpocheInfo, trans_info_t* t_info, N* startNode) const {
		EpocheGuardReadonly epocheGuard(threadEpocheInfo);
        restart:
        bool needRestart = false;

        N *node;
        N *parentNode = nullptr;
        uint64_t v;
        uint32_t level = 0;
        bool optimisticPrefixMatch = false;
		bool transactional = t_info != nullptr;

		PRINT_DEBUG("Looking up %s\n", keyToStr(k).c_str())

        node = (startNode == nullptr? root: startNode);
        v = node->readLockOrRestart(needRestart);
        // Dimos: when looking up for a key from a startNode in ABSENT_VALIDATION 2 or 3 there is a case that the node is obsolete and will always stay obsolete. Abort the transaction and the new attempt will end up in the new node.
        if(node->isObsolete(v) && startNode != nullptr) {
            t_info->shouldAbort = true;
            return 0;
        }
        if (needRestart)
            goto restart;
        
        while (true) {
            #if MEASURE_ART_NODE_ACCESSES == 1
            t_info->accessed_nodes++;
            #endif
            switch (checkPrefix(node, k, level)) { // increases level
                case CheckPrefixResult::NoMatch:
					PRINT_DEBUG("No match!\n")
					if(transactional){
						t_info->cur_node = node;
                        t_info->updated_node1 = node_vers_t(node, v);
					}
					node->readUnlockOrRestart(v, needRestart);
					if (needRestart) goto restart;
                    return 0;
                case CheckPrefixResult::OptimisticMatch:
                    optimisticPrefixMatch = true;
                    // fallthrough
                case CheckPrefixResult::Match:
                    uint8_t keyslice=0;
					PRINT_DEBUG("Match: keyslice: %c, level: %u, key length: %u\n", keyslice, level, k.getKeyLen())
					if (k.getKeyLen() <= level) {
						//TODO: Check if required to add in the node set here!
						// Dimos: Treat the special case where we added the 0 as a keyslice whenever a leaf is a prefix of another leaf
						if(N::getChild(0, node) != nullptr) {
							keyslice = 0;
						}
						else {
							if(transactional){
								t_info->cur_node = node;
                                t_info->updated_node1 = node_vers_t(node, v);
                            }
							PRINT_DEBUG("child in keyslice 0 is null!\n")
							node->readUnlockOrRestart(v, needRestart);
							if (needRestart) goto restart;
                       		return 0;
						}
                    } else
						keyslice = k[level];
                    parentNode = node;
                    node = N::getChild(keyslice, parentNode);
                    parentNode->checkOrRestart(v,needRestart);
                    if (needRestart) goto restart;

                    if (node == nullptr) {
                        parentNode->checkOrRestart(v,needRestart);
                        if (needRestart) goto restart;
						if(transactional){ // current node is null, add the parent node! (The node that would contain that key,val)
							t_info->cur_node = parentNode;
							t_info->updated_node1 = node_vers_t(parentNode, v);
						}
						PRINT_DEBUG("node from keyslice %c is null!\n", keyslice)
						return 0;
                    }
                    if (N::isLeaf(node)) {
                        parentNode->readUnlockOrRestart(v, needRestart);
                        if (needRestart) goto restart;
                        TID tid = N::getLeaf(node);
						// Dim: handle the 0 case! We need to check the key even if the encountered node is a leaf and the level is less than the key
						// length. That's because we have the case of 0 as the keyslice to follow the leaf node.
						// eg: When having node ABBA, we should not reply that we found AB (level=1, key length=2).
                        // It is wrong even without the 0 case! Consider the following tree contents:
                        // ABBA, AC
                        // lookup(AB) will say it found it because level=1 and keylen=2!! (level = keylen-1 so key will not be checked!)
						//if (level < k.getKeyLen() - 1 || optimisticPrefixMatch) {
						if (level < k.getKeyLen() || optimisticPrefixMatch) {
							if(transactional){
								t_info->check_key = true;
								t_info->cur_node = parentNode;
                                t_info->updated_node1 = node_vers_t(parentNode, v);
								return tid;
							}
							else
								return checkKey(tid, k);
                        }
                        return tid;
                    }
                    level++;
            }
            uint64_t nv = node->readLockOrRestart(needRestart);
            if (needRestart) goto restart;

            parentNode->readUnlockOrRestart(v, needRestart);
            if (needRestart) goto restart;
            v = nv;
        }
    }

    bool Tree::lookupRange(const Key &start, const Key &end, Key &continueKey, TID result[],
                                std::size_t resultSize, std::size_t &resultsFound, ThreadInfo &threadEpocheInfo) const {
       return lookupRange(start, end, continueKey, result, resultSize, resultsFound, threadEpocheInfo, nullptr); 
    }

    bool Tree::lookupRange(const Key &start, const Key &end, Key &continueKey, TID result[],
                                std::size_t resultSize, std::size_t &resultsFound, ThreadInfo &threadEpocheInfo, trans_info_range_t* t_info) const {
        bool transactional = (t_info != nullptr);
        for (uint32_t i = 0; i < std::min(start.getKeyLen(), end.getKeyLen()); ++i) {
            if (start[i] > end[i]) {
                resultsFound = 0;
                return false;
            } else if (start[i] < end[i]) {
                break;
            }
        }
        EpocheGuard epocheGuard(threadEpocheInfo);
        TID toContinue = 0;
        
        std::function<void(const N *, const N*, uint64_t)> copy = [&result, &resultSize, &resultsFound, &toContinue, &copy, transactional, &t_info](const N *node, const N* parentNode, uint64_t vers) {
            if (N::isLeaf(node)) {
                if (resultsFound == resultSize) {
                    toContinue = N::getLeaf(node);
                    return;
                }
                result[resultsFound] = N::getLeaf(node);
                resultsFound++;
                if(transactional){
                    t_info->addKeyRS(N::getLeaf(node)); // add key in the read set - this is implemented inside TART
                    t_info->addNodeNS(parentNode, vers); // add parent node together with its version in the node set - this is implemented inside TART
                }
            } else {
                std::tuple<uint8_t, N *> children[256];
                uint32_t childrenCount = 0;
                uint64_t v = N::getChildren(node, 0u, 255u, children, childrenCount);
                for (uint32_t i = 0; i < childrenCount; ++i) {
                    const N *n = std::get<1>(children[i]);
                    copy(n, node, v);
                    if (toContinue != 0) {
                        break;
                    }
                }
            }
        };
        std::function<void(N *, uint8_t, uint32_t, const N *, uint64_t)> findStart = [&copy, &start, &findStart, &toContinue, this](
                N *node, uint8_t nodeK, uint32_t level, const N *parentNode, uint64_t vp) {
            if (N::isLeaf(node)) {
                copy(node, parentNode, vp);
                return;
            }
            uint64_t v;
            PCCompareResults prefixResult;

            {
                readAgain:
                bool needRestart = false;
                v = node->readLockOrRestart(needRestart);
                if (needRestart) goto readAgain;

                prefixResult = checkPrefixCompare(node, start, 0, level, loadKey, needRestart);
                if (needRestart) goto readAgain;

                parentNode->readUnlockOrRestart(vp, needRestart);
                if (needRestart) {
                    readParentAgain:
                    vp = parentNode->readLockOrRestart(needRestart);
                    if (needRestart) goto readParentAgain;

                    node = N::getChild(nodeK, parentNode);

                    parentNode->readUnlockOrRestart(vp, needRestart);
                    if (needRestart) goto readParentAgain;

                    if (node == nullptr) {
                        return;
                    }
                    if (N::isLeaf(node)) {
                        copy(node, parentNode, vp);
                        return;
                    }
                    goto readAgain;
                }
                node->readUnlockOrRestart(v, needRestart);
                if (needRestart) goto readAgain;
            }

            switch (prefixResult) {
                case PCCompareResults::Bigger:
                    copy(node, parentNode, vp);
                    break;
                case PCCompareResults::Equal: {
                    uint8_t startLevel = (start.getKeyLen() > level) ? start[level] : 0;
                    std::tuple<uint8_t, N *> children[256];
                    uint32_t childrenCount = 0;
                    v = N::getChildren(node, startLevel, 255, children, childrenCount);
                    for (uint32_t i = 0; i < childrenCount; ++i) {
                        const uint8_t k = std::get<0>(children[i]);
                        N *n = std::get<1>(children[i]);
                        if (k == startLevel) {
                            findStart(n, k, level + 1, node, v);
                        } else if (k > startLevel) {
                            copy(n, node, v);
                        }
                        if (toContinue != 0) {
                            break;
                        }
                    }
                    break;
                }
                case PCCompareResults::Smaller:
                    break;
            }
        };
        std::function<void(N *, uint8_t, uint32_t, const N *, uint64_t)> findEnd = [&copy, &end, &toContinue, &findEnd, this](
                N *node, uint8_t nodeK, uint32_t level, const N *parentNode, uint64_t vp) {
            if (N::isLeaf(node)) {
                return;
            }
            uint64_t v;
            PCCompareResults prefixResult;
            {
                readAgain:
                bool needRestart = false;
                v = node->readLockOrRestart(needRestart);
                if (needRestart) goto readAgain;

                prefixResult = checkPrefixCompare(node, end, 255, level, loadKey, needRestart);
                if (needRestart) goto readAgain;

                parentNode->readUnlockOrRestart(vp, needRestart);
                if (needRestart) {
                    readParentAgain:
                    vp = parentNode->readLockOrRestart(needRestart);
                    if (needRestart) goto readParentAgain;

                    node = N::getChild(nodeK, parentNode);

                    parentNode->readUnlockOrRestart(vp, needRestart);
                    if (needRestart) goto readParentAgain;

                    if (node == nullptr) {
                        return;
                    }
                    if (N::isLeaf(node)) {
                        return;
                    }
                    goto readAgain;
                }
                node->readUnlockOrRestart(v, needRestart);
                if (needRestart) goto readAgain;
            }
            switch (prefixResult) {
                case PCCompareResults::Smaller:
                    copy(node, parentNode, vp);
                    break;
                case PCCompareResults::Equal: {
                    uint8_t endLevel = (end.getKeyLen() > level) ? end[level] : 255;
                    std::tuple<uint8_t, N *> children[256];
                    uint32_t childrenCount = 0;
                    v = N::getChildren(node, 0, endLevel, children, childrenCount);
                    for (uint32_t i = 0; i < childrenCount; ++i) {
                        const uint8_t k = std::get<0>(children[i]);
                        N *n = std::get<1>(children[i]);
                        if (k == endLevel) {
                            findEnd(n, k, level + 1, node, v);
                        } else if (k < endLevel) {
                            copy(n, node, v);
                        }
                        if (toContinue != 0) {
                            break;
                        }
                    }
                    break;
                }
                case PCCompareResults::Bigger:
                    break;
            }
        };

        restart:
        bool needRestart = false;

        resultsFound = 0;

        uint32_t level = 0;
        N *node = nullptr;
        N *nextNode = root;
        N *parentNode;
        uint64_t v = 0;
        uint64_t vp;

        while (true) {
            parentNode = node;
            vp = v;
            node = nextNode;
            PCEqualsResults prefixResult;
            v = node->readLockOrRestart(needRestart);
            if (needRestart) goto restart;
            prefixResult = checkPrefixEquals(node, level, start, end, loadKey, needRestart);
            if (needRestart) goto restart;
            if (parentNode != nullptr) {
                parentNode->readUnlockOrRestart(vp, needRestart);
                if (needRestart) goto restart;
            }
            node->readUnlockOrRestart(v, needRestart);
            if (needRestart) goto restart;

            switch (prefixResult) {
                case PCEqualsResults::NoMatch: {
                    return false;
                }
                case PCEqualsResults::Contained: {
                    copy(node, parentNode, vp);
                    break;
                }
                case PCEqualsResults::BothMatch: {
                    uint8_t startLevel = (start.getKeyLen() > level) ? start[level] : 0;
                    uint8_t endLevel = (end.getKeyLen() > level) ? end[level] : 255;
                    if (startLevel != endLevel) {
                        std::tuple<uint8_t, N *> children[256];
                        uint32_t childrenCount = 0;
                        v = N::getChildren(node, startLevel, endLevel, children, childrenCount);
                        for (uint32_t i = 0; i < childrenCount; ++i) {
                            const uint8_t k = std::get<0>(children[i]);
                            N *n = std::get<1>(children[i]);
                            if (k == startLevel) {
                                findStart(n, k, level + 1, node, v);
                            } else if (k > startLevel && k < endLevel) {
                                copy(n, node, v);
                            } else if (k == endLevel) {
                                findEnd(n, k, level + 1, node, v);
                            }
                            if (toContinue) {
                                break;
                            }
                        }
                    } else {
                        nextNode = N::getChild(startLevel, node);
                        node->readUnlockOrRestart(v, needRestart);
                        if (needRestart) goto restart;
                        level++;
                        continue;
                    }
                    break;
                }
            }
            break;
        }
        if (toContinue != 0) {
            loadKey(toContinue, continueKey);
            return true;
        } else {
            return false;
        }
    }


    TID Tree::checkKey(const TID tid, const Key &k) const {
        Key kt;
        this->loadKey(tid, kt);
        if (k == kt) {
            return tid;
        }
        return 0;
    }

	void Tree::insert(const Key &k, TID tid, ThreadInfo &epocheInfo){
		insert(k, tid, epocheInfo, nullptr);
	}

    void Tree::insert(const Key &k, TID tid, ThreadInfo &epocheInfo, trans_info_t* t_info) {
        EpocheGuard epocheGuard(epocheInfo);
        restart:
        bool needRestart = false;
        if(t_info != nullptr){
            bzero(t_info, sizeof(trans_info_t));
        }

        N *node = nullptr;
        N *nextNode = root;
        N *parentNode = nullptr;
        uint8_t parentKey, nodeKey = 0;
        uint64_t parentVersion = 0;
        uint32_t level = 0;

		bool transactional = false;

		PRINT_DEBUG("-------------------------\nInserting (key:%s, tid: %lu)\n", keyToStr(k).c_str(), tid);

		transactional = t_info != nullptr;
        while (true) {
            parentNode = node;
            parentKey = nodeKey;
            node = nextNode;
            auto v = node->readLockOrRestart(needRestart);
			//PRINT_DEBUG("Is node locked? %u\n", node->isLocked(node->getVersion()))
			if (needRestart)
				goto restart;

            uint32_t nextLevel = level;

            uint8_t nonMatchingKey;
            Prefix remainingPrefix;
            
			PRINT_DEBUG("next level = %u\n", nextLevel);
            auto res = checkPrefixPessimistic(node, k, nextLevel, nonMatchingKey, remainingPrefix,
                                   this->loadKey, needRestart); // increases level
            if (needRestart) goto restart;
            switch (res) {
                case CheckPrefixPessimisticResult::NoMatch: {
                    PRINT_DEBUG("Prefix mismatch!\n")
					if(transactional) { // get current node versions before locking!
                        t_info->updated_node1 = node_vers_t(parentNode, parentVersion);
                        t_info->updated_node2 = node_vers_t(node, v);
                    }
                    parentNode->upgradeToWriteLockOrRestart(parentVersion, needRestart);
                    //PRINT_DEBUG("-- Locking parent node %p\n", parentNode);
					if (needRestart) goto restart;
					//PRINT_DEBUG("-- Locking node %p\n", node);
                    node->upgradeToWriteLockOrRestart(v, needRestart);
                    if (needRestart) {
                        parentNode->writeUnlock();
                        goto restart;
                    }
					PRINT_DEBUG("Prefix of new node will be %u\n", nextLevel - level)
                    // 1) Create new node which will be parent of node, Set common prefix, level to this node
                    auto newNode = new N4(node->getPrefix(), nextLevel - level);
                    #if MEASURE_TREE_SIZE == 1
                    if(transactional)
                        t_info->addedSize = 36; // 4 + 4 * 64-bit pointers
                    #endif

                    // 2)  add node and (tid, *k) as children
                    // Dimos: 0 case: add new node under 0 if nextLevel >= k's length (this means that new key is a prefix of another one and we will
                    // go out of bounds if we do k[nextLevel]
					// Dim STO: do not insert if transactional: We must insert the rec* casted to TID and setLeaf as the value!
					uint8_t keyslice = ((nextLevel >= k.getKeyLen()) ? 0 : k[nextLevel] );
					PRINT_DEBUG("Inserting new leaf node in %c\n", keyslice)
					if(!transactional)
						newNode->insert(keyslice, N::setLeaf(tid));
					else {
						t_info->cur_node = newNode;
						t_info->keyslice = keyslice;
					}
					PRINT_DEBUG("Inserting previous node in %c\n", nonMatchingKey)
                    newNode->insert(nonMatchingKey, node);

                    // 3) upgradeToWriteLockOrRestart, update parentNode to point to the new node, unlock
                    N::change(parentNode, parentKey, newNode);
                    // Dim STO: Do not unlock yet!
					if(!transactional)
						parentNode->writeUnlock();
					else{
						t_info->l_parent_node = parentNode;
                    }
                    // 4) update prefix of node, unlock
                    /*if (node->getPrefixLength() - ((nextLevel - level) + 1) < node->getPrefixLength()){
                        stringstream ss;
                        ss<<"Setting prefix size " << (node->getPrefixLength() - ((nextLevel - level) + 1)) << ", node prefix length: "<<node->getPrefixLength()<<    ", nextLevel: "<< nextLevel <<", level: "<<level<<endl;
                        cout<<ss.str();
                    }*/
                    node->setPrefix(remainingPrefix,
                                    node->getPrefixLength() - ((nextLevel - level) + 1));
					// Dim STO: Do not unlock yet!
                    if(!transactional)
						node->writeUnlock();
					else
						t_info->l_node = node;
                    return;
                }
                case CheckPrefixPessimisticResult::Match:
                    break;
            }
            level = nextLevel;
            bool levelBeyondKeyLength = false;
			if (level >= k.getKeyLen()){ // the case where level is beyond the given key bounds
				nodeKey = 0;
				nextNode = N::getChild(nodeKey, node);
                levelBeyondKeyLength = true;
				// could be an update!
                //assert(nextNode == nullptr);
			}
			else {
				nodeKey = k[level];
				nextNode = N::getChild(nodeKey, node);
			}
            node->checkOrRestart(v,needRestart);
            if (needRestart) goto restart;
			PRINT_DEBUG("level = %u\n", level)
			PRINT_DEBUG("Keyslice at current level: %c\n", (char) nodeKey)
            if (nextNode == nullptr) {
				PRINT_DEBUG("Next node is null, insert!\n")
                N::insert(node, v, parentNode, parentVersion, parentKey, nodeKey, N::setLeaf(tid), needRestart, t_info, epocheInfo);
                if (needRestart) goto restart;
				return;
            }

            if (parentNode != nullptr) {
				//PRINT_DEBUG("Parent is non-null, read unlock!\n")
                parentNode->readUnlockOrRestart(parentVersion, needRestart);
                if (needRestart) goto restart;
            }

            if (N::isLeaf(nextNode)) {
				PRINT_DEBUG("Next node is leaf, expand it!\n")
                //PRINT_DEBUG("-- Locking node %p\n", node);
                // Dimos: Do not get write lock if transactional as it could be an update! We don't need a write lock in the update when we are in transactional mode! The updated tid will be added in the write set and updated at STO commit time
                if(!transactional) {
                    node->upgradeToWriteLockOrRestart(v, needRestart);
				    if (needRestart) goto restart;
                }
                Key key;
                loadKey(N::getLeaf(nextNode), key);
                // Dimos: Do not increase the level if we hit the '0' keyslice or we will go out of key bounds
                //if(nodeKey != 0)
                if(!levelBeyondKeyLength)
				    level++;
                uint32_t prefixLength = 0;
                // Dimos: Must check whether level is within the key bounds!
                PRINT_DEBUG("Calculate common prefix of keys: %s | %s\n", keyToStr(key).c_str(), keyToStr(k).c_str())
                if (level < key.getKeyLen() && level < k.getKeyLen()) {
					while (key[level + prefixLength] == k[level + prefixLength]) {
                    	prefixLength++;
						if (level + prefixLength >= key.getKeyLen() || level + prefixLength >= k.getKeyLen())
							break;
                	}
				}
                // Dimos: handle updates!
                if(prefixLength + level == k.getKeyLen() && (k.getKeyLen() == key.getKeyLen())) { // update existing key!
                    PRINT_DEBUG("Updating!\n")
                    if(!transactional){
                        PRINT_DEBUG("Updating from tid %lu to %lu\n", N::getLeaf(nextNode), tid)
                        N::change(node, nodeKey, N::setLeaf(tid));
                        node->writeUnlock();
                    }
                    else {
                        t_info->updatedVal = tid;
                        t_info->prevVal = N::getLeaf(nextNode);
                        // we don't need to lock if it's an update!
                        //t_info->l_node = node;
                    }
                    return;
                }
                PRINT_DEBUG("Prefix length: %u, level: %u, key length: %u\n", prefixLength, level, k.getKeyLen())
				// Dimos: 0 case: there is a case that level goes out of key bounds! In this case prefixLength will be zero (if statement above will
				//  not be executed)
				auto n4 = new N4((level < k.getKeyLen() ? &k[level] : nullptr), prefixLength);
				#if MEASURE_TREE_SIZE == 1
                    if(transactional)
                        t_info->addedSize = 36; // 4 + 4 64-bit pointers
                #endif
                // insert in keyscice 0 if index is out of key bounds
				uint8_t keyslice = ( (level + prefixLength < k.getKeyLen()) ? k[level + prefixLength] : 0 );
				// Dim STO: do not insert if transactional
				PRINT_DEBUG("Inserting new leaf node in keyslice %c (level: %u, prefixLength: %u, key: %s)\n", keyslice, level, prefixLength, keyToStr(k).c_str())
				if(!transactional)
					n4->insert(keyslice, N::setLeaf(tid));
				else {
					t_info->cur_node = n4;
					t_info->keyslice = keyslice;
				}
				// Dimos: 0 case: Must check whether level is within the key bounds!
				// if level is not within key bounds, insert previous node in keyslice 0!
				//  eg. AB and ABBA: AB must end to a leaf stored in keyslice 0 and ABBA to a leaf with keyslice B
				PRINT_DEBUG("level: %u, prefix length: %u, key length: %u\n", level, prefixLength, key.getKeyLen())
				if (level + prefixLength < key.getKeyLen()){
					PRINT_DEBUG("Inserting previous node in keyslice %c (key (as got from loadKey): %s)\n", key[level + prefixLength], keyToStr(key).c_str())
					n4->insert(key[level + prefixLength], nextNode);
				}
				else {
					n4->insert(0, nextNode);
					PRINT_DEBUG("Inserting previous node in keyslice 0!\n")
				}
                // grab the write lock now
                if(transactional){
                    t_info->updated_node1 = node_vers_t(node, v); // get the version before locking! Increases by 2 at lock!
                    node->upgradeToWriteLockOrRestart(v, needRestart);
                    if (needRestart) goto restart;
                }
                
                // TODO node set: we might need to update the version number of this node, if in node set! Line 562: we do store node in the t_info->updated_node1.
                N::change(node, k[level - 1], n4);
                // Dim STO: do not unlock now!
                if(!transactional)
					node->writeUnlock();
				else
					t_info->l_node = node;
                return;
            }
            level++;
            parentVersion = v;
        }
    }

    void Tree::remove(const Key&k, TID tid, ThreadInfo &threadInfo){
        remove(k, tid, threadInfo, nullptr);
    }

    void Tree::remove(const Key &k, TID tid, ThreadInfo &threadInfo, trans_info_t* t_info) {
        EpocheGuard epocheGuard(threadInfo);
        restart:
        bool needRestart = false;

        N *node = nullptr;
        N *nextNode = root;
        N *parentNode = nullptr;
        uint8_t parentKey, nodeKey = 0;
        uint64_t parentVersion = 0;
        uint32_t level = 0;

        bool transactional = (t_info != nullptr);

        while (true) {
            parentNode = node;
            parentKey = nodeKey;
            node = nextNode;
            auto v = node->readLockOrRestart(needRestart);
            if (needRestart) goto restart;

            switch (checkPrefix(node, k, level)) { // increases level
                case CheckPrefixResult::NoMatch:
					node->readUnlockOrRestart(v, needRestart);
                    if (needRestart) goto restart;
                    if(transactional)
                        t_info->shouldAbort = true; // let the caller know that the element was not deleted! We are using the shouldAbort field so that to not include an extra field for 'deleted'
					return;
                case CheckPrefixResult::OptimisticMatch:
                    // fallthrough
                case CheckPrefixResult::Match: {
                    // Dimos: Handle the common prefix/suffix case (was earlier 0 case)
                    if(level == k.getKeyLen())
						nodeKey = 0;
					else
						nodeKey = k[level];
					nextNode = N::getChild(nodeKey, node);

                    node->checkOrRestart(v, needRestart);
                    if (needRestart) goto restart;
                    if (nextNode == nullptr) {
                        node->readUnlockOrRestart(v, needRestart);
                        if (needRestart) goto restart;
                        if(transactional)
                            t_info->shouldAbort = true; // let the caller know that the element was not deleted! We are using the shouldAbort field so that to not include an extra field for 'deleted'
						return;
                    }
                    if (N::isLeaf(nextNode)) {
                        if (N::getLeaf(nextNode) != tid) {
							cout <<"TID mismatch! provided tid: "<< tid<<", found TID: " << N::getLeaf(nextNode) << ". Will not remove!"<<endl;
                            node->readUnlockOrRestart(v, needRestart);
                            if (needRestart) goto restart;
                            if(transactional)
                                t_info->shouldAbort = true; // let the caller know that the element was not deleted! We are using the shouldAbort field so that to not include an extra field for 'deleted'
                            return;
                        }
                        assert(parentNode == nullptr || node->getCount() != 1);
                        if (node->getCount() == 2 && parentNode != nullptr) {
                            parentNode->upgradeToWriteLockOrRestart(parentVersion, needRestart);
                            if (needRestart) goto restart;

                            node->upgradeToWriteLockOrRestart(v, needRestart);
                            if (needRestart) {
                                parentNode->writeUnlock();
                                goto restart;
                            }
                            // 1. check remaining entries
                            N *secondNodeN;
                            uint8_t secondNodeK;
                            std::tie(secondNodeN, secondNodeK) = N::getSecondChild(node, nodeKey);
                            if (N::isLeaf(secondNodeN)) {
								//N::remove(node, k[level]); not necessary
                                N::change(parentNode, parentKey, secondNodeN);
                                parentNode->writeUnlock();
                                node->writeUnlockObsolete();
                                this->epoche.markNodeForDeletion(node, threadInfo);
                            } else {
                                secondNodeN->writeLockOrRestart(needRestart);
                                if (needRestart) {
                                    node->writeUnlock();
                                    parentNode->writeUnlock();
                                    goto restart;
                                }

                                //N::remove(node, k[level]); not necessary
                                N::change(parentNode, parentKey, secondNodeN);
								parentNode->writeUnlock();
                                secondNodeN->addPrefixBefore(node, secondNodeK);
								secondNodeN->writeUnlock();
								node->writeUnlockObsolete();
								this->epoche.markNodeForDeletion(node, threadInfo);
                            }
                        } else {
							// Dimos: 0 case
                            //N::remove(node, v, k[level], parentNode, parentVersion, parentKey, needRestart, threadInfo);
                            N::remove(node, v, nodeKey, parentNode, parentVersion, parentKey, needRestart, threadInfo);
                            if (needRestart) goto restart;
                        }
                        return;
                    }
                    level++;
                    parentVersion = v;
                }
            }
        }
    }


    inline typename Tree::CheckPrefixResult Tree::checkPrefix(N *n, const Key &k, uint32_t &level) {
        if (n->hasPrefix()) {
			//PRINT_DEBUG("checkPrefix()-> Key length: %u, level: %u, prefix length: %u, node has keyslice 0? %u\n", k.getKeyLen(), level, n->getPrefixLength(), N::getChild(0, n)!=nullptr)
			// Dimos: handle the 0 case
			if (k.getKeyLen() < level + n->getPrefixLength() || ( k.getKeyLen() == level + n->getPrefixLength() && N::getChild(0, n) == nullptr ) ) {
				PRINT_DEBUG("1) No match!")
				return CheckPrefixResult::NoMatch;
            }
            for (uint32_t i = 0; i < std::min(n->getPrefixLength(), maxStoredPrefixLength); ++i) {
                if (n->getPrefix()[i] != k[level]) {
					PRINT_DEBUG("2) No match!")
					return CheckPrefixResult::NoMatch;
                }
                ++level;
            }
            if (n->getPrefixLength() > maxStoredPrefixLength) {
                level = level + (n->getPrefixLength() - maxStoredPrefixLength);
                return CheckPrefixResult::OptimisticMatch;
            }
        }
        return CheckPrefixResult::Match;
    }

    int memcpy_thread_safe(uint8_t* dest, const uint8_t* src, size_t len){
        for(size_t i=0; i<len; i++){
            if(src == nullptr)
                return -1;
            *(dest+i) = *(src+i);
        }
        return 0;
    }

    typename Tree::CheckPrefixPessimisticResult Tree::checkPrefixPessimistic(N *n, const Key &k, uint32_t &level,
                                                                        uint8_t &nonMatchingKey,
                                                                        Prefix &nonMatchingPrefix,
                                                                        LoadKeyFunction loadKey, bool &needRestart) {
        if (n->hasPrefix()) {
            uint32_t prevLevel = level;
            Key kt;
            auto v = n->readLockOrRestart(needRestart);
            uint32_t prefixLen = n->getPrefixLength();
            n->readUnlockOrRestart(v, needRestart);
            if (needRestart){
                //cout<<"NEEDS RESTART!\n";
                return CheckPrefixPessimisticResult::Match;
            }
            v = n->readLockOrRestart(needRestart);
            for (uint32_t i = 0; i < prefixLen; ++i) {
                // Dimos: fixed bug: We must check whether node changed by a concurrent thread!
                //auto v = n->readLockOrRestart(needRestart);
				//PRINT_DEBUG("Key length: %u, prefix length: %u, level: %u\n", k.getKeyLen(), prefixLen, level)
                if (i == maxStoredPrefixLength) {
                    auto anyTID = N::getAnyChildTid(n, needRestart);
                    if (needRestart) return CheckPrefixPessimisticResult::Match;
                    loadKey(anyTID, kt);
                }
                uint8_t curKey = i >= maxStoredPrefixLength ? kt[level] : n->getPrefix()[i];
                // Dimos: the 0 case: level is >= than the given key length: prefix mismatch (case with STOHASTIC, STOH and inserting STO)!
				if (level >= k.getKeyLen() || curKey != k[level]) {
                    nonMatchingKey = curKey;
                    if (prefixLen > maxStoredPrefixLength) {
						PRINT_DEBUG("prefix length is greater than the max stored prefix length!\n")
                        if (i < maxStoredPrefixLength) {
                            auto anyTID = N::getAnyChildTid(n, needRestart);
                            if (needRestart) return CheckPrefixPessimisticResult::Match;
                            loadKey(anyTID, kt);
                        }
                        memcpy(nonMatchingPrefix, &kt[0] + level + 1, std::min((prefixLen - (level - prevLevel) - 1),
                                                                           maxStoredPrefixLength));
                    } else {
						// Dimos: fixed bug: We must check whether node changed by a concurrent thread!
						// program will crash if n->getPrefixLength() has changed!
						// We now get the current value of prefixLength above, right before the for loop
                        /*n->readUnlockOrRestart(v, needRestart);
						if (needRestart){
                            //cout<<"NEEDS RESTART!\n";
                            return CheckPrefixPessimisticResult::Match;
                        }
                        if(n==nullptr){ //someone change it! Just restart!
                            needRestart = true;
                            return CheckPrefixPessimisticResult::Match;
                        }
						if(n->getPrefixLength() <= i) {// someone changed it!!
							//cout << "SOMEONE CHANGED NODE!\n";
                            n->readUnlockOrRestart(v, needRestart);
                            if (needRestart) return CheckPrefixPessimisticResult::Match;
						}
                        //if(n->getPrefixLength() == 0){
                        //    cout<<"Oh no!\n";
                        //}
					    if( ((int)n->getPrefixLength()) - (int)i - 1 < 0){
                            needRestart = true;
                            return CheckPrefixPessimisticResult::Match;
                        }*/
					    memcpy(nonMatchingPrefix, n->getPrefix() + i + 1, prefixLen - i - 1);
                    }
                    n->readUnlockOrRestart(v, needRestart);
                    if(needRestart){
                        return CheckPrefixPessimisticResult::Match;
                    }
					PRINT_DEBUG("No match! Common prefix is until level %u\n", level)
                    return CheckPrefixPessimisticResult::NoMatch;
                }
                ++level;
            }
            n->readUnlockOrRestart(v, needRestart);
            if(needRestart){
                return CheckPrefixPessimisticResult::Match;
            }
        }
        return CheckPrefixPessimisticResult::Match;
    }

    typename Tree::PCCompareResults Tree::checkPrefixCompare(const N *n, const Key &k, uint8_t fillKey, uint32_t &level,
                                                        LoadKeyFunction loadKey, bool &needRestart) {
        if (n->hasPrefix()) {
            Key kt;
            for (uint32_t i = 0; i < n->getPrefixLength(); ++i) {
                if (i == maxStoredPrefixLength) {
                    auto anyTID = N::getAnyChildTid(n, needRestart);
                    if (needRestart) return PCCompareResults::Equal;
                    loadKey(anyTID, kt);
                }
                uint8_t kLevel = (k.getKeyLen() > level) ? k[level] : fillKey;

                uint8_t curKey = i >= maxStoredPrefixLength ? kt[level] : n->getPrefix()[i];
                if (curKey < kLevel) {
                    return PCCompareResults::Smaller;
                } else if (curKey > kLevel) {
                    return PCCompareResults::Bigger;
                }
                ++level;
            }
        }
        return PCCompareResults::Equal;
    }

    typename Tree::PCEqualsResults Tree::checkPrefixEquals(const N *n, uint32_t &level, const Key &start, const Key &end,
                                                      LoadKeyFunction loadKey, bool &needRestart) {
        if (n->hasPrefix()) {
            Key kt;
            for (uint32_t i = 0; i < n->getPrefixLength(); ++i) {
                if (i == maxStoredPrefixLength) {
                    auto anyTID = N::getAnyChildTid(n, needRestart);
                    if (needRestart) return PCEqualsResults::BothMatch;
                    loadKey(anyTID, kt);
                }
                uint8_t startLevel = (start.getKeyLen() > level) ? start[level] : 0;
                uint8_t endLevel = (end.getKeyLen() > level) ? end[level] : 255;

                uint8_t curKey = i >= maxStoredPrefixLength ? kt[level] : n->getPrefix()[i];
                if (curKey > startLevel && curKey < endLevel) {
                    return PCEqualsResults::Contained;
                } else if (curKey < startLevel || curKey > endLevel) {
                    return PCEqualsResults::NoMatch;
                }
                ++level;
            }
        }
        return PCEqualsResults::BothMatch;
    }
}
