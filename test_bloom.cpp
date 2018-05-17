#include <iostream>
#include <chrono>
#include "tbb/tbb.h"

using namespace std;

#include "OptimisticLockCoupling/Tree.h"
#include "ROWEX/Tree.h"
#include "ART/Tree.h"

#define N_KEYS 10000000

#define SINGLE_TREE 0
#define MISS_RATIO_MOD 2
#define SEQ_INSERT1 0
#define SEQ_INSERT2 1

#define MAX_CPUS 80

#define PRINT_LATENCIES 0

#define BLOOM_SIZE 100000 // 6.4MB size

#include <thread>
#include <sched.h>
#include <map>
#include <mutex>

// if we use a #define, the performance is different: the i%_NKEYS will be calculated on compile time and it is an expensive operation.
uint64_t n_keys = 10000000;

std::atomic<uint64_t> bloom [BLOOM_SIZE];


void bloom_insert(uint64_t key){
	
	
}

void loadKey(TID tid, Key &key) {
    // Store the key of the tuple into the key vector
    // Implementation is database specific
    key.setKeyLen(sizeof(tid));
    reinterpret_cast<uint64_t *>(&key[0])[0] = __builtin_bswap64(tid);
}

void singlethreaded(char **argv, bool all_lookups) {
    std::cout << "single threaded:" << std::endl;
    uint64_t n = std::atoll(argv[1]);
	uint64_t lookups_n = (all_lookups? n : std::atoll(argv[2]));
    uint64_t *keys = new uint64_t[n_keys];

    // Generate keys
    for (uint64_t i = 0; i < n_keys; i++)
        // dense, sorted
        keys[i] = i + 1;
    if (atoi(argv[(all_lookups? 2: 3) ]) == 1)
        // dense, random
        std::random_shuffle(keys, keys + n_keys);
    if (atoi(argv[(all_lookups? 2: 3) ]) == 2)
        // "pseudo-sparse" (the most-significant leaf bit gets lost)
        for (uint64_t i = 0; i < n_keys; i++)
            keys[i] = (static_cast<uint64_t>(rand()) << 32) | static_cast<uint64_t>(rand());

    printf("operation,n,ops/ms\n");
    ART_unsynchronized::Tree tree_rw(loadKey);
	#if !SINGLE_TREE
		ART_unsynchronized::Tree tree_compacted(loadKey);
	#endif
    // Build tree
    {
        auto starttime = std::chrono::system_clock::now();
        for (uint64_t i=0; i!= n; i++){
			Key key;
			loadKey(keys[i], key);
			tree_rw.insert(key, keys[i]);
		}
		#if !SINGLE_TREE 
			for (uint64_t i = 0; i != n_keys; i++) {
            	Key key;
				loadKey(keys[i], key);
				tree_compacted.insert(key, keys[i]);
        	}
		#endif
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now() - starttime);
        printf("insert,%ld,%f\n", n, (n * 1.0) / duration.count());
    }

    {
        // Lookup
        auto starttime = std::chrono::system_clock::now();
        for (uint64_t i = 0; i != lookups_n; i++) {
			Key key;
            uint64_t ind=0;
        	#if SINGLE_TREE
				ind=i%n;
				loadKey(keys[ind], key);
				auto val1 = tree_rw.lookup(key);
				if(val1 != keys[ind]){
					std::cout << "wrong key read: " << val1 << " expected: " << keys[ind] << std::endl;
					throw;
				}
			#else
				bool found = true;
				if(i%MISS_RATIO_MOD == 0){
					loadKey(200000000+i, key); // simulate an absent key, so that to go to the compact ART tree
					found = false;
					ind = i%n_keys;
				}
				else{
					ind = i%n;
					loadKey(keys[ind], key);
				}
				auto val1 = tree_rw.lookup(key);
				if (found && val1 != keys[ind]){
					std::cout << "wrong key read: " << val1 << " expected: " << keys[ind] << std::endl;
					throw;
				}
				if (!found){
					loadKey(keys[ind], key);
					auto val2 = tree_compacted.lookup(key);
					if (val2 != keys[ind]) {
						std::cout << "wrong key read: " << val2 << " expected:" << keys[ind] << std::endl;
						throw;
					}
				}
				
			#endif
        }
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now() - starttime);
        printf("single lookup\t%ld\t%f\n", lookups_n, (lookups_n * 1.0) / duration.count());
    }

    {
        auto starttime = std::chrono::system_clock::now();

        for (uint64_t i = 0; i != n; i++) {
            Key key;
            loadKey(keys[i], key);
            tree_rw.remove(key, keys[i]);
			#if !SINGLE_TREE
				tree_compacted.remove(key, keys[i]);
			#endif
        }
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now() - starttime);
        printf("remove,%ld,%f\n", n, (n * 1.0) / duration.count());
    }
    delete[] keys;

    std::cout << std::endl;
}

void singlethreaded(char **argv){
    singlethreaded(argv, true);
}

void set_affinity(std::thread& t, unsigned i){
	cpu_set_t cpuset;
	CPU_ZERO(&cpuset);
    CPU_SET(i, &cpuset);
	int rc = pthread_setaffinity_np(t.native_handle(), sizeof(cpu_set_t), &cpuset);
	if (rc != 0) {
		std::cerr << "Error calling pthread_setaffinity_np: " << rc << "\n";
	}
}

void multithreaded(char **argv, bool all_lookups) {
    std::cout << "multi threaded:" << std::endl;
	//tbb::task_scheduler_init init(20);

    uint64_t n = std::atoll(argv[1]);
   	uint64_t lookups_n = (all_lookups? n : std::atoll(argv[2]));
	uint64_t *keys = new uint64_t[n_keys];

    // Generate keys
    for (uint64_t i = 0; i < n_keys; i++)
        // dense, sorted
        keys[i] = i + 1;
    if (atoi(argv[(all_lookups? 2: 3)]) == 1)
        // dense, random
        std::random_shuffle(keys, keys + n_keys);
    if (atoi(argv[(all_lookups? 2: 3)]) == 2)
        // "pseudo-sparse" (the most-significant leaf bit gets lost)
        for (uint64_t i = 0; i < n_keys; i++)
            keys[i] = (static_cast<uint64_t>(rand()) << 32) | static_cast<uint64_t>(rand());

    printf("operation,n,ops/ms\n");
    ART_OLC::Tree tree_rw(loadKey);
	#if !SINGLE_TREE
		ART_OLC::Tree tree_compacted(loadKey);
	#endif
    //ART_ROWEX::Tree tree(loadKey);

    // Build tree
    {
        auto starttime = std::chrono::system_clock::now();
        auto t1 = tree_rw.getThreadInfo();
		#if SEQ_INSERT1
		for (uint64_t i = 0; i<n; i++){
			Key key;
			loadKey(keys[i], key);
			tree_rw.insert(key, keys[i], t1);
		}
		#else
		tbb::parallel_for(tbb::blocked_range<uint64_t>(0, n), [&](const tbb::blocked_range<uint64_t> &range) {
            auto t1 = tree_rw.getThreadInfo();
			for (uint64_t i = range.begin(); i != range.end(); i++) {
                Key key;
                loadKey(keys[i], key);
                tree_rw.insert(key, keys[i], t1);
            }
        });
		#endif
		// if I insert in the second tree as well, it becomes faster on smaller trees: NUMA effect! Use numactl to use CPUs from the same socket!
		// even with a single socket we see a difference when inserting the keys of tree_compacted sequentially vs in parallel.
		// When we insert sequentially, a single L1 cache is 'pollutted' with 64KB (1,000 keys) tree_compacted entries since lookups access only tree_rw.
		// When we insert in parallel, all 10 L1 caches will be 'pollutted', but less (6.4KB on average). This leaves more tree_rw entries in their caches.
		#if !SINGLE_TREE
			#if SEQ_INSERT2
			auto t2 = tree_compacted.getThreadInfo();
				for (uint64_t i = 0; i<n; i++){
					Key key;
					loadKey(keys[i], key);
					tree_compacted.insert(key, keys[i], t2);
				}
			#else
			tbb::parallel_for(tbb::blocked_range<uint64_t>(0, n_keys), [&](const tbb::blocked_range<uint64_t> &range) {
				//int cpu = sched_getcpu();
				//printf("Inserting. Current CPU: %d\n", cpu);
				auto t2 = tree_compacted.getThreadInfo();
				for (uint64_t i = range.begin(); i != range.end(); i++) {
                	Key key;
                	loadKey(keys[i], key);
                	tree_compacted.insert(key, keys[i], t2);
            	}
			});
			#endif
		#endif
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now() - starttime);
        printf("insert,%ld,%f\n", n, (n * 1.0) / duration.count());
    }

    {
        // Lookup
        std::mutex mtx;
		//tbb::affinity_partitioner ap;
		auto starttime = std::chrono::system_clock::now();
		tbb::parallel_for(tbb::blocked_range<uint64_t>(0, lookups_n), [&](const tbb::blocked_range<uint64_t> &range) {
            //int cpu = sched_getcpu();
			//printf("Current CPU: %d\n", cpu);
			//unsigned num_cpus = std::thread::hardware_concurrency();
  			//std::cout << "Launching " << num_cpus << " threads\n";
			auto t1 = tree_rw.getThreadInfo();
			#if !SINGLE_TREE
				//auto t2 = tree_compacted.getThreadInfo();
			#endif
			#if PRINT_LATENCIES
			uint64_t indLatencySum = 0, loadLatencySum = 0, lookupLatencySum = 0;
			uint64_t cnt = 0;
			#endif
			for (uint64_t i = range.begin(); i != range.end(); i++) {
	            Key key;
				uint64_t ind = 0;
				#if SINGLE_TREE
                	#if PRINT_LATENCIES
					auto starttimeInd = std::chrono::system_clock::now();
					#endif
					ind=i%n;
					#if PRINT_LATENCIES
					auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(
                					std::chrono::system_clock::now() - starttimeInd);
					indLatencySum += duration.count();
					auto starttimeLoad = std::chrono::system_clock::now();
					#endif
					loadKey(keys[ind], key);
					#if PRINT_LATENCIES
					duration = std::chrono::duration_cast<std::chrono::nanoseconds>(
									std::chrono::system_clock::now() - starttimeLoad);
					loadLatencySum += duration.count();
					auto starttimeLookup = std::chrono::system_clock::now();
                	#endif
					auto val1 = tree_rw.lookup(key, t1);
					#if PRINT_LATENCIES
					duration = std::chrono::duration_cast<std::chrono::nanoseconds>(
								std::chrono::system_clock::now() - starttimeLookup);
					lookupLatencySum +=  duration.count();
					cnt++;
					#endif
                	if(val1 != keys[ind]){
                    	std::cout << "wrong key read: " << val1 << " expected: " << keys[ind] << std::endl;
                    	throw;
                	}
            	#else
					#if 0
					bool found = true;
					if(i%MISS_RATIO_MOD == 0){
                    	loadKey(200000000+i, key); // simulate an absent key, so that to go to the compact ART tree
                    	found = false;
                    	ind = i%n_keys;
                	}
                	else{
                    	ind = i%n;
                    	loadKey(keys[ind], key);
                	}
                	auto val1 = tree_rw.lookup(key, t1);
                	if (found && val1 != keys[ind]){
                    	std::cout << "wrong key read: " << val1 << " expected: " << keys[ind] << std::endl;
                    	throw;
                	}
                	if (!found){
                    	loadKey(keys[ind], key);
                    	auto val2 = tree_compacted.lookup(key, t2);
                    	if (val2 != keys[ind]) {
                        	std::cout << "wrong key read: " << val2 << " expected:" << keys[ind] << std::endl;
                        	throw;
                    	}
                	}
					#endif
					uint64_t val2;
					#if PRINT_LATENCIES
					auto starttimeInd = std::chrono::system_clock::now();
					#endif
					ind = i%n;
					#if PRINT_LATENCIES	
					auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(
									std::chrono::system_clock::now() - starttimeInd);
					indLatencySum += duration.count();
					auto starttimeLoad = std::chrono::system_clock::now();
					#endif
					loadKey(keys[ind], key);
					#if PRINT_LATENCIES
					duration = std::chrono::duration_cast<std::chrono::nanoseconds>(
                                     std::chrono::system_clock::now() - starttimeLoad);
					loadLatencySum += duration.count();
					auto starttimeLookup = std::chrono::system_clock::now();
					#endif
					auto val1 = tree_rw.lookup(key, t1);
					#if PRINT_LATENCIES
					duration = std::chrono::duration_cast<std::chrono::nanoseconds>(
									std::chrono::system_clock::now() - starttimeLookup);
					lookupLatencySum += duration.count();
					cnt++;
					#endif
					if (val1 != keys[ind]) {
                    	std::cout << "wrong key read: " << val2 << " expected:" << keys[ind] << std::endl;
                        throw;
                    }
				#endif
			}
			#if PRINT_LATENCIES
			mtx.lock();
			cout<< "load: "<< (double) loadLatencySum/cnt<<endl;
			cout<< "lookup: "<< (double) lookupLatencySum/cnt<<endl;
			cout<< "index: "<< (double) indLatencySum/cnt<<endl;
			mtx.unlock();
			#endif
		});
		auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                                    std::chrono::system_clock::now() - starttime);
		printf("multi lookup\t%ld\t%f\n", lookups_n, (lookups_n * 1.0) / duration.count());
    }

    {
        auto starttime = std::chrono::system_clock::now();

        tbb::parallel_for(tbb::blocked_range<uint64_t>(0, n), [&](const tbb::blocked_range<uint64_t> &range) {
            auto t1 = tree_rw.getThreadInfo();
			#if !SINGLE_TREE
				auto t2 = tree_compacted.getThreadInfo();
			#endif
            for (uint64_t i = range.begin(); i != range.end(); i++) {
                Key key;
                loadKey(keys[i], key);
                tree_rw.remove(key, keys[i], t1);
				#if !SINGLE_TREE
					tree_compacted.remove(key, keys[i], t2);
            	#endif
			}
        });
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now() - starttime);
        printf("remove,%ld,%f\n", n, (n * 1.0) / duration.count());
    }
    delete[] keys;
}

void multithreaded(char **argv){
    multithreaded(argv, true);
}

int main(int argc, char **argv) {
	if (argc < 3 || argc > 4) {
        printf("usage: %s n [n lookups] 0|1|2\nn: number of keys\n0: sorted keys\n1: dense keys\n2: sparse keys\n", argv[0]);
        return 1;
    }
    //singlethreaded(argv, argc==3);

    multithreaded(argv, argc==3);

    return 0;
}
