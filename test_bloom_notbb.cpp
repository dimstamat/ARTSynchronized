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

#define MAX_CPUS 80

#include <thread>
#include <sched.h>
#include <map>
#include <mutex>

unsigned CPUS [] = {0,4,8,12,16,20,24,28,32,36,40,44,48,52,56,60,64,68,72,76};

// if we use a #define, the performance is different: the i%_NKEYS will be calculated on compile time and it is an expensive operation.
uint64_t n_keys = 10000000;

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

    printf("operation,n,ops/us\n");
    ART_unsynchronized::Tree tree1(loadKey);
	#if !SINGLE_TREE
		ART_unsynchronized::Tree tree2(loadKey);
	#endif
    // Build tree
    {
        auto starttime = std::chrono::system_clock::now();
        for (uint64_t i=0; i!= n; i++){
			Key key;
			loadKey(keys[i], key);
			tree1.insert(key, keys[i]);
		}
		#if !SINGLE_TREE 
			for (uint64_t i = 0; i != n_keys; i++) {
            	Key key;
				loadKey(keys[i], key);
				tree2.insert(key, keys[i]);
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
				//auto starttime1 = std::chrono::system_clock::now();
				ind=i%n;
				loadKey(keys[ind], key);
				auto val1 = tree1.lookup(key);
				if(val1 != keys[ind]){
					std::cout << "wrong key read: " << val1 << " expected: " << keys[ind] << std::endl;
					throw;
				}
				//auto duration1 = std::chrono::duration_cast<std::chrono::nanoseconds>(
				//					std::chrono::system_clock::now() - starttime1);
				//printf("single-lookup: %ld\n", duration1.count());
			#else
				//auto starttime2 = std::chrono::system_clock::now();
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
				auto val1 = tree1.lookup(key);
				if (found && val1 != keys[ind]){
					std::cout << "wrong key read: " << val1 << " expected: " << keys[ind] << std::endl;
					throw;
				}
				if (!found){
					loadKey(keys[ind], key);
					auto val2 = tree2.lookup(key);
					if (val2 != keys[ind]) {
						std::cout << "wrong key read: " << val2 << " expected:" << keys[ind] << std::endl;
						throw;
					}
					//auto duration2 = std::chrono::duration_cast<std::chrono::nanoseconds>(
					//				std::chrono::system_clock::now() - starttime2);
					//printf("double-lookup: %ld\n", duration2.count());
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
            tree1.remove(key, keys[i]);
			#if !SINGLE_TREE
				tree2.remove(key, keys[i]);
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
	//cout <<"Setting thread " << t.get_id() << " to cpu "<< i<<endl;
	cpu_set_t cpuset;
	CPU_ZERO(&cpuset);
    CPU_SET(i, &cpuset);
	int rc = pthread_setaffinity_np(t.native_handle(), sizeof(cpu_set_t), &cpuset);
	if (rc != 0) {
		std::cerr << "Error calling pthread_setaffinity_np: " << rc << "\n";
	}
}


void start_thread_lookups(std::thread * thread_pool, ART_OLC::Tree& tree, uint64_t* keys, uint64_t& lookups_n, unsigned n_threads){
	    unsigned bucket_size = lookups_n/n_threads;
        unsigned ind_start, ind_end;
        for(unsigned i=0; i<n_threads; i++){
            ind_start = i*bucket_size;
            ind_end = ind_start + bucket_size;
            thread_pool[i] = std::thread([&tree, &keys, ind_start, ind_end] {
                auto t = tree.getThreadInfo();
                for(unsigned i=ind_start; i<ind_end; i++){
                    uint64_t ind = i%n_keys;
                    Key key;
                    loadKey(keys[ind], key);
                    auto val = tree.lookup(key, t);
                    if (val != keys[ind]) {
                        std::cout << "wrong key read: " << val << " expected:" << keys[ind] << std::endl;
                        throw;
                    }
                }
            });
        }
}


void multithreaded(char **argv, bool all_lookups) {
    std::cout << "multi threaded:" << std::endl;

	tbb::task_scheduler_init init(20);

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

    printf("operation,n,ops/us\n");
    ART_OLC::Tree tree1(loadKey);
	#if !SINGLE_TREE
		ART_OLC::Tree tree2(loadKey);
	#endif
    //ART_ROWEX::Tree tree(loadKey);

    // Build tree
    {
        auto starttime = std::chrono::system_clock::now();
        auto t1 = tree1.getThreadInfo();
		/*for (uint64_t i = 0; i<n; i++){
			Key key;
			loadKey(keys[i], key);
			tree1.insert(key, keys[i], t1);
		}*/
		tbb::parallel_for(tbb::blocked_range<uint64_t>(0, n), [&](const tbb::blocked_range<uint64_t> &range) {
            auto t1 = tree1.getThreadInfo();
			for (uint64_t i = range.begin(); i != range.end(); i++) {
                Key key;
                loadKey(keys[i], key);
                tree1.insert(key, keys[i], t1);
            }
        });
		// if I insert in the second tree as well, it becomes faster on smaller trees: NUMA effect! Use numactl to use CPUs from the same socket!
		#if !SINGLE_TREE
			auto t2 = tree2.getThreadInfo();
			tbb::parallel_for(tbb::blocked_range<uint64_t>(0, n_keys), [&](const tbb::blocked_range<uint64_t> &range) {
				//int cpu = sched_getcpu();
				//printf("Inserting. Current CPU: %d\n", cpu);
				auto t2 = tree2.getThreadInfo();
				for (uint64_t i = range.begin(); i != range.end(); i++) {
                	Key key;
                	loadKey(keys[i], key);
                	tree2.insert(key, keys[i], t2);
            	}
			});
		#endif
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now() - starttime);
        printf("insert,%ld,%f\n", n, (n * 1.0) / duration.count());
    }

    {
		// for measuring individual latencies
		/*
		struct SumLookupLatencies{
			double val;

			SumLookupLatencies() : val(0) {}

			SumLookupLatencies(SumLookupLatencies& s, split) { val = 0;}

			void operator () (const tbb::blocked_range<uint64_t> &range) {
				auto t = tree1.getThreadInfo();
				long int sum = 0, sum_prev = 0;
				for(uint64_t i = range.begin(); i!= range.end(); i++){
					Key key;
					auto ind = i%n;
					auto starttime = std::chrono::system_clock::now();
					loadKey(keys[ind], key);
					auto val = tree2.lookup(key, t);
					auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(
									std::chrono::system_clock::now() - starttime);
					sum_prev = sum;
					sum+= duration.count();
					// check for overflow!
					if(sum < sum_prev)
						cout << "Overflow while calculating lookup latency avg!!"<<endl;
					if (val != keys[ind]) {
						std::cout << "wrong key read: " << val << " expected:" << keys[ind] << std::endl;
						throw;
					}
				}
				// convert to ms to prevent overflow!
				val = ((double) sum)/1000;
			void join(SumLookupLatencies&rhs) { val += rhs.val;}
		};
		Sum sum;
		tbb::parallel_reduce(tbb::blocked_range<uint64_t>(0, lookups_n), sum);
		*/
        // Lookup

		unsigned n_threads_1 = 10;
		unsigned n_threads_2 = 10;
		// create a pool of threads and do the tree2 lookups in parallel
		std::thread thread_pool [n_threads_1];
		std::thread thread_pool2 [n_threads_2];
		auto starttime = std::chrono::system_clock::now();
		//start_thread_lookups(thread_pool, tree1, keys, lookups_n, n_threads);
		unsigned bucket_size = lookups_n/n_threads_1;
        unsigned ind_start, ind_end;
        for(unsigned i=0; i<n_threads_1; i++){
            ind_start = i*bucket_size;
            ind_end = ind_start + bucket_size;
            thread_pool[i] = std::thread([&tree1, &tree2, &keys, ind_start, ind_end] {
                auto t = tree1.getThreadInfo();
                for(unsigned i=ind_start; i<ind_end; i++){
                    uint64_t ind = i%n_keys;
                    Key key;
                    loadKey(keys[ind], key);
                    auto val1 = tree1.lookup(key, t); 
					auto val2 = tree2.lookup(key, t);
                    if (val1 != keys[ind] ||  val2 != keys[ind]) {
                        std::cout << "wrong key read: (v1: " << val1 << "), (v2: "<< val2<< ") expected:" << keys[ind] << std::endl;
                        throw;
                    }
                }   
            });
			set_affinity(thread_pool[i], CPUS[i]);
        }   

		#if !SINGLE_TREE
			bucket_size = lookups_n/n_threads_2;
        	for(unsigned i=0; i<n_threads_2; i++){
            	ind_start = i*bucket_size;
            	ind_end = ind_start + bucket_size;
            	thread_pool2[i] = std::thread([&tree2, &keys, ind_start, ind_end] {
                	auto t = tree2.getThreadInfo();
                	for(unsigned i=ind_start; i<ind_end; i++){
                    	uint64_t ind = i%n_keys;
                    	Key key;
                    	loadKey(keys[ind], key);
                    	auto val = tree2.lookup(key, t); 
                    	if (val != keys[ind]) {
                        	std::cout << "wrong key read: " << val << " expected:" << keys[ind] << std::endl;
                        	throw;
                    	}
                	}
            	});
				set_affinity(thread_pool2[i], CPUS[n_threads_1 + i]);
        	}
		#endif

		for (unsigned i=0; i<n_threads_1; i++)
			thread_pool[i].join();

		#if !SINGLE_TREE
			for (unsigned i=0; i<n_threads_2; i++)
				thread_pool2[i].join();
		#endif

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
                	auto val1 = tree1.lookup(key, t1);
                	if (found && val1 != keys[ind]){
                    	std::cout << "wrong key read: " << val1 << " expected: " << keys[ind] << std::endl;
                    	throw;
                	}
                	if (!found){
                    	loadKey(keys[ind], key);
                    	auto val2 = tree2.lookup(key, t2);
                    	if (val2 != keys[ind]) {
                        	std::cout << "wrong key read: " << val2 << " expected:" << keys[ind] << std::endl;
                        	throw;
                    	}
                	}
			#endif
		auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                                    std::chrono::system_clock::now() - starttime);
		printf("multi lookup\t%ld\t%f\n", lookups_n, (lookups_n * 1.0) / duration.count());
    }

    {
        auto starttime = std::chrono::system_clock::now();

        tbb::parallel_for(tbb::blocked_range<uint64_t>(0, n), [&](const tbb::blocked_range<uint64_t> &range) {
            auto t1 = tree1.getThreadInfo();
			#if !SINGLE_TREE
				auto t2 = tree2.getThreadInfo();
			#endif
            for (uint64_t i = range.begin(); i != range.end(); i++) {
                Key key;
                loadKey(keys[i], key);
                tree1.remove(key, keys[i], t1);
				#if !SINGLE_TREE
					tree2.remove(key, keys[i], t2);
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
    singlethreaded(argv, argc==3);

    multithreaded(argv, argc==3);

    return 0;
}
