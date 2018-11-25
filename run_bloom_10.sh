if [ "$1" == "" ] || [ "$2" == "" ] || [ "$3" == "" ]
then
echo "Usage: $0 <num of keys> <num of operations> <insert ratio> <1: single NUMA node, 0: all NUMA nodes>"
exit
fi

i=0
if [ "$4" == 1 ]
then
	numa_prefix="numactl --physcpubind=0,4,8,12,16,20,24,28,32,36,40,44,48,52,56,60,64,68,72,76 -- "
else
	numa_prefix=
fi
while [ $i -lt 10 ]
do
	$numa_prefix ./test_bloom -n $1 -o $2 -i $3 0 > output_$i
	i=$(($i+1))
done
