CC=gcc
FLAGS="-std=gnu17 -O3 -g -lpthread -D_GNU_SOURCE"
#FLAGS="-std=gnu17  -ggdb -lpthread -D_GNU_SOURCE"
#FLAGS="-std=gnu17 -ggdb -lpthread -D_GNU_SOURCE -fsanitize=address -fsanitize=undefined"

$CC thread_pool_benchmark.c $FLAGS -o thread_pool_benchmark
$CC thread_pool_test.c $FLAGS -o thread_pool_test
$CC files_test.c $FLAGS -o files_test
$CC files_benchmark.c $FLAGS -o files_benchmark
./thread_pool_benchmark
./thread_pool_test
./files_test
./files_benchmark
