/***************************************************************************
 *  stream/test_asynchronous_distribute_gather.cpp
 *
 *  Part of the STXXL. See http://stxxl.sourceforge.net
 *
 *  Copyright (C) 2009 Johannes Singler <singler@ira.uka.de>
 *
 *  Distributed under the Boost Software License, Version 1.0.
 *  (See accompanying file LICENSE_1_0.txt or copy at
 *  http://www.boost.org/LICENSE_1_0.txt)
 **************************************************************************/

//! \example stream/test_asynchronous_distribute_gather.cpp
//! This is an example of how to use the asynchronous distribute/gather mechanism.

#define STXXL_START_PIPELINE_DEFERRED 1
#define INTERMEDIATE 1
#define PIPELINED 1
#define BATCHED 1

#define OUTPUT_STATS 1

//#define STXXL_VERBOSE_LEVEL 0

#include "test_parallel_pipelining_common.h"
#include <vector>
#include <stxxl/stream>
#include <stxxl/vector>

stxxl::unsigned_type memory_to_use = 2048 * megabyte;
stxxl::unsigned_type run_size = memory_to_use / 4;
stxxl::unsigned_type buffer_size = 16 * megabyte;

void distribute_gather(vector_type & input, bool deferred)
{
    using stxxl::stream::deterministic_round_robin;
    using stxxl::stream::deterministic_distribute;
    using stxxl::stream::streamify;
    using stxxl::stream::async::pull;
    using stxxl::stream::async::pull_batch;
    using stxxl::stream::async::pull_empty;
    using stxxl::stream::async::pull_empty_batch;
    using stxxl::stream::async::dummy_pull;
    using stxxl::stream::async::push;
    using stxxl::stream::async::push_batch;
    using stxxl::stream::async::dummy_push;
    using stxxl::stream::async::push_pull;
    using stxxl::stream::async::connect_pull;
    using stxxl::stream::pusher;
    using stxxl::stream::transform;
    using stxxl::stream::sort;
    using stxxl::stream::runs_creator;
    using stxxl::stream::runs_creator_batch;
    using stxxl::stream::runs_merger;
    using stxxl::stream::make_tuple;
    using stxxl::stream::use_push;

#ifdef BOOST_MSVC
    typedef stxxl::stream::streamify_traits<vector_type::iterator>::stream_type input_stream_type;
#else
    typedef __typeof__(streamify(input.begin(), input.end())) input_stream_type;
#endif //BOOST_MSVC

    stxxl::stats_data stats_begin(*stxxl::stats::get_instance());

    vector_type output(input.size());
    accumulate<my_type> acc1;
    accumulate<my_type> acc2;
    vector_type::iterator o;

    input_stream_type input_stream = streamify(input.begin(), input.end());

    typedef transform<accumulate<my_type>, input_stream_type> accumulate_stream1_type;
    accumulate_stream1_type accumulate_stream1(acc1, input_stream);

    const unsigned int num_workers = 2;

    typedef push_pull<my_type> bucket_type;

    bucket_type * buckets[num_workers];
#if INTERMEDIATE
    typedef transform<identity<my_type>, bucket_type> worker_stream_type;
    typedef pull<worker_stream_type> worker_stream_node_type;
    identity<my_type> id;
    worker_stream_type * workers[num_workers];
    worker_stream_node_type * worker_nodes[num_workers];
#else
    typedef bucket_type worker_stream_node_type;
    worker_stream_node_type ** worker_nodes = buckets;
#endif


    for (unsigned int w = 0; w < num_workers; ++w)
    {
        buckets[w] = new bucket_type(buffer_size, deferred);
#if INTERMEDIATE
        workers[w] = new worker_stream_type(id, *buckets[w]);
        worker_nodes[w] = new worker_stream_node_type(buffer_size, *workers[w], deferred);
#endif
    }

    const stxxl::unsigned_type chunk_size = buffer_size / sizeof(my_type) / (3 * num_workers);

    typedef deterministic_distribute<worker_stream_node_type> distributor_stream_type;
    distributor_stream_type distributor_stream(worker_nodes, num_workers, chunk_size, deferred);

    typedef pusher<accumulate_stream1_type, distributor_stream_type> pusher_stream_type;
    pusher_stream_type pusher_stream(accumulate_stream1, distributor_stream);

#if BATCHED
    typedef pull_empty_batch<pusher_stream_type> pull_empty_type;
#else
    typedef pull_empty<pusher_stream_type> pull_empty_type;
#endif
    pull_empty_type pull_empty_node(pusher_stream, deferred);

    typedef deterministic_round_robin<worker_stream_node_type> fetcher_stream_type;
    fetcher_stream_type fetcher_stream(worker_nodes, num_workers, chunk_size, deferred);

    typedef connect_pull<fetcher_stream_type, pull_empty_type> connect_stream_type;
    connect_stream_type connect_stream(fetcher_stream, pull_empty_node, deferred);

    typedef transform<accumulate<my_type>, connect_stream_type> accumulate_stream_type2;
    accumulate_stream_type2 accumulate_stream2(acc2, connect_stream);

#if BATCHED
    o = materialize_batch(accumulate_stream2, output.begin(), output.end(), 0, deferred);
#else
    o = materialize(accumulate_stream2, output.begin(), output.end(), 0, deferred);
#endif

    assert(o == output.end());

    for (unsigned int w = 0; w < num_workers; ++w)
    {
        delete buckets[w];
//		delete workers[w];
//		delete worker_nodes[w];
    }

#if OUTPUT_STATS
    std::cout << stxxl::stats_data(*stxxl::stats::get_instance()) - stats_begin;
#endif

    std::cout << "Distributing/gathering done." << std::endl;

    if (acc1.result() != acc2.result())
    {
        STXXL_MSG("WRONG DATA");
        std::cout << acc1.result() << std::endl;
        std::cout << acc2.result() << " - " << acc1.result() << " = " << ((long long)acc2.result() - (long long)acc1.result()) << std::endl;
    }
    else
        std::cout << acc1.result() << std::endl;
}

int main()
{
    const int megabytes_to_process = 1024;
    const stxxl::int64 n_records =
        stxxl::int64(megabytes_to_process) * stxxl::int64(megabyte) / sizeof(my_type);
    vector_type input(n_records);

#if PIPELINED
    std::cout << "PIPELINED" << std::endl;
#endif
#if BATCHED
    std::cout << "BATCHED" << std::endl;
#endif

    int seed = 1000;

    STXXL_MSG("Filling vector..., input size =" << input.size());

    random_my_type rnd(seed);

    stxxl::stats_data stats_begin(*stxxl::stats::get_instance());

    stxxl::generate(input.begin(), input.end(), rnd, memory_to_use / STXXL_DEFAULT_BLOCK_SIZE(my_type));

#if OUTPUT_STATS
    std::cout << stxxl::stats_data(*stxxl::stats::get_instance()) - stats_begin;
#endif

    distribute_gather(input, true);

    return 0;
}
