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

#define STXXL_START_PIPELINE 1

#define INTERMEDIATE 1

#define OUTPUT_STATS 1

//#define STXXL_VERBOSE_LEVEL 0

#include "test_parallel_pipelining_common.h"
#include <vector>
#include <stxxl/stream>
#include <stxxl/vector>

stxxl::unsigned_type memory_to_use = 2048 * megabyte;
stxxl::unsigned_type run_size = memory_to_use / 4;
stxxl::unsigned_type buffer_size = 16 * megabyte;

void distribute_gather(vector_type & input)
{
    using stxxl::stream::deterministic_round_robin;
    using stxxl::stream::deterministic_distribute;
    using stxxl::stream::streamify;
    using stxxl::stream::pipeline::pull_stage;
    using stxxl::stream::pipeline::pull_stage_batch;
    using stxxl::stream::pipeline::pull_empty_stage;
    using stxxl::stream::pipeline::pull_empty_stage_batch;
    using stxxl::stream::pipeline::dummy_pull_stage;
    using stxxl::stream::pipeline::push_stage;
    using stxxl::stream::pipeline::push_stage_batch;
    using stxxl::stream::pipeline::dummy_push_stage;
    using stxxl::stream::pipeline::push_pull_stage;
    using stxxl::stream::pipeline::connect_pull_stage;
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

    stxxl::stats::get_instance()->reset();

    vector_type output(input.size());
    accumulate<my_type> acc1;
    accumulate<my_type> acc2;
    vector_type::iterator o;

    input_stream_type input_stream = streamify(input.begin(), input.end());

    typedef transform<accumulate<my_type>, input_stream_type> accumulate_stream1_type;
    accumulate_stream1_type accumulate_stream1(acc1, input_stream);

    const unsigned int num_workers = 2;

    typedef push_pull_stage<my_type> bucket_type;

    bucket_type * buckets[num_workers];
#if INTERMEDIATE
    typedef transform<identity<my_type>, bucket_type> worker_stream_type;
    typedef pull_stage<worker_stream_type> worker_stream_stage_type;
    identity<my_type> id;
    worker_stream_type * workers[num_workers];
    worker_stream_stage_type * worker_stages[num_workers];
#else
    typedef bucket_type worker_stream_stage_type;
    worker_stream_stage_type ** worker_stages = buckets;
#endif


    for (unsigned int w = 0; w < num_workers; ++w)
    {
        buckets[w] = new bucket_type(buffer_size);
#if INTERMEDIATE
        workers[w] = new worker_stream_type(id, *buckets[w]);
        worker_stages[w] = new worker_stream_stage_type(buffer_size, *workers[w]);
#endif
    }

    const stxxl::unsigned_type chunk_size = buffer_size / sizeof(my_type) / (3 * num_workers);

    typedef deterministic_distribute<worker_stream_stage_type> distributor_stream_type;
    distributor_stream_type distributor_stream(worker_stages, num_workers, chunk_size);

    typedef pusher<accumulate_stream1_type, distributor_stream_type> pusher_stream_type;
    pusher_stream_type pusher_stream(accumulate_stream1, distributor_stream);

#if BATCHED
    typedef pull_empty_stage_batch<pusher_stream_type> pull_empty_type;
#else
    typedef pull_empty_stage<pusher_stream_type> pull_empty_type;
#endif
    pull_empty_type pull_empty(pusher_stream);

    typedef deterministic_round_robin<worker_stream_stage_type> fetcher_stream_type;
    fetcher_stream_type fetcher_stream(worker_stages, num_workers, chunk_size);

    typedef connect_pull_stage<fetcher_stream_type, pull_empty_type> connect_stream_type;
    connect_stream_type connect_stream(fetcher_stream, pull_empty);

    typedef transform<accumulate<my_type>, connect_stream_type> accumulate_stream_type2;
    accumulate_stream_type2 accumulate_stream2(acc2, connect_stream);

#if BATCHED
    o = materialize_batch(accumulate_stream2, output.begin(), output.end());
#else
    o = materialize(accumulate_stream2, output.begin(), output.end());
#endif

    assert(o == output.end());

    for (unsigned int w = 0; w < num_workers; ++w)
    {
        delete buckets[w];
//		delete workers[w];
//		delete worker_stages[w];
    }

#if OUTPUT_STATS
    std::cout << *(stxxl::stats::get_instance()) << std::endl;
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
#if SYMMETRIC
    std::cout << "SYMMETRIC" << std::endl;
#endif

    int seed = 1000;

    STXXL_MSG("Filling vector..., input size =" << input.size());

    random_my_type rnd(seed);

    stxxl::stats::get_instance()->reset();

    stxxl::generate(input.begin(), input.end(), rnd, memory_to_use / STXXL_DEFAULT_BLOCK_SIZE(my_type));

#if OUTPUT_STATS
    std::cout << *(stxxl::stats::get_instance()) << std::endl;
#endif

    distribute_gather(input);
}
