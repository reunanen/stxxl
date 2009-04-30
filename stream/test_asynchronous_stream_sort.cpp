/***************************************************************************
 *  stream/test_asynchronous_stream_sort.cpp
 *
 *  Part of the STXXL. See http://stxxl.sourceforge.net
 *
 *  Copyright (C) 2009 Johannes Singler <singler@ira.uka.de>
 *
 *  Distributed under the Boost Software License, Version 1.0.
 *  (See accompanying file LICENSE_1_0.txt or copy at
 *  http://www.boost.org/LICENSE_1_0.txt)
 **************************************************************************/

//! \example stream/test_asynchronous_stream_sort.cpp
//! This is an example of how to use the asynchronous stream sorter.

//#define STXXL_PARALLEL_MULTIWAY_MERGE 0

#define STXXL_START_PIPELINE 1
#define STXXL_STREAM_SORT_ASYNCHRONOUS_READ 1

#define PIPELINED 1
#define BATCHED 1
#define SYMMETRIC 1

#define OUTPUT_STATS 1

//#define STXXL_VERBOSE_LEVEL 0

#include "test_parallel_pipelining_common.h"
#include <vector>
#include <stxxl/stream>
#include <stxxl/vector>

stxxl::unsigned_type memory_to_use = 2048 * megabyte;
stxxl::unsigned_type run_size = memory_to_use / 4;
stxxl::unsigned_type buffer_size = 16 * megabyte;

stxxl::unsigned_type checksum(vector_type & input)
{
    stxxl::unsigned_type sum = 0;
    for (vector_type::const_iterator i = input.begin(); i != input.end(); ++i)
        sum += (*i)._key;
    return sum;
}

void linear_sort_streamed(vector_type & input, vector_type & output)
{
    using stxxl::stream::generator2stream;
    using stxxl::stream::round_robin;
    using stxxl::stream::streamify;
    using stxxl::stream::pipeline::pull;
    using stxxl::stream::pipeline::pull_batch;
    using stxxl::stream::pipeline::dummy_pull;
    using stxxl::stream::pipeline::push_stage;
    using stxxl::stream::pipeline::push_stage_batch;
    using stxxl::stream::pipeline::dummy_push_stage;
    using stxxl::stream::transform;
    using stxxl::stream::sort;
    using stxxl::stream::runs_creator;
    using stxxl::stream::runs_creator_batch;
    using stxxl::stream::runs_merger;
    using stxxl::stream::make_tuple;
    using stxxl::stream::use_push;

    stxxl::unsigned_type sum1 = checksum(input);

    stxxl::stats::get_instance()->reset();

#ifdef BOOST_MSVC
    typedef stxxl::stream::streamify_traits<vector_type::iterator>::stream_type input_stream_type;
#else
    typedef __typeof__(streamify(input.begin(), input.end())) input_stream_type;
#endif //BOOST_MSVC

    input_stream_type input_stream = streamify(input.begin(), input.end());


    typedef cmp_less_key comparator_type;
    comparator_type cl;

#if BATCHED
    typedef sort<input_stream_type, comparator_type, block_size, STXXL_DEFAULT_ALLOC_STRATEGY, runs_creator_batch<input_stream_type, comparator_type, block_size, STXXL_DEFAULT_ALLOC_STRATEGY> > sort_stream_type;
#else
    typedef sort<input_stream_type, comparator_type, block_size> sort_stream_type;
#endif

    sort_stream_type sort_stream(input_stream, cl, run_size);

#if BATCHED
    vector_type::iterator o = materialize_batch(sort_stream, output.begin(), output.end());
#else
    vector_type::iterator o = materialize(sort_stream, output.begin(), output.end());
#endif

#if OUTPUT_STATS
    std::cout << *(stxxl::stats::get_instance()) << std::endl;
#endif


    stxxl::unsigned_type sum2 = checksum(output);

    std::cout << sum1 << " ?= " << sum2 << std::endl;
    if (sum1 != sum2)
        STXXL_MSG("WRONG DATA");

    STXXL_MSG((stxxl::is_sorted<vector_type::const_iterator>(output.begin(), output.end(), comparator_type()) ? "OK" : "NOT SORTED"));

    std::cout << "Linear sorting streamed done." << std::endl;
}


int main()
{
    const int megabytes_to_process = 1024;
    const stxxl::int64 n_records =
        stxxl::int64(megabytes_to_process) * stxxl::int64(megabyte) / sizeof(my_type);
    vector_type input(n_records), output(n_records);

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

    linear_sort_streamed(input, output);
}
