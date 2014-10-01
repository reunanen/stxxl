/***************************************************************************
 *  include/stxxl/bits/stream/distribute.h
 *
 *  Part of the STXXL. See http://stxxl.sourceforge.net
 *
 *  Copyright (C) 2009 Johannes Singler <singler@kit.edu>
 *
 *  Distributed under the Boost Software License, Version 1.0.
 *  (See accompanying file LICENSE_1_0.txt or copy at
 *  http://www.boost.org/LICENSE_1_0.txt)
 **************************************************************************/

#ifndef STXXL_STREAM_DISTRIBUTE_HEADER
#define STXXL_STREAM_DISTRIBUTE_HEADER

#include <stxxl/bits/namespace.h>

STXXL_BEGIN_NAMESPACE

//! Stream package subnamespace
namespace stream {

////////////////////////////////////////////////////////////////////////
//     DISTRIBUTE                                                     //
////////////////////////////////////////////////////////////////////////

//! Helper function to call basic_push::push() in a Pthread thread.
template <class Output>
void * call_stop_push(void* param)
{
    static_cast<Output*>(param)->stop_push();
    return NULL;
}

//! Push data to different outputs, in a round-robin fashion.
//!
//! Template parameters:
//! - \c Input_ type of the input
template <class Output>
class basic_distribute
{
public:
    //! Standard stream typedef
    typedef typename Output::value_type value_type;

protected:
    Output** outputs;
    Output* current_output;
    int num_outputs;
    value_type current;
    int pos;
    int empty_count;
    StartMode start_mode;

    void next()
    {
        ++pos;
        if (pos == num_outputs)
            pos = 0;
        current_output = outputs[pos];
        //STXXL_VERBOSE0("distribute next " << pos);
    }

public:
    //! Construction
    basic_distribute(/*Input_ input, */ Output** outputs, int num_outputs,
                                        StartMode start_mode)
        : /*input(input),*/ outputs(outputs),
          num_outputs(num_outputs), start_mode(start_mode)
    {
        empty_count = 0;
        if (start_mode == start_deferred)
            pos = 0;
        else
        {
            pos = -1;
            next();
        }
    }

    //! Standard stream method
    void start_push()
    {
        STXXL_VERBOSE0("distribute " << this << " starts push.");
        for (int i = 0; i < num_outputs; ++i)
            outputs[i]->start_push();
        if (start_mode == start_deferred)
        {
            pos = -1;
            next();
        }
    }

    //! Standard stream method.
    void stop_push() const
    {
        STXXL_VERBOSE0("distribute " << this << " stops push.");

#ifdef STXXL_BOOST_THREADS
        boost::thread** threads = new boost::thread*[num_outputs];
#else
        pthread_t* threads = new pthread_t[num_outputs];
#endif
        for (int i = 0; i < num_outputs; ++i)
#ifdef STXXL_BOOST_THREADS
            threads[i] = new boost::thread(boost::bind(call_stop_push<Output>, outputs[i]));
#else
            STXXL_CHECK_PTHREAD_CALL(pthread_create(&(threads[i]), NULL, call_stop_push<Output>, outputs[i]));
#endif

        for (int i = 0; i < num_outputs; ++i)
#ifdef STXXL_BOOST_THREADS
        {
            threads[i]->join();
            delete threads[i];
        }
#else
        {
            void* return_code;
            STXXL_CHECK_PTHREAD_CALL(pthread_join(threads[i], &return_code));
        }
#endif

        delete[] threads;
    }
};

//! Push data to different outputs, in a round-robin fashion.
//!
//! Template parameters:
//! - \c Input_ type of the input
template <class Output>
class distribute : public basic_distribute<Output>
{
    typedef basic_distribute<Output> base;
    typedef typename base::value_type value_type;
    using base::current_output;
    using base::next;

public:
    distribute(Output** outputs, int num_outputs)
        : base(outputs, num_outputs)
    { }

    //! Standard stream method.
    void push(const value_type& val)
    {
        current_output->push(val);
        next();
    }

    //! Batched stream method.
    unsigned_type push_batch_length() const
    {
        return current_output->push_batch_length();
    }

    //! Batched stream method.
    void push_batch(const value_type* batch_begin, const value_type* batch_end)
    {
        current_output->push_batch(batch_begin, batch_end);
        next();
    }
};

//! Push data to different outputs, in a round-robin fashion.
//!
//! Template parameters:
//! - \c Input_ type of the input
template <class Output>
class deterministic_distribute : public basic_distribute<Output>
{
    typedef basic_distribute<Output> base;
    using base::current_output;
    using base::next;

    unsigned_type elements_per_chunk, elements_left;

public:
    typedef typename base::value_type value_type;

    deterministic_distribute(Output** outputs, int num_outputs,
                             unsigned_type elements_per_chunk,
                             StartMode start_mode = STXXL_START_PIPELINE_DEFERRED_DEFAULT)
        : base(outputs, num_outputs, start_mode)
    {
        this->elements_per_chunk = elements_per_chunk;
        elements_left = elements_per_chunk;
    }

    //! Standard stream method.
    void push(const value_type& val)
    {
        current_output->push(val);
        --elements_left;
        if (elements_left == 0)
        {
            next();
            elements_left = elements_per_chunk;
        }
    }

    //! Batched stream method.
    unsigned_type push_batch_length() const
    {
        return STXXL_MIN<unsigned_type>(elements_left, current_output->push_batch_length());
    }

    //! Batched stream method.
    void push_batch(const value_type* batch_begin, const value_type* batch_end)
    {
        current_output->push_batch(batch_begin, batch_end);
        elements_left -= (batch_end - batch_begin);
        if (elements_left == 0)
        {
            next();
            elements_left = elements_per_chunk;
        }
    }
};

//! \}

} // namespace stream

STXXL_END_NAMESPACE

#endif // !STXXL_STREAM_DISTRIBUTE_HEADER
// vim: et:ts=4:sw=4
