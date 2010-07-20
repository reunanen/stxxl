/***************************************************************************
 *  include/stxxl/bits/stream/stream.h
 *
 *  Part of the STXXL. See http://stxxl.sourceforge.net
 *
 *  Copyright (C) 2009 Johannes Singler <singler@kit.edu>
 *
 *  Distributed under the Boost Software License, Version 1.0.
 *  (See accompanying file LICENSE_1_0.txt or copy at
 *  http://www.boost.org/LICENSE_1_0.txt)
 **************************************************************************/

#ifndef STXXL_STREAM__DISTRIBUTE_H_
#define STXXL_STREAM__DISTRIBUTE_H_

#include <stxxl/bits/namespace.h>


__STXXL_BEGIN_NAMESPACE

//! \brief Stream package subnamespace
namespace stream
{
    ////////////////////////////////////////////////////////////////////////
    //     DISTRIBUTE                                                     //
    ////////////////////////////////////////////////////////////////////////

    //! \brief Helper function to call basic_push::push() in a Pthread thread.
    template <class Output_>
    void * call_stop_push(void * param)
    {
        static_cast<Output_ *>(param)->stop_push();
        return NULL;
    }

    //! \brief Push data to different outputs, in a round-robin fashion.
    //!
    //! Template parameters:
    //! - \c Input_ type of the input
    template <class Output_>
    class basic_distribute
    {
    public:
        //! \brief Standard stream typedef
        typedef typename Output_::value_type value_type;

    protected:
        Output_ ** outputs;
        Output_ * current_output;
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
        //! \brief Construction
        basic_distribute(/*Input_ input, */ Output_ ** outputs, int num_outputs, StartMode start_mode) : /*input(input),*/ outputs(outputs), num_outputs(num_outputs), start_mode(start_mode)
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

        //! \brief Standard stream method
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

        //! \brief Standard stream method.
        void stop_push() const
        {
            STXXL_VERBOSE0("distribute " << this << " stops push.");

#ifdef STXXL_BOOST_THREADS
            boost::thread ** threads = new boost::thread *[num_outputs];
#else
            pthread_t * threads = new pthread_t[num_outputs];
#endif
            for (int i = 0; i < num_outputs; ++i)
#ifdef STXXL_BOOST_THREADS
                threads[i] = new boost::thread(boost::bind(call_stop_push<Output_>, outputs[i]));
#else
                check_pthread_call(pthread_create(&(threads[i]), NULL, call_stop_push<Output_>, outputs[i]));
#endif

            for (int i = 0; i < num_outputs; ++i)
#ifdef STXXL_BOOST_THREADS
            {
                threads[i]->join();
                delete threads[i];
            }
#else
            {
                void * return_code;
                check_pthread_call(pthread_join(threads[i], &return_code));
            }
#endif

            delete[] threads;
        }
    };

    //! \brief Push data to different outputs, in a round-robin fashion.
    //!
    //! Template parameters:
    //! - \c Input_ type of the input
    template <class Output_>
    class distribute : public basic_distribute<Output_>
    {
        typedef basic_distribute<Output_> base;
        typedef typename base::value_type value_type;
        using base::current_output;
        using base::next;

    public:
        distribute(Output_ ** outputs, int num_outputs) : base(outputs, num_outputs)
        { }

        //! \brief Standard stream method.
        void push(const value_type & val)
        {
            current_output->push(val);
            next();
        }

        //! \brief Batched stream method.
        unsigned_type push_batch_length() const
        {
            return current_output->push_batch_length();
        }

        //! \brief Batched stream method.
        void push_batch(const value_type * batch_begin, const value_type * batch_end)
        {
            current_output->push_batch(batch_begin, batch_end);
            next();
        }
    };

    //! \brief Push data to different outputs, in a round-robin fashion.
    //!
    //! Template parameters:
    //! - \c Input_ type of the input
    template <class Output_>
    class deterministic_distribute : public basic_distribute<Output_>
    {
        typedef basic_distribute<Output_> base;
        using base::current_output;
        using base::next;

        unsigned_type elements_per_chunk, elements_left;

    public:
        typedef typename base::value_type value_type;

        deterministic_distribute(Output_ ** outputs, int num_outputs, unsigned_type elements_per_chunk, StartMode start_mode = STXXL_START_PIPELINE_DEFERRED_DEFAULT) : base(outputs, num_outputs, start_mode)
        {
            this->elements_per_chunk = elements_per_chunk;
            elements_left = elements_per_chunk;
        }

        //! \brief Standard stream method.
        void push(const value_type & val)
        {
            current_output->push(val);
            --elements_left;
            if (elements_left == 0)
            {
                next();
                elements_left = elements_per_chunk;
            }
        }

        //! \brief Batched stream method.
        unsigned_type push_batch_length() const
        {
            return STXXL_MIN<unsigned_type>(elements_left, current_output->push_batch_length());
        }

        //! \brief Batched stream method.
        void push_batch(const value_type * batch_begin, const value_type * batch_end)
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
}

__STXXL_END_NAMESPACE


#endif // !STXXL_STREAM__DISTRIBUTE_H_
// vim: et:ts=4:sw=4
