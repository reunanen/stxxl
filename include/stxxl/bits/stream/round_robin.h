/***************************************************************************
 *  include/stxxl/bits/stream/round_robin.h
 *
 *  Part of the STXXL. See http://stxxl.sourceforge.net
 *
 *  Copyright (C) 2009 Johannes Singler <singler@kit.edu>
 *
 *  Distributed under the Boost Software License, Version 1.0.
 *  (See accompanying file LICENSE_1_0.txt or copy at
 *  http://www.boost.org/LICENSE_1_0.txt)
 **************************************************************************/

#ifndef STXXL_STREAM__ROUND_ROBIN_H_
#define STXXL_STREAM__ROUND_ROBIN_H_

#include <stxxl/bits/namespace.h>


__STXXL_BEGIN_NAMESPACE

//! \brief Stream package subnamespace
namespace stream
{
    ////////////////////////////////////////////////////////////////////////
    //     ROUND ROBIN                                                    //
    ////////////////////////////////////////////////////////////////////////

    //! \brief Fetch data from different inputs, in a round-robin fashion.
    //!
    //! Template parameters:
    //! - \c Input_ type of the input
    template <class Input_>
    class basic_round_robin
    {
    public:
        //! \brief Standard stream typedef
        typedef typename Input_::value_type value_type;
        typedef typename Input_::const_iterator const_iterator;

    protected:
        Input_ ** inputs;
        Input_ * current_input;
        bool * already_empty;
        int num_inputs;
        value_type current;
        int pos;
        int empty_count;
        StartMode start_mode;

        void next()
        {
            do
            {
                ++pos;
                if (pos == num_inputs)
                    pos = 0;
                current_input = inputs[pos];

                if (current_input->empty())
                {
                    if (!already_empty[pos])
                    {
                        already_empty[pos] = true;
                        ++empty_count;
                        if (empty_count >= num_inputs)
                            break;  //empty() == true
                    }
                }
                else
                    break;
            } while (true);
            //STXXL_VERBOSE0("next " << pos);
        }

    public:
        //! \brief Construction
        basic_round_robin(Input_ ** inputs, int num_inputs, StartMode start_mode) : inputs(inputs), num_inputs(num_inputs), start_mode(start_mode)
        {
            empty_count = 0;
            already_empty = new bool[num_inputs];
            for (int i = 0; i < num_inputs; ++i)
                already_empty[i] = false;

            if (start_mode == start_deferred)
                pos = 0;
            else
            {
                pos = -1;
                next();
            }
        }

        ~basic_round_robin()
        {
            delete[] already_empty;
        }

        //! \brief Standard stream method
        void start_pull()
        {
            STXXL_VERBOSE0("basic_round_robin " << this << " starts.");
            for (int i = 0; i < num_inputs; ++i)
                inputs[i]->start_pull();
            if (start_mode == start_deferred)
            {
                pos = -1;
                next();
            }
        }

        //! \brief Standard stream method
        bool empty() const
        {
            return empty_count >= num_inputs;
        }

        //! \brief Standard stream method
        const value_type & operator * () const
        {
            return *(*current_input);
        }

        const value_type * operator -> () const
        {
            return &(operator * ());
        }

        const_iterator batch_begin() const
        {
            return current_input->batch_begin();
        }

        const value_type & operator [] (unsigned_type index) const
        {
            return (*current_input)[index];
        }
    };

    //! \brief Fetch data from different inputs, in a round-robin fashion.
    //!
    //! Template parameters:
    //! - \c Input_ type of the input
    template <class Input_>
    class round_robin : public basic_round_robin<Input_>
    {
        typedef basic_round_robin<Input_> base;
        using base::current_input;
        using base::next;

    public:
        //! \brief Construction
        round_robin(Input_ ** inputs, int num_inputs, StartMode start_mode = STXXL_START_PIPELINE_DEFERRED_DEFAULT) : base(inputs, num_inputs, start_mode)
        { }

        unsigned_type batch_length() const
        {
            return current_input->batch_length();
        }

        //! \brief Standard stream method
        round_robin & operator ++ ()
        {
            ++(*current_input);
            next();

            return *this;
        }

        //! \brief Standard stream method
        round_robin & operator += (unsigned_type length)
        {
            assert(length > 0);

            (*current_input) += length;
            next();

            return *this;
        }
    };

    //! \brief Fetch data from different inputs, in a round-robin fashion.
    //!
    //! Template parameters:
    //! - \c Input_ type of the input
    template <class Input_>
    class deterministic_round_robin : public basic_round_robin<Input_>
    {
        typedef basic_round_robin<Input_> base;
        using base::current_input;
        using base::next;

        unsigned_type elements_per_chunk, elements_left;

    public:
        //! \brief Construction
        deterministic_round_robin(Input_ ** inputs, int num_inputs, unsigned_type elements_per_chunk, StartMode start_mode = STXXL_START_PIPELINE_DEFERRED_DEFAULT) : base(inputs, num_inputs, start_mode)
        {
            this->elements_per_chunk = elements_per_chunk;
            elements_left = elements_per_chunk;
        }

        //! \brief Standard stream method
        deterministic_round_robin & operator ++ ()
        {
            ++(*current_input);
            --elements_left;
            if (elements_left == 0 || current_input->empty())
            {
                next();
                elements_left = elements_per_chunk;
            }

            return *this;
        }

        unsigned_type batch_length() const
        {
            return STXXL_MIN<unsigned_type>(elements_left, current_input->batch_length());
        }

        //! \brief Standard stream method
        deterministic_round_robin & operator += (unsigned_type length)
        {
            assert(length > 0);

            (*current_input) += length;
            elements_left -= length;

            if (elements_left == 0 || current_input->batch_length() == 0)
            {
                next();
                elements_left = elements_per_chunk;
            }

            return *this;
        }
    };


//! \}
}

__STXXL_END_NAMESPACE


#endif // !STXXL_STREAM__ROUND_ROBIN_H_
// vim: et:ts=4:sw=4
