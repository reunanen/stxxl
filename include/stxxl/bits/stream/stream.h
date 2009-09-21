/***************************************************************************
 *  include/stxxl/bits/stream/stream.h
 *
 *  Part of the STXXL. See http://stxxl.sourceforge.net
 *
 *  Copyright (C) 2003-2005 Roman Dementiev <dementiev@mpi-sb.mpg.de>
 *
 *  Distributed under the Boost Software License, Version 1.0.
 *  (See accompanying file LICENSE_1_0.txt or copy at
 *  http://www.boost.org/LICENSE_1_0.txt)
 **************************************************************************/

#ifndef STXXL_STREAM_HEADER
#define STXXL_STREAM_HEADER

#ifndef STXXL_START_PIPELINE_DEFERRED
#define STXXL_START_PIPELINE_DEFERRED 0
#endif

#ifndef STXXL_STREAM_SORT_ASYNCHRONOUS_PULL
#define STXXL_STREAM_SORT_ASYNCHRONOUS_PULL 0
#endif


#ifdef STXXL_BOOST_THREADS // Use Portable Boost threads
 #include <boost/bind.hpp>
#endif

#include <stxxl/bits/namespace.h>
#include <stxxl/bits/mng/buf_istream.h>
#include <stxxl/bits/mng/buf_ostream.h>
#include <stxxl/bits/common/tuple.h>
#include <stxxl/vector>
#include <stxxl/bits/compat_unique_ptr.h>


#ifndef STXXL_STREAM_SORT_ASYNCHRONOUS_PULL_DEFAULT
#define STXXL_STREAM_SORT_ASYNCHRONOUS_PULL_DEFAULT false
#endif

#ifndef STXXL_WAIT_FOR_STOP_DEFAULT
#define STXXL_WAIT_FOR_STOP_DEFAULT false
#endif


#ifndef STXXL_VERBOSE_MATERIALIZE
#define STXXL_VERBOSE_MATERIALIZE STXXL_VERBOSE3
#endif


__STXXL_BEGIN_NAMESPACE

//! \brief Stream package subnamespace
namespace stream
{
    enum StartMode { start_deferred, start_immediately };

    #ifndef STXXL_START_PIPELINE_DEFERRED_DEFAULT
    #define STXXL_START_PIPELINE_DEFERRED_DEFAULT start_immediately
    #endif

    //! \weakgroup streampack Stream package
    //! Package that enables pipelining of consequent sorts
    //! and scans of the external data avoiding the saving the intermediate
    //! results on the disk, e.g. the output of a sort can be directly
    //! fed into a scan procedure without the need to save it on a disk.
    //! All components of the package are contained in the \c stxxl::stream
    //! namespace.
    //!
    //!    STREAM ALGORITHM CONCEPT (Do not confuse with C++ input/output streams)
    //!
    //! \verbatim
    //!
    //!    struct stream_algorithm // stream, pipe, whatever
    //!    {
    //!      typedef some_type value_type;
    //!
    //!      const value_type & operator * () const; // return current element of the stream
    //!      stream_algorithm & operator ++ ();      // go to next element. precondition: empty() == false
    //!      bool empty() const;                     // return true if end of stream is reached
    //!
    //!    };
    //! \endverbatim
    //!
    //! \{


    ////////////////////////////////////////////////////////////////////////
    //     STREAMIFY                                                      //
    ////////////////////////////////////////////////////////////////////////

    //! \brief A model of stream that retrieves the data from an input iterator
    //! For convenience use \c streamify function instead of direct instantiation
    //! of \c iterator2stream .
    template <class InputIterator_>
    class iterator2stream
    {
        InputIterator_ current_, end_;

    public:
        //! \brief Standard stream typedef
        typedef typename std::iterator_traits<InputIterator_>::value_type value_type;

        iterator2stream(InputIterator_ begin, InputIterator_ end) :
            current_(begin), end_(end) { }

        iterator2stream(const iterator2stream & a) : current_(a.current_), end_(a.end_) { }

        //! \brief Standard stream method
        void start_pull()
        {
            STXXL_VERBOSE0("iterator2stream " << this << " starts.");
            //do nothing
        }

        //! \brief Standard stream method
        const value_type & operator * () const
        {
            return *current_;
        }

        const value_type * operator -> () const
        {
            return &(*current_);
        }

        //! \brief Standard stream method
        iterator2stream & operator ++ ()
        {
            assert(end_ != current_);
            ++current_;
            return *this;
        }

        //! \brief Standard stream method
        bool empty() const
        {
            return (current_ == end_);
        }
    };


    //! \brief Input iterator range to stream converter
    //! \param begin iterator, pointing to the first value
    //! \param end iterator, pointing to the last + 1 position, i.e. beyond the range
    //! \return an instance of a stream object
    template <class InputIterator_>
    iterator2stream<InputIterator_> streamify(InputIterator_ begin, InputIterator_ end)
    {
        return iterator2stream<InputIterator_>(begin, end);
    }

    //! \brief Traits class of \c streamify function
    template <class InputIterator_>
    struct streamify_traits
    {
        //! \brief return type (stream type) of \c streamify for \c InputIterator_
        typedef iterator2stream<InputIterator_> stream_type;
    };

    //! \brief A model of stream that retrieves data from an external \c stxxl::vector iterator.
    //! It is more efficient than generic \c iterator2stream thanks to use of overlapping
    //! For convenience use \c streamify function instead of direct instantiation
    //! of \c vector_iterator2stream .
    template <class InputIterator_>
    class vector_iterator2stream
    {
        InputIterator_ current_, end_;
        typedef buf_istream<typename InputIterator_::block_type,
                            typename InputIterator_::bids_container_iterator> buf_istream_type;

        typedef typename stxxl::compat_unique_ptr<buf_istream_type>::result buf_istream_unique_ptr_type;
        mutable buf_istream_unique_ptr_type in;

        void delete_stream()
        {
            in.reset(); //delete object
        }

    public:
        typedef vector_iterator2stream<InputIterator_> Self_;

        //! \brief Standard stream typedef
        typedef typename std::iterator_traits<InputIterator_>::value_type value_type;
        typedef const value_type * const_iterator;

        vector_iterator2stream(InputIterator_ begin, InputIterator_ end, unsigned_type nbuffers = 0) :
            current_(begin), end_(end), in(NULL)
        {
            begin.flush();     // flush container
            typename InputIterator_::bids_container_iterator end_iter = end.bid() + ((end.block_offset()) ? 1 : 0);

            if (end_iter - begin.bid() > 0)
            {
                in.reset(new buf_istream_type(begin.bid(), end_iter, nbuffers ? nbuffers :
                                              (2 * config::get_instance()->disks_number())));

                InputIterator_ cur = begin - begin.block_offset();

                // skip the beginning of the block
                for ( ; cur != begin; ++cur)
                    ++(*in);
            }
        }

        vector_iterator2stream(const Self_ & a) : 
            current_(a.current_), end_(a.end_), in(a.in.release()) { }

        //! \brief Standard stream method
        void start_pull()
        {
            STXXL_VERBOSE0("vector_iterator2stream " << this << " starts.");
            //do nothing
        }

        //! \brief Standard stream method
        const value_type & operator * () const
        {
            return **in;
        }

        const value_type * operator -> () const
        {
            return &(**in);
        }

        //! \brief Standard stream method
        Self_ & operator ++ ()
        {
            assert(end_ != current_);
            ++current_;
            ++(*in);
            if (empty())
                delete_stream();

            return *this;
        }

        //! \brief Standard stream method
        bool empty() const
        {
            return (current_ == end_);
        }

        //! \brief Standard stream method
        vector_iterator2stream & operator += (unsigned_type length)
        {
            assert(0 < length && length <= batch_length());
            current_ += length;
            (*in) += length;
            return *this;
        }

        unsigned_type batch_length()
        {
            STXXL_VERBOSE(in->batch_length() << " " << end_ - current_);
            return STXXL_MIN<unsigned_type>(in->batch_length(), end_ - current_);
        }

        const_iterator batch_begin()
        {
            return in->batch_begin();
        }

        value_type & operator [] (unsigned_type index)
        {
            assert(current_ + index < end_);
            return (*in)[index];
        }

        virtual ~vector_iterator2stream()
        {
            delete_stream();      // not needed actually
        }
    };

    //! \brief Input external \c stxxl::vector iterator range to stream converter
    //! It is more efficient than generic input iterator \c streamify thanks to use of overlapping
    //! \param begin iterator, pointing to the first value
    //! \param end iterator, pointing to the last + 1 position, i.e. beyond the range
    //! \param nbuffers number of blocks used for overlapped reading (0 is default,
    //! which equals to (2 * number_of_disks)
    //! \return an instance of a stream object

    template <typename Tp_, typename AllocStr_, typename SzTp_, typename DiffTp_,
              unsigned BlkSize_, typename PgTp_, unsigned PgSz_>
    vector_iterator2stream<stxxl::vector_iterator<Tp_, AllocStr_, SzTp_, DiffTp_, BlkSize_, PgTp_, PgSz_> >
    streamify(
        stxxl::vector_iterator<Tp_, AllocStr_, SzTp_, DiffTp_, BlkSize_, PgTp_, PgSz_> begin,
        stxxl::vector_iterator<Tp_, AllocStr_, SzTp_, DiffTp_, BlkSize_, PgTp_, PgSz_> end,
        unsigned_type nbuffers = 0)
    {
        STXXL_VERBOSE1("streamify for vector_iterator range is called");
        return vector_iterator2stream<stxxl::vector_iterator<Tp_, AllocStr_, SzTp_, DiffTp_, BlkSize_, PgTp_, PgSz_> >
                   (begin, end, nbuffers);
    }

    template <typename Tp_, typename AllocStr_, typename SzTp_, typename DiffTp_,
              unsigned BlkSize_, typename PgTp_, unsigned PgSz_>
    struct streamify_traits<stxxl::vector_iterator<Tp_, AllocStr_, SzTp_, DiffTp_, BlkSize_, PgTp_, PgSz_> >
    {
        typedef vector_iterator2stream<stxxl::vector_iterator<Tp_, AllocStr_, SzTp_, DiffTp_, BlkSize_, PgTp_, PgSz_> > stream_type;
    };

    //! \brief Input external \c stxxl::vector const iterator range to stream converter
    //! It is more efficient than generic input iterator \c streamify thanks to use of overlapping
    //! \param begin const iterator, pointing to the first value
    //! \param end const iterator, pointing to the last + 1 position, i.e. beyond the range
    //! \param nbuffers number of blocks used for overlapped reading (0 is default,
    //! which equals to (2 * number_of_disks)
    //! \return an instance of a stream object

    template <typename Tp_, typename AllocStr_, typename SzTp_, typename DiffTp_,
              unsigned BlkSize_, typename PgTp_, unsigned PgSz_>
    vector_iterator2stream<stxxl::const_vector_iterator<Tp_, AllocStr_, SzTp_, DiffTp_, BlkSize_, PgTp_, PgSz_> >
    streamify(
        stxxl::const_vector_iterator<Tp_, AllocStr_, SzTp_, DiffTp_, BlkSize_, PgTp_, PgSz_> begin,
        stxxl::const_vector_iterator<Tp_, AllocStr_, SzTp_, DiffTp_, BlkSize_, PgTp_, PgSz_> end,
        unsigned_type nbuffers = 0)
    {
        STXXL_VERBOSE1("streamify for const_vector_iterator range is called");
        return vector_iterator2stream<stxxl::const_vector_iterator<Tp_, AllocStr_, SzTp_, DiffTp_, BlkSize_, PgTp_, PgSz_> >
                   (begin, end, nbuffers);
    }

    template <typename Tp_, typename AllocStr_, typename SzTp_, typename DiffTp_,
              unsigned BlkSize_, typename PgTp_, unsigned PgSz_>
    struct streamify_traits<stxxl::const_vector_iterator<Tp_, AllocStr_, SzTp_, DiffTp_, BlkSize_, PgTp_, PgSz_> >
    {
        typedef vector_iterator2stream<stxxl::const_vector_iterator<Tp_, AllocStr_, SzTp_, DiffTp_, BlkSize_, PgTp_, PgSz_> > stream_type;
    };


    //! \brief Version of  \c iterator2stream. Switches between \c vector_iterator2stream and \c iterator2stream .
    //!
    //! small range switches between
    //! \c vector_iterator2stream and \c iterator2stream .
    //! iterator2stream is chosen if the input iterator range
    //! is small ( < B )
    template <class InputIterator_>
    class vector_iterator2stream_sr
    {
        vector_iterator2stream<InputIterator_> * vec_it_stream;
        iterator2stream<InputIterator_> * it_stream;

        typedef typename InputIterator_::block_type block_type;

    public:
        typedef vector_iterator2stream_sr<InputIterator_> Self_;

        //! \brief Standard stream typedef
        typedef typename std::iterator_traits<InputIterator_>::value_type value_type;

        vector_iterator2stream_sr(InputIterator_ begin, InputIterator_ end, unsigned_type nbuffers = 0)
        {
            if (end - begin < block_type::size)
            {
                STXXL_VERBOSE1("vector_iterator2stream_sr::vector_iterator2stream_sr: Choosing iterator2stream<InputIterator_>");
                it_stream = new iterator2stream<InputIterator_>(begin, end);
                vec_it_stream = NULL;
            }
            else
            {
                STXXL_VERBOSE1("vector_iterator2stream_sr::vector_iterator2stream_sr: Choosing vector_iterator2stream<InputIterator_>");
                it_stream = NULL;
                vec_it_stream = new vector_iterator2stream<InputIterator_>(begin, end, nbuffers);
            }
        }

        vector_iterator2stream_sr(const Self_ & a) : vec_it_stream(a.vec_it_stream), it_stream(a.it_stream) { }

        //! \brief Standard stream method
        void start_pull()
        {
            STXXL_VERBOSE0("vector_iterator2stream_sr " << this << " starts.");
            //do nothing
        }

        //! \brief Standard stream method
        const value_type & operator * () const
        {
            if (it_stream)      //PERFORMANCE: Frequent run-time decision
                return **it_stream;

            return **vec_it_stream;
        }

        const value_type * operator -> () const
        {
            if (it_stream)      //PERFORMANCE: Frequent run-time decision
                return &(**it_stream);

            return &(**vec_it_stream);
        }

        //! \brief Standard stream method
        Self_ & operator ++ ()
        {
            if (it_stream)      //PERFORMANCE: Frequent run-time decision
                ++(*it_stream);

            else
                ++(*vec_it_stream);


            return *this;
        }

        //! \brief Standard stream method
        bool empty() const
        {
            if (it_stream)      //PERFORMANCE: Frequent run-time decision
                return it_stream->empty();

            return vec_it_stream->empty();
        }
        virtual ~vector_iterator2stream_sr()
        {
            if (it_stream)      //PERFORMANCE: Frequent run-time decision
                delete it_stream;

            else
                delete vec_it_stream;
        }
    };

    //! \brief Version of  \c streamify. Switches from \c vector_iterator2stream to \c iterator2stream for small ranges.
    template <typename Tp_, typename AllocStr_, typename SzTp_, typename DiffTp_,
              unsigned BlkSize_, typename PgTp_, unsigned PgSz_>
    vector_iterator2stream_sr<stxxl::vector_iterator<Tp_, AllocStr_, SzTp_, DiffTp_, BlkSize_, PgTp_, PgSz_> >
    streamify_sr(
        stxxl::vector_iterator<Tp_, AllocStr_, SzTp_, DiffTp_, BlkSize_, PgTp_, PgSz_> begin,
        stxxl::vector_iterator<Tp_, AllocStr_, SzTp_, DiffTp_, BlkSize_, PgTp_, PgSz_> end,
        unsigned_type nbuffers = 0)
    {
        STXXL_VERBOSE1("streamify_sr for vector_iterator range is called");
        return vector_iterator2stream_sr<stxxl::vector_iterator<Tp_, AllocStr_, SzTp_, DiffTp_, BlkSize_, PgTp_, PgSz_> >
                   (begin, end, nbuffers);
    }

    //! \brief Version of  \c streamify. Switches from \c vector_iterator2stream to \c iterator2stream for small ranges.
    template <typename Tp_, typename AllocStr_, typename SzTp_, typename DiffTp_,
              unsigned BlkSize_, typename PgTp_, unsigned PgSz_>
    vector_iterator2stream_sr<stxxl::const_vector_iterator<Tp_, AllocStr_, SzTp_, DiffTp_, BlkSize_, PgTp_, PgSz_> >
    streamify_sr(
        stxxl::const_vector_iterator<Tp_, AllocStr_, SzTp_, DiffTp_, BlkSize_, PgTp_, PgSz_> begin,
        stxxl::const_vector_iterator<Tp_, AllocStr_, SzTp_, DiffTp_, BlkSize_, PgTp_, PgSz_> end,
        unsigned_type nbuffers = 0)
    {
        STXXL_VERBOSE1("streamify_sr for const_vector_iterator range is called");
        return vector_iterator2stream_sr<stxxl::const_vector_iterator<Tp_, AllocStr_, SzTp_, DiffTp_, BlkSize_, PgTp_, PgSz_> >
                   (begin, end, nbuffers);
    }


    ////////////////////////////////////////////////////////////////////////
    //     MATERIALIZE                                                    //
    ////////////////////////////////////////////////////////////////////////

    //! \brief Stores consecutively stream content to an output iterator
    //! \param in stream to be stored used as source
    //! \param out output iterator used as destination
    //! \param start_mode start node immediately or deferred
    //! \return value of the output iterator after all increments,
    //! i.e. points to the first unwritten value
    //! \pre Output (range) is large enough to hold the all elements in the input stream
    template <class OutputIterator_, class StreamAlgorithm_>
    OutputIterator_ materialize(StreamAlgorithm_ & in, OutputIterator_ out, StartMode start_mode)
    {
        STXXL_VERBOSE_MATERIALIZE(STXXL_PRETTY_FUNCTION_NAME);
#if STXXL_START_PIPELINE_DEFERRED
        if (start_mode == start_deferred)
            in.start_pull();
#else
        UNUSED(start_mode);
#endif
        while (!in.empty())
        {
            *out = *in;
            ++out;
            ++in;
        }
        return out;
    }


    //! \brief Stores consecutively stream content to an output iterator
    //! \param in stream to be stored used as source
    //! \param out output iterator used as destination
    //! \return value of the output iterator after all increments,
    //! i.e. points to the first unwritten value
    //! \pre Output (range) is large enough to hold the all elements in the input stream
    template <class OutputIterator_, class StreamAlgorithm_>
    OutputIterator_ materialize(StreamAlgorithm_ & in, OutputIterator_ out)
    {
    	return materialize(in, out, STXXL_START_PIPELINE_DEFERRED_DEFAULT);
    }


    //! \brief Stores consecutively stream content to an output iterator
    //! \param in stream to be stored used as source
    //! \param out output iterator used as destination
    //! \param start_mode start node immediately or deferred
    //! \return value of the output iterator after all increments,
    //! i.e. points to the first unwritten value
    //! \pre Output (range) is large enough to hold the all elements in the input stream
    template <class OutputIterator_, class StreamAlgorithm_>
    OutputIterator_ materialize_batch(StreamAlgorithm_ & in, OutputIterator_ out, StartMode start_mode)
    {
        STXXL_VERBOSE_MATERIALIZE(STXXL_PRETTY_FUNCTION_NAME);
#if STXXL_START_PIPELINE_DEFERRED
        if (start_mode == start_deferred)
        {
            STXXL_VERBOSE0("materialize_batch starts.");
            in.start_pull();
        }
#else
        UNUSED(start_mode);
#endif
        unsigned_type length;
        while ((length = in.batch_length()) > 0)
        {
            out = std::copy(in.batch_begin(), in.batch_begin() + length, out);
            in += length;
        }
        return out;
    }

    //! \brief Stores consecutively stream content to an output iterator
    //! \param in stream to be stored used as source
    //! \param out output iterator used as destination
    //! \return value of the output iterator after all increments,
    //! i.e. points to the first unwritten value
    //! \pre Output (range) is large enough to hold the all elements in the input stream
    template <class OutputIterator_, class StreamAlgorithm_>
    OutputIterator_ materialize_batch(StreamAlgorithm_ & in, OutputIterator_ out)
    {
    	return materialize_batch(in, out, STXXL_START_PIPELINE_DEFERRED_DEFAULT);
    }

    //! \brief Stores consecutively stream content to an output iterator range \b until end of the stream or end of the iterator range is reached
    //! \param in stream to be stored used as source
    //! \param outbegin output iterator used as destination
    //! \param outend output end iterator, pointing beyond the output range
    //! \param start_mode start node immediately or deferred
    //! \return value of the output iterator after all increments,
    //! i.e. points to the first unwritten value
    //! \pre Output range is large enough to hold the all elements in the input stream
    //!
    //! This function is useful when you do not know the length of the stream beforehand.
    template <class OutputIterator_, class StreamAlgorithm_>
    OutputIterator_ materialize(StreamAlgorithm_ & in, OutputIterator_ outbegin, OutputIterator_ outend, StartMode start_mode)
    {
        STXXL_VERBOSE_MATERIALIZE(STXXL_PRETTY_FUNCTION_NAME);
#if STXXL_START_PIPELINE_DEFERRED
        if (start_mode == start_deferred)
            in.start_pull();
#else
        UNUSED(start_mode);
#endif
        while ((!in.empty()) && outend != outbegin)
        {
            *outbegin = *in;
            ++outbegin;
            ++in;
        }
        return outbegin;
    }

    //! \brief Stores consecutively stream content to an output iterator range \b until end of the stream or end of the iterator range is reached
    //! \param in stream to be stored used as source
    //! \param outbegin output iterator used as destination
    //! \param outend output end iterator, pointing beyond the output range
    //! \return value of the output iterator after all increments,
    //! i.e. points to the first unwritten value
    //! \pre Output range is large enough to hold the all elements in the input stream
    //!
    //! This function is useful when you do not know the length of the stream beforehand.
    template <class OutputIterator_, class StreamAlgorithm_>
    OutputIterator_ materialize(StreamAlgorithm_ & in, OutputIterator_ outbegin, OutputIterator_ outend)
    {
    	return materialize(in, outbegin, outend, STXXL_START_PIPELINE_DEFERRED_DEFAULT);
    }


    //! \brief Stores consecutively stream content to an output iterator range \b until end of the stream or end of the iterator range is reached
    //! \param in stream to be stored used as source
    //! \param outbegin output iterator used as destination
    //! \param outend output end iterator, pointing beyond the output range
    //! \param start_mode start node immediately or deferred
    //! \return value of the output iterator after all increments,
    //! i.e. points to the first unwritten value
    //! \pre Output range is large enough to hold the all elements in the input stream
    //!
    //! This function is useful when you do not know the length of the stream beforehand.
    template <class OutputIterator_, class StreamAlgorithm_>
    OutputIterator_ materialize_batch(StreamAlgorithm_ & in, OutputIterator_ outbegin, OutputIterator_ outend, StartMode start_mode)
    {
        STXXL_VERBOSE_MATERIALIZE(STXXL_PRETTY_FUNCTION_NAME);
#if STXXL_START_PIPELINE_DEFERRED
        if (start_mode == start_deferred)
        {
            STXXL_VERBOSE0("materialize_batch starts.");
            in.start_pull();
        }
#else
        UNUSED(start_mode);
#endif
        unsigned_type length;
        while ((length = in.batch_length()) > 0 && outbegin != outend)
        {
            length = STXXL_MIN<unsigned_type>(length, outend - outbegin);
            outbegin = std::copy(in.batch_begin(), in.batch_begin() + length, outbegin);
            in += length;
        }
        return outbegin;
    }

    //! \brief Stores consecutively stream content to an output iterator range \b until end of the stream or end of the iterator range is reached
    //! \param in stream to be stored used as source
    //! \param outbegin output iterator used as destination
    //! \param outend output end iterator, pointing beyond the output range
    //! \return value of the output iterator after all increments,
    //! i.e. points to the first unwritten value
    //! \pre Output range is large enough to hold the all elements in the input stream
    //!
    //! This function is useful when you do not know the length of the stream beforehand.
    template <class OutputIterator_, class StreamAlgorithm_>
    OutputIterator_ materialize_batch(StreamAlgorithm_ & in, OutputIterator_ outbegin, OutputIterator_ outend)
    {
    	return materialize_batch(in, outbegin, outend, STXXL_START_PIPELINE_DEFERRED_DEFAULT);
    }

    //! \brief Stores consecutively stream content to an output \c stxxl::vector iterator \b until end of the stream or end of the iterator range is reached
    //! \param in stream to be stored used as source
    //! \param outbegin output \c stxxl::vector iterator used as destination
    //! \param outend output end iterator, pointing beyond the output range
    //! \param nbuffers number of blocks used for overlapped writing (0 is default,
    //! \param start_mode start node immediately or deferred
    //! which equals to (2 * number_of_disks)
    //! \return value of the output iterator after all increments,
    //! i.e. points to the first unwritten value
    //! \pre Output range is large enough to hold the all elements in the input stream
    //!
    //! This function is useful when you do not know the length of the stream beforehand.
    template <typename Tp_, typename AllocStr_, typename SzTp_, typename DiffTp_,
              unsigned BlkSize_, typename PgTp_, unsigned PgSz_, class StreamAlgorithm_>
    stxxl::vector_iterator<Tp_, AllocStr_, SzTp_, DiffTp_, BlkSize_, PgTp_, PgSz_>
    materialize(StreamAlgorithm_ & in,
                stxxl::vector_iterator<Tp_, AllocStr_, SzTp_, DiffTp_, BlkSize_, PgTp_, PgSz_> outbegin,
                stxxl::vector_iterator<Tp_, AllocStr_, SzTp_, DiffTp_, BlkSize_, PgTp_, PgSz_> outend,
                unsigned_type nbuffers = 0,
                StartMode start_mode = STXXL_START_PIPELINE_DEFERRED_DEFAULT)
    {
        STXXL_VERBOSE_MATERIALIZE(STXXL_PRETTY_FUNCTION_NAME);
        typedef stxxl::vector_iterator<Tp_, AllocStr_, SzTp_, DiffTp_, BlkSize_, PgTp_, PgSz_> ExtIterator;
        typedef stxxl::const_vector_iterator<Tp_, AllocStr_, SzTp_, DiffTp_, BlkSize_, PgTp_, PgSz_> ConstExtIterator;
        typedef buf_ostream<typename ExtIterator::block_type, typename ExtIterator::bids_container_iterator> buf_ostream_type;

#if STXXL_START_PIPELINE_DEFERRED
        if (start_mode == start_deferred)
            in.start_pull();
#else
        UNUSED(start_mode);
#endif
        while (outbegin.block_offset()) //  go to the beginning of the block
        //  of the external vector
        {
            if (in.empty() || outbegin == outend)
                return outbegin;

            *outbegin = *in;
            ++outbegin;
            ++in;
        }

        if (nbuffers == 0)
            nbuffers = 2 * config::get_instance()->disks_number();


        outbegin.flush(); // flush container

        // create buffered write stream for blocks
        buf_ostream_type outstream(outbegin.bid(), nbuffers);

        assert(outbegin.block_offset() == 0);

        while (!in.empty() && outend != outbegin)
        {
            if (outbegin.block_offset() == 0)
                outbegin.block_externally_updated();

            *outstream = *in;
            ++outbegin;
            ++outstream;
            ++in;
        }

        ConstExtIterator const_out = outbegin;

        while (const_out.block_offset()) // filling the rest of the block
        {
            *outstream = *const_out;
            ++const_out;
            ++outstream;
        }
        outbegin.flush();

        return outbegin;
    }


    //! \brief Stores consecutively stream content to an output \c stxxl::vector iterator \b until end of the stream or end of the iterator range is reached
    //! \param in stream to be stored used as source
    //! \param outbegin output \c stxxl::vector iterator used as destination
    //! \param outend output end iterator, pointing beyond the output range
    //! \param nbuffers number of blocks used for overlapped writing (0 is default,
    //! \param start_mode start node immediately or deferred
    //! which equals to (2 * number_of_disks)
    //! \return value of the output iterator after all increments,
    //! i.e. points to the first unwritten value
    //! \pre Output range is large enough to hold the all elements in the input stream
    //!
    //! This function is useful when you do not know the length of the stream beforehand.
    template <typename Tp_, typename AllocStr_, typename SzTp_, typename DiffTp_,
              unsigned BlkSize_, typename PgTp_, unsigned PgSz_, class StreamAlgorithm_>
    stxxl::vector_iterator<Tp_, AllocStr_, SzTp_, DiffTp_, BlkSize_, PgTp_, PgSz_>
    materialize_batch(StreamAlgorithm_ & in,
                      stxxl::vector_iterator<Tp_, AllocStr_, SzTp_, DiffTp_, BlkSize_, PgTp_, PgSz_> outbegin,
                      stxxl::vector_iterator<Tp_, AllocStr_, SzTp_, DiffTp_, BlkSize_, PgTp_, PgSz_> outend,
                      unsigned_type nbuffers = 0,
                      StartMode start_mode = STXXL_START_PIPELINE_DEFERRED_DEFAULT)
    {
        STXXL_VERBOSE_MATERIALIZE(STXXL_PRETTY_FUNCTION_NAME);
        typedef stxxl::vector_iterator<Tp_, AllocStr_, SzTp_, DiffTp_, BlkSize_, PgTp_, PgSz_> ExtIterator;
        typedef stxxl::const_vector_iterator<Tp_, AllocStr_, SzTp_, DiffTp_, BlkSize_, PgTp_, PgSz_> ConstExtIterator;
        typedef buf_ostream<typename ExtIterator::block_type, typename ExtIterator::bids_container_iterator> buf_ostream_type;

#if STXXL_START_PIPELINE_DEFERRED
        if (start_mode == start_deferred)
        {
            STXXL_VERBOSE0("materialize_batch starts.");
            in.start_pull();
        }
#else
        UNUSED(start_mode);
#endif

        ExtIterator outcurrent = outbegin;

        while (outcurrent.block_offset()) //  go to the beginning of the block
        //  of the external vector
        {
            if (in.empty() || outcurrent == outend)
                return outcurrent;

            *outcurrent = *in;
            ++outcurrent;
            ++in;
        }

        if (nbuffers == 0)
            nbuffers = 2 * config::get_instance()->disks_number();


        outcurrent.flush(); // flush container

        // create buffered write stream for blocks
        buf_ostream_type outstream(outcurrent.bid(), nbuffers);

        assert(outcurrent.block_offset() == 0);

        unsigned_type length;
        while ((length = in.batch_length()) > 0 && outend != outcurrent)
        {
            if (outcurrent.block_offset() == 0)
                outcurrent.block_externally_updated();

            length = STXXL_MIN<unsigned_type>(length, STXXL_MIN<unsigned_type>(outend - outcurrent, ExtIterator::block_type::size - outcurrent.block_offset()));

            for (typename StreamAlgorithm_::const_iterator i = in.batch_begin(), end = in.batch_begin() + length; i != end; ++i)
            {
                *outstream = *i;
                ++outstream;
            }
            outcurrent += length;
            in += length;
            //STXXL_VERBOSE0("materialized " << (outcurrent - outbegin));
        }

        ConstExtIterator const_out = outcurrent;

        while (const_out.block_offset()) // filling the rest of the block
        {
            *outstream = *const_out;
            ++const_out;
            ++outstream;
        }
        outcurrent.flush();

        return outcurrent;
    }


    //! \brief Stores consecutively stream content to an output \c stxxl::vector iterator
    //! \param in stream to be stored used as source
    //! \param out output \c stxxl::vector iterator used as destination
    //! \param nbuffers number of blocks used for overlapped writing (0 is default,
    //! \param start_mode start node immediately or deferred
    //! which equals to (2 * number_of_disks)
    //! \return value of the output iterator after all increments,
    //! i.e. points to the first unwritten value
    //! \pre Output (range) is large enough to hold the all elements in the input stream
    template <typename Tp_, typename AllocStr_, typename SzTp_, typename DiffTp_,
              unsigned BlkSize_, typename PgTp_, unsigned PgSz_, class StreamAlgorithm_>
    stxxl::vector_iterator<Tp_, AllocStr_, SzTp_, DiffTp_, BlkSize_, PgTp_, PgSz_>
    materialize(StreamAlgorithm_ & in,
                stxxl::vector_iterator<Tp_, AllocStr_, SzTp_, DiffTp_, BlkSize_, PgTp_, PgSz_> out,
                unsigned_type nbuffers = 0,
                StartMode start_mode = STXXL_START_PIPELINE_DEFERRED_DEFAULT)
    {
        STXXL_VERBOSE_MATERIALIZE(STXXL_PRETTY_FUNCTION_NAME);
        typedef stxxl::vector_iterator<Tp_, AllocStr_, SzTp_, DiffTp_, BlkSize_, PgTp_, PgSz_> ExtIterator;
        typedef stxxl::const_vector_iterator<Tp_, AllocStr_, SzTp_, DiffTp_, BlkSize_, PgTp_, PgSz_> ConstExtIterator;
        typedef buf_ostream<typename ExtIterator::block_type, typename ExtIterator::bids_container_iterator> buf_ostream_type;

        // on the I/O complexity of "materialize":
        // crossing block boundary causes O(1) I/Os
        // if you stay in a block, then materialize function accesses only the cache of the
        // vector (only one block indeed), amortized complexity should apply here

#if STXXL_START_PIPELINE_DEFERRED
        if (start_mode == start_deferred)
            in.start_pull();
#else
        UNUSED(start_mode);
#endif
        while (out.block_offset()) //  go to the beginning of the block
        //  of the external vector
        {
            if (in.empty())
                return out;

            *out = *in;
            ++out;
            ++in;
        }

        if (nbuffers == 0)
            nbuffers = 2 * config::get_instance()->disks_number();


        out.flush(); // flush container

        // create buffered write stream for blocks
        buf_ostream_type outstream(out.bid(), nbuffers);

        assert(out.block_offset() == 0);

        while (!in.empty())
        {
            if (out.block_offset() == 0)
                out.block_externally_updated();
            // tells the vector that the block was modified
            *outstream = *in;
            ++out;
            ++outstream;
            ++in;
        }

        ConstExtIterator const_out = out;

        while (const_out.block_offset())
        {
            *outstream = *const_out;             // might cause I/Os for loading the page that
            ++const_out;                         // contains data beyond out
            ++outstream;
        }
        out.flush();

        return out;
    }


    //! \brief Stores consecutively stream content to an output \c stxxl::vector iterator
    //! \param in stream to be stored used as source
    //! \param out output \c stxxl::vector iterator used as destination
    //! \param nbuffers number of blocks used for overlapped writing (0 is default,
    //! \param start_mode start node immediately or deferred
    //! which equals to (2 * number_of_disks)
    //! \return value of the output iterator after all increments,
    //! i.e. points to the first unwritten value
    //! \pre Output (range) is large enough to hold the all elements in the input stream
    template <typename Tp_, typename AllocStr_, typename SzTp_, typename DiffTp_,
              unsigned BlkSize_, typename PgTp_, unsigned PgSz_, class StreamAlgorithm_>
    stxxl::vector_iterator<Tp_, AllocStr_, SzTp_, DiffTp_, BlkSize_, PgTp_, PgSz_>
    materialize_batch(StreamAlgorithm_ & in,
                      stxxl::vector_iterator<Tp_, AllocStr_, SzTp_, DiffTp_, BlkSize_, PgTp_, PgSz_> out,
                      unsigned_type nbuffers = 0,
                      StartMode start_mode = STXXL_START_PIPELINE_DEFERRED_DEFAULT)
    {
        STXXL_VERBOSE_MATERIALIZE(STXXL_PRETTY_FUNCTION_NAME);
        typedef stxxl::vector_iterator<Tp_, AllocStr_, SzTp_, DiffTp_, BlkSize_, PgTp_, PgSz_> ExtIterator;
        typedef stxxl::const_vector_iterator<Tp_, AllocStr_, SzTp_, DiffTp_, BlkSize_, PgTp_, PgSz_> ConstExtIterator;
        typedef buf_ostream<typename ExtIterator::block_type, typename ExtIterator::bids_container_iterator> buf_ostream_type;

        // on the I/O complexity of "materialize":
        // crossing block boundary causes O(1) I/Os
        // if you stay in a block, then materialize function accesses only the cache of the
        // vector (only one block indeed), amortized complexity should apply here

#if STXXL_START_PIPELINE_DEFERRED
        if (start_mode == start_deferred)
        {
            STXXL_VERBOSE0("materialize_batch starts.");
            in.start_pull();
        }
#else
        UNUSED(start_mode);
#endif
        while (out.block_offset()) //  go to the beginning of the block
        //  of the external vector
        {
            if (in.empty())
                return out;

            *out = *in;
            ++out;
            ++in;
        }

        if (nbuffers == 0)
            nbuffers = 2 * config::get_instance()->disks_number();


        out.flush(); // flush container

        // create buffered write stream for blocks
        buf_ostream_type outstream(out.bid(), nbuffers);

        assert(out.block_offset() == 0);

        unsigned_type length;
        while ((length = in.batch_length()) > 0)
        {
            if (out.block_offset() == 0)
                out.block_externally_updated();
            length = STXXL_MIN<unsigned_type>(length, ExtIterator::block_type::size - out.block_offset());
            for (typename StreamAlgorithm_::const_iterator i = in.batch_begin(), end = in.batch_begin() + length; i != end; ++i)
            {
                *outstream = *i;
                ++outstream;
            }
            in += length;
            out += length;
        }

        while (!in.empty())
        {
            if (out.block_offset() == 0)
                out.block_externally_updated();
            // tells the vector that the block was modified
            *outstream = *in;
            ++out;
            ++outstream;
            ++in;
        }

        ConstExtIterator const_out = out;

        while (const_out.block_offset())
        {
            *outstream = *const_out;             // might cause I/Os for loading the page that
            ++const_out;                         // contains data beyond out
            ++outstream;
        }
        out.flush();

        return out;
    }


    ////////////////////////////////////////////////////////////////////////
    //     PULL                                                           //
    //     FIXME: this is not a good, self-explanatory name
    //     FIXME: why does it need to call operator * () ?
    //            add a transform step before the pull if you need
    //            to do "something" with the steam elements
    ////////////////////////////////////////////////////////////////////////

    //! \brief Pulls from a stream, discards output
    //! \param in stream to be stored used as source
    //! \param num_elements number of elements to pull
    template <class StreamAlgorithm_>
    unsigned_type pull(StreamAlgorithm_ & in, unsigned_type num_elements)
    {
        unsigned_type i;
        for (i = 0; i < num_elements && !in.empty(); ++i)
        {
            *in;
            ++in;
        }
        return i;
    }


    //! \brief Pulls from a stream, discards output
    //! \param in stream to be stored used as source
    //! \param num_elements number of elements to pull
    template <class StreamAlgorithm_>
    unsigned_type pull_batch(StreamAlgorithm_ & in, unsigned_type num_elements)
    {
        unsigned_type i, length;
        typename StreamAlgorithm_::value_type dummy;
        for (i = 0; i < num_elements && ((length = in.batch_length()) > 0); )
        {
            length = STXXL_MIN<unsigned_type>(length, num_elements - i);
            for (typename StreamAlgorithm_::const_iterator j = in.batch_begin(); j != in.batch_begin() + length; ++j)
                dummy = *j;
            in += length;
            i += length;
        }
        return i;
    }


    ////////////////////////////////////////////////////////////////////////
    //     GENERATE                                                       //
    ////////////////////////////////////////////////////////////////////////

    //! \brief A model of stream that outputs data from an adaptable generator functor
    //! For convenience use \c streamify function instead of direct instantiation
    //! of \c generator2stream .
    template <class Generator_, typename T = typename Generator_::value_type>
    class generator2stream
    {
    public:
        //! \brief Standard stream typedef
        typedef T value_type;
        typedef generator2stream & iterator;
        typedef const generator2stream & const_iterator;

    private:
        Generator_ gen_;
        value_type current_;

    public:
        generator2stream(Generator_ g) :
            gen_(g), current_(gen_()) { }

        generator2stream(const generator2stream & a) : gen_(a.gen_), current_(a.current_) { }

        //! \brief Standard stream method
        void start_pull()
        {
            STXXL_VERBOSE0("generator2stream " << this << " starts.");
            //do nothing
        }

        //! \brief Standard stream method
        const value_type & operator * () const
        {
            return current_;
        }

        const value_type * operator -> () const
        {
            return &current_;
        }

        //! \brief Standard stream method
        generator2stream & operator ++ ()
        {
            current_ = gen_();
            return *this;
        }

        //! \brief Standard stream method
        bool empty() const
        {
            return false;
        }
    };

    //! \brief Adaptable generator to stream converter
    //! \param gen_ generator object
    //! \return an instance of a stream object
    template <class Generator_>
    generator2stream<Generator_> streamify(Generator_ gen_)
    {
        return generator2stream<Generator_>(gen_);
    }


    ////////////////////////////////////////////////////////////////////////
    //     TRANSFORM                                                      //
    ////////////////////////////////////////////////////////////////////////

    struct Stopper { };

    //! \brief Processes (up to) 6 input streams using given operation functor
    //!
    //! Template parameters:
    //! - \c Operation_ type of the operation (type of an
    //! adaptable functor that takes 6 parameters)
    //! - \c Input1_ type of the 1st input
    //! - \c Input2_ type of the 2nd input
    //! - \c Input3_ type of the 3rd input
    //! - \c Input4_ type of the 4th input
    //! - \c Input5_ type of the 5th input
    //! - \c Input6_ type of the 6th input
    template <class Operation_, class Input1_,
              class Input2_ = Stopper,
              class Input3_ = Stopper,
              class Input4_ = Stopper,
              class Input5_ = Stopper,
              class Input6_ = Stopper
              >
    class transform
    {
        Operation_ op;
        Input1_ & i1;
        Input2_ & i2;
        Input3_ & i3;
        Input4_ & i4;
        Input5_ & i5;
        Input6_ & i6;

    public:
        //! \brief Standard stream typedef
        typedef typename Operation_::value_type value_type;

    private:
        value_type current;

    public:
        //! \brief Construction
        transform(Operation_ o, Input1_ & i1_, Input2_ & i2_, Input3_ & i3_, Input4_ & i4_,
                  Input5_ & i5_, Input5_ & i6_) :
            op(o), i1(i1_), i2(i2_), i3(i3_), i4(i4_), i5(i5_), i6(i6_),
            current(op(*i1, *i2, *i3, *i4, *i5, *i6)) { }

        //! \brief Standard stream method
        const value_type & operator * () const
        {
            return current;
        }

        const value_type * operator -> () const
        {
            return &current;
        }

        //! \brief Standard stream method
        transform & operator ++ ()
        {
            ++i1;
            ++i2;
            ++i3;
            ++i4;
            ++i5;
            ++i6;

            if (!empty())
                current = op(*i1, *i2, *i3, *i4, *i5, *i6);


            return *this;
        }

        //! \brief Standard stream method
        bool empty() const
        {
            return i1.empty() || i2.empty() || i3.empty() ||
                   i4.empty() || i5.empty() || i6.empty();
        }
    };

    // Specializations

    template <class Operation_, class Input1_Iterator_>
    class transforming_iterator : public std::iterator<std::random_access_iterator_tag, typename Operation_::value_type>
    {
    public:
        typedef typename Operation_::value_type value_type;

    private:
        Input1_Iterator_ i1;
        Operation_ & op;
        mutable value_type current;

    public:
        transforming_iterator(Operation_ & op, const Input1_Iterator_ & i1) : i1(i1), op(op) { }

        const value_type & operator * () const  //RETURN BY VALUE
        {
            current = op(*i1);
            return current;
        }

        const value_type * operator -> () const
        {
            return &(operator * ());
        }

        transforming_iterator & operator ++ ()
        {
            ++i1;

            return *this;
        }

        transforming_iterator operator + (unsigned_type addend) const
        {
            return transforming_iterator(op, i1 + addend);
        }

        unsigned_type operator - (const transforming_iterator & subtrahend) const
        {
            return i1 - subtrahend.i1;
        }

        bool operator != (const transforming_iterator & ti) const
        {
            return ti.i1 != i1;
        }
    };


    ////////////////////////////////////////////////////////////////////////
    //     TRANSFORM (1 input stream)                                     //
    ////////////////////////////////////////////////////////////////////////

    //! \brief Processes an input stream using given operation functor
    //!
    //! Template parameters:
    //! - \c Operation_ type of the operation (type of an
    //! adaptable functor that takes 1 parameter)
    //! - \c Input1_ type of the input
    //! \remark This is a specialization of \c transform .
    template <class Operation_, class Input1_>
    class transform<Operation_, Input1_, Stopper, Stopper, Stopper, Stopper, Stopper>
    {
        Operation_ & op;
        Input1_ & i1;

    public:
        //! \brief Standard stream typedef
        typedef typename Operation_::value_type value_type;
        typedef transforming_iterator<Operation_, typename Input1_::const_iterator> const_iterator;

    public:
        //! \brief Construction
        transform(Operation_ & o, Input1_ & i1_) : op(o), i1(i1_) { }

        //! \brief Standard stream method
        void start_pull()
        {
            STXXL_VERBOSE0("transform " << this << " starts.");
            i1.start_pull();
            op.start_push();
        }

        //! \brief Standard stream method
        const value_type & operator * () const
        {
            return op(*i1);
        }

        const value_type * operator -> () const
        {
            return &(operator * ());
        }

        //! \brief Standard stream method
        transform & operator ++ ()
        {
            ++i1;

            return *this;
        }

        //! \brief Standard stream method
        bool empty() const
        {
            bool is_empty = i1.empty();
            if (is_empty)
            {
                STXXL_VERBOSE0("transform " << this << " stops pushing.");
                op.stop_push();
            }
            return is_empty;
        }

        //! \brief Batched stream method
        unsigned_type batch_length() const
        {
            unsigned_type batch_length = i1.batch_length();
            if (batch_length == 0)
            {
                STXXL_VERBOSE0("transform " << this << " stops pushing.");
                op.stop_push();
            }
            return batch_length;
        }

        //! \brief Batched stream method
        const_iterator batch_begin() const
        {
            return const_iterator(op, i1.batch_begin());
        }

        //! \brief Batched stream method
        const value_type & operator [] (unsigned_type index) const
        {
            return op(i1[index]);
        }

        //! \brief Batched stream method
        transform & operator += (unsigned_type length)
        {
            assert(length > 0);

            i1 += length;

            return *this;
        }
    };

    template <class Output_>
    class Pusher
    {
        Output_ & output;

    public:
        typedef typename Output_::value_type value_type;

        void start_push()
        {
            STXXL_VERBOSE0("pusher " << this << " starts push.");
            output.start_push();
        }

        Pusher(Output_ & output) : output(output)
        { }

        const value_type & operator () (const value_type & val)
        {
            output.push(val);
            return val;
        }

        void stop_push()
        {
            STXXL_VERBOSE0("pusher " << this << " stops push.");
            output.stop_push();
        }
    };

    template <class Input_, class Output_>
    class pusher : public transform<Pusher<Output_>, Input_>
    {
        Pusher<Output_> p;

    public:
        pusher(Input_ & input, Output_ & output) :
            transform<Pusher<Output_>, Input_>(p, input),
            p(output)
        { }
    };


    ////////////////////////////////////////////////////////////////////////
    //     TRANSFORM (2 input streams)                                    //
    ////////////////////////////////////////////////////////////////////////

    //! \brief Processes 2 input streams using given operation functor
    //!
    //! Template parameters:
    //! - \c Operation_ type of the operation (type of an
    //! adaptable functor that takes 2 parameters)
    //! - \c Input1_ type of the 1st input
    //! - \c Input2_ type of the 2nd input
    //! \remark This is a specialization of \c transform .
    template <class Operation_, class Input1_,
              class Input2_
              >
    class transform<Operation_, Input1_, Input2_, Stopper, Stopper, Stopper, Stopper>
    {
        Operation_ op;
        Input1_ & i1;
        Input2_ & i2;

    public:
        //! \brief Standard stream typedef
        typedef typename Operation_::value_type value_type;

    private:
        value_type current;

    public:
        //! \brief Construction
        transform(Operation_ o, Input1_ & i1_, Input2_ & i2_) : op(o), i1(i1_), i2(i2_),
                                                                current(op(*i1, *i2)) { }

        //! \brief Standard stream method
        const value_type & operator * () const
        {
            return current;
        }

        const value_type * operator -> () const
        {
            return &current;
        }

        //! \brief Standard stream method
        transform & operator ++ ()
        {
            ++i1;
            ++i2;
            if (!empty())
                current = op(*i1, *i2);


            return *this;
        }

        //! \brief Standard stream method
        bool empty() const
        {
            return i1.empty() || i2.empty();
        }
    };


    ////////////////////////////////////////////////////////////////////////
    //     TRANSFORM (3 input streams)                                    //
    ////////////////////////////////////////////////////////////////////////

    //! \brief Processes 3 input streams using given operation functor
    //!
    //! Template parameters:
    //! - \c Operation_ type of the operation (type of an
    //! adaptable functor that takes 3 parameters)
    //! - \c Input1_ type of the 1st input
    //! - \c Input2_ type of the 2nd input
    //! - \c Input3_ type of the 3rd input
    //! \remark This is a specialization of \c transform .
    template <class Operation_, class Input1_,
              class Input2_,
              class Input3_
              >
    class transform<Operation_, Input1_, Input2_, Input3_, Stopper, Stopper, Stopper>
    {
        Operation_ op;
        Input1_ & i1;
        Input2_ & i2;
        Input3_ & i3;

    public:
        //! \brief Standard stream typedef
        typedef typename Operation_::value_type value_type;

    private:
        value_type current;

    public:
        //! \brief Construction
        transform(Operation_ o, Input1_ & i1_, Input2_ & i2_, Input3_ & i3_) :
            op(o), i1(i1_), i2(i2_), i3(i3_),
            current(op(*i1, *i2, *i3)) { }

        //! \brief Standard stream method
        const value_type & operator * () const
        {
            return current;
        }

        const value_type * operator -> () const
        {
            return &current;
        }

        //! \brief Standard stream method
        transform & operator ++ ()
        {
            ++i1;
            ++i2;
            ++i3;
            if (!empty())
                current = op(*i1, *i2, *i3);


            return *this;
        }

        //! \brief Standard stream method
        bool empty() const
        {
            return i1.empty() || i2.empty() || i3.empty();
        }
    };


    ////////////////////////////////////////////////////////////////////////
    //     TRANSFORM (4 input streams)                                    //
    ////////////////////////////////////////////////////////////////////////

    //! \brief Processes 4 input streams using given operation functor
    //!
    //! Template parameters:
    //! - \c Operation_ type of the operation (type of an
    //! adaptable functor that takes 4 parameters)
    //! - \c Input1_ type of the 1st input
    //! - \c Input2_ type of the 2nd input
    //! - \c Input3_ type of the 3rd input
    //! - \c Input4_ type of the 4th input
    //! \remark This is a specialization of \c transform .
    template <class Operation_, class Input1_,
              class Input2_,
              class Input3_,
              class Input4_
              >
    class transform<Operation_, Input1_, Input2_, Input3_, Input4_, Stopper, Stopper>
    {
        Operation_ op;
        Input1_ & i1;
        Input2_ & i2;
        Input3_ & i3;
        Input4_ & i4;

    public:
        //! \brief Standard stream typedef
        typedef typename Operation_::value_type value_type;

    private:
        value_type current;

    public:
        //! \brief Construction
        transform(Operation_ o, Input1_ & i1_, Input2_ & i2_, Input3_ & i3_, Input4_ & i4_) :
            op(o), i1(i1_), i2(i2_), i3(i3_), i4(i4_),
            current(op(*i1, *i2, *i3, *i4)) { }

        //! \brief Standard stream method
        const value_type & operator * () const
        {
            return current;
        }

        const value_type * operator -> () const
        {
            return &current;
        }

        //! \brief Standard stream method
        transform & operator ++ ()
        {
            ++i1;
            ++i2;
            ++i3;
            ++i4;
            if (!empty())
                current = op(*i1, *i2, *i3, *i4);


            return *this;
        }

        //! \brief Standard stream method
        bool empty() const
        {
            return i1.empty() || i2.empty() || i3.empty() || i4.empty();
        }
    };


    ////////////////////////////////////////////////////////////////////////
    //     TRANSFORM (5 input streams)                                    //
    ////////////////////////////////////////////////////////////////////////

    //! \brief Processes 5 input streams using given operation functor
    //!
    //! Template parameters:
    //! - \c Operation_ type of the operation (type of an
    //! adaptable functor that takes 5 parameters)
    //! - \c Input1_ type of the 1st input
    //! - \c Input2_ type of the 2nd input
    //! - \c Input3_ type of the 3rd input
    //! - \c Input4_ type of the 4th input
    //! - \c Input5_ type of the 5th input
    //! \remark This is a specialization of \c transform .
    template <class Operation_, class Input1_,
              class Input2_,
              class Input3_,
              class Input4_,
              class Input5_
              >
    class transform<Operation_, Input1_, Input2_, Input3_, Input4_, Input5_, Stopper>
    {
        Operation_ op;
        Input1_ & i1;
        Input2_ & i2;
        Input3_ & i3;
        Input4_ & i4;
        Input5_ & i5;

    public:
        //! \brief Standard stream typedef
        typedef typename Operation_::value_type value_type;

    private:
        value_type current;

    public:
        //! \brief Construction
        transform(Operation_ o, Input1_ & i1_, Input2_ & i2_, Input3_ & i3_, Input4_ & i4_,
                  Input5_ & i5_) :
            op(o), i1(i1_), i2(i2_), i3(i3_), i4(i4_), i5(i5_),
            current(op(*i1, *i2, *i3, *i4, *i5)) { }

        //! \brief Standard stream method
        const value_type & operator * () const
        {
            return current;
        }

        const value_type * operator -> () const
        {
            return &current;
        }

        //! \brief Standard stream method
        transform & operator ++ ()
        {
            ++i1;
            ++i2;
            ++i3;
            ++i4;
            ++i5;
            if (!empty())
                current = op(*i1, *i2, *i3, *i4, *i5);


            return *this;
        }

        //! \brief Standard stream method
        bool empty() const
        {
            return i1.empty() || i2.empty() || i3.empty() || i4.empty() || i5.empty();
        }
    };


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
            boost::thread ** threads = new boost::thread*[num_outputs];
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
                            break; //empty() == true
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


    ////////////////////////////////////////////////////////////////////////
    //     MAKE TUPLE                                                     //
    ////////////////////////////////////////////////////////////////////////

    //! \brief Creates stream of 6-tuples from 6 input streams
    //!
    //! Template parameters:
    //! - \c Input1_ type of the 1st input
    //! - \c Input2_ type of the 2nd input
    //! - \c Input3_ type of the 3rd input
    //! - \c Input4_ type of the 4th input
    //! - \c Input5_ type of the 5th input
    //! - \c Input6_ type of the 6th input
    template <class Input1_,
              class Input2_,
              class Input3_ = Stopper,
              class Input4_ = Stopper,
              class Input5_ = Stopper,
              class Input6_ = Stopper
              >
    class make_tuple
    {
        Input1_ & i1;
        Input2_ & i2;
        Input3_ & i3;
        Input4_ & i4;
        Input5_ & i5;
        Input6_ & i6;

    public:
        //! \brief Standard stream typedef
        typedef typename stxxl::tuple<
            typename Input1_::value_type,
            typename Input2_::value_type,
            typename Input3_::value_type,
            typename Input4_::value_type,
            typename Input5_::value_type,
            typename Input6_::value_type
            > value_type;

    private:
        value_type current;

    public:
        //! \brief Construction
        make_tuple(
            Input1_ & i1_,
            Input2_ & i2_,
            Input3_ & i3_,
            Input4_ & i4_,
            Input5_ & i5_,
            Input6_ & i6_
            ) :
            i1(i1_), i2(i2_), i3(i3_), i4(i4_), i5(i5_), i6(i6_),
            current(value_type(*i1, *i2, *i3, *i4, *i5, *i6)) { }

        //! \brief Standard stream method
        const value_type & operator * () const
        {
            return current;
        }

        const value_type * operator -> () const
        {
            return &current;
        }

        //! \brief Standard stream method
        make_tuple & operator ++ ()
        {
            ++i1;
            ++i2;
            ++i3;
            ++i4;
            ++i5;
            ++i6;

            if (!empty())
                current = value_type(*i1, *i2, *i3, *i4, *i5, *i6);


            return *this;
        }

        //! \brief Standard stream method
        bool empty() const
        {
            return i1.empty() || i2.empty() || i3.empty() ||
                   i4.empty() || i5.empty() || i6.empty();
        }
    };

    template <class Input1_Iterator_, class Input2_Iterator_>
    class make_tuple_iterator : std::iterator<
                                    std::random_access_iterator_tag,
                                    tuple<typename std::iterator_traits<Input1_Iterator_>::value_type, typename std::iterator_traits<Input2_Iterator_>::value_type>
                                    >
    {
    public:
        typedef tuple<typename std::iterator_traits<Input1_Iterator_>::value_type, typename std::iterator_traits<Input2_Iterator_>::value_type> value_type;

    private:
        Input1_Iterator_ i1;
        Input2_Iterator_ i2;
        mutable value_type current;

    public:
        make_tuple_iterator(const Input1_Iterator_ & i1, const Input2_Iterator_ & i2) : i1(i1), i2(i2) { }

        const value_type & operator * () const  //RETURN BY VALUE
        {
            current = value_type(*i1, *i2);
            return current;
        }

        const value_type * operator -> () const
        {
            return &(operator * ());
        }

        make_tuple_iterator & operator ++ ()
        {
            ++i1;
            ++i2;

            return *this;
        }

        make_tuple_iterator operator + (unsigned_type addend) const
        {
            return make_tuple_iterator(i1 + addend, i2 + addend);
        }

        bool operator != (const make_tuple_iterator & mti) const
        {
            return mti.i1 != i1 || mti.i2 != i2;
        }
    };


    //! \brief Creates stream of 2-tuples (pairs) from 2 input streams
    //!
    //! Template parameters:
    //! - \c Input1_ type of the 1st input
    //! - \c Input2_ type of the 2nd input
    //! \remark A specialization of \c make_tuple .
    template <class Input1_,
              class Input2_
              >
    class make_tuple<Input1_, Input2_, Stopper, Stopper, Stopper, Stopper>
    {
    public:
        //! \brief Standard stream typedef
        typedef typename stxxl::tuple<
            typename Input1_::value_type,
            typename Input2_::value_type
            > value_type;
        typedef make_tuple_iterator<typename Input1_::const_iterator, typename Input2_::const_iterator> const_iterator;

    private:
        Input1_ & i1;
        Input2_ & i2;
        mutable value_type current;

    public:
        //! \brief Construction
        make_tuple(
            Input1_ & i1_,
            Input2_ & i2_
            ) :
            i1(i1_), i2(i2_)
        { }

        //! \brief Standard stream method
        void start_pull()
        {
            STXXL_VERBOSE0("make_tuple " << this << " starts.");
            i1.start_pull();
            i2.start_pull();
        }

        //! \brief Standard stream method
        void start_push()
        {
            //do nothing
        }

        //! \brief Standard stream method
        const value_type & operator * () const  //RETURN BY VALUE
        {
            current = value_type(*i1, *i2);
            return current;
        }

        const value_type * operator -> () const
        {
            return &(operator * ());
        }

        //! \brief Standard stream method
        make_tuple & operator ++ ()
        {
            ++i1;
            ++i2;

            return *this;
        }

        //! \brief Standard stream method
        bool empty() const
        {
            return i1.empty() || i2.empty();
        }

        //! \brief Batched stream method.
        unsigned_type batch_length()
        {
            return STXXL_MIN<unsigned_type>(i1.batch_length(), i2.batch_length());
        }

        //! \brief Batched stream method.
        make_tuple & operator += (unsigned_type size)
        {
            i1 += size;
            i2 += size;

            return *this;
        }

        //! \brief Batched stream method.
        const_iterator batch_begin()
        {
            return const_iterator(i1.batch_begin(), i2.batch_begin());
        }

        //! \brief Batched stream method.
        value_type operator [] (unsigned_type index) const
        {
            return value_type(i1[index], i2[index]);
        }
    };

    //! \brief Creates stream of 3-tuples from 3 input streams
    //!
    //! Template parameters:
    //! - \c Input1_ type of the 1st input
    //! - \c Input2_ type of the 2nd input
    //! - \c Input3_ type of the 3rd input
    //! \remark A specialization of \c make_tuple .
    template <class Input1_,
              class Input2_,
              class Input3_
              >
    class make_tuple<Input1_, Input2_, Input3_, Stopper, Stopper, Stopper>
    {
        Input1_ & i1;
        Input2_ & i2;
        Input3_ & i3;

    public:
        //! \brief Standard stream typedef
        typedef typename stxxl::tuple<
            typename Input1_::value_type,
            typename Input2_::value_type,
            typename Input3_::value_type
            > value_type;

    private:
        value_type current;

    public:
        //! \brief Construction
        make_tuple(
            Input1_ & i1_,
            Input2_ & i2_,
            Input3_ & i3_
            ) :
            i1(i1_), i2(i2_), i3(i3_),
            current(value_type(*i1, *i2, *i3)) { }

        //! \brief Standard stream method
        const value_type & operator * () const
        {
            return current;
        }

        const value_type * operator -> () const
        {
            return &current;
        }

        //! \brief Standard stream method
        make_tuple & operator ++ ()
        {
            ++i1;
            ++i2;
            ++i3;

            if (!empty())
                current = value_type(*i1, *i2, *i3);


            return *this;
        }

        //! \brief Standard stream method
        bool empty() const
        {
            return i1.empty() || i2.empty() || i3.empty();
        }
    };

    //! \brief Creates stream of 4-tuples from 4 input streams
    //!
    //! Template parameters:
    //! - \c Input1_ type of the 1st input
    //! - \c Input2_ type of the 2nd input
    //! - \c Input3_ type of the 3rd input
    //! - \c Input4_ type of the 4th input
    //! \remark A specialization of \c make_tuple .
    template <class Input1_,
              class Input2_,
              class Input3_,
              class Input4_
              >
    class make_tuple<Input1_, Input2_, Input3_, Input4_, Stopper, Stopper>
    {
        Input1_ & i1;
        Input2_ & i2;
        Input3_ & i3;
        Input4_ & i4;

    public:
        //! \brief Standard stream typedef
        typedef typename stxxl::tuple<
            typename Input1_::value_type,
            typename Input2_::value_type,
            typename Input3_::value_type,
            typename Input4_::value_type
            > value_type;

    private:
        value_type current;

    public:
        //! \brief Construction
        make_tuple(
            Input1_ & i1_,
            Input2_ & i2_,
            Input3_ & i3_,
            Input4_ & i4_
            ) :
            i1(i1_), i2(i2_), i3(i3_), i4(i4_),
            current(value_type(*i1, *i2, *i3, *i4)) { }

        //! \brief Standard stream method
        const value_type & operator * () const
        {
            return current;
        }

        const value_type * operator -> () const
        {
            return &current;
        }

        //! \brief Standard stream method
        make_tuple & operator ++ ()
        {
            ++i1;
            ++i2;
            ++i3;
            ++i4;

            if (!empty())
                current = value_type(*i1, *i2, *i3, *i4);


            return *this;
        }

        //! \brief Standard stream method
        bool empty() const
        {
            return i1.empty() || i2.empty() || i3.empty() ||
                   i4.empty();
        }
    };

    //! \brief Creates stream of 5-tuples from 5 input streams
    //!
    //! Template parameters:
    //! - \c Input1_ type of the 1st input
    //! - \c Input2_ type of the 2nd input
    //! - \c Input3_ type of the 3rd input
    //! - \c Input4_ type of the 4th input
    //! - \c Input5_ type of the 5th input
    //! \remark A specialization of \c make_tuple .
    template <
        class Input1_,
        class Input2_,
        class Input3_,
        class Input4_,
        class Input5_
        >
    class make_tuple<Input1_, Input2_, Input3_, Input4_, Input5_, Stopper>
    {
        Input1_ & i1;
        Input2_ & i2;
        Input3_ & i3;
        Input4_ & i4;
        Input5_ & i5;

    public:
        //! \brief Standard stream typedef
        typedef typename stxxl::tuple<
            typename Input1_::value_type,
            typename Input2_::value_type,
            typename Input3_::value_type,
            typename Input4_::value_type,
            typename Input5_::value_type
            > value_type;

    private:
        value_type current;

    public:
        //! \brief Construction
        make_tuple(
            Input1_ & i1_,
            Input2_ & i2_,
            Input3_ & i3_,
            Input4_ & i4_,
            Input5_ & i5_
            ) :
            i1(i1_), i2(i2_), i3(i3_), i4(i4_), i5(i5_),
            current(value_type(*i1, *i2, *i3, *i4, *i5)) { }

        //! \brief Standard stream method
        const value_type & operator * () const
        {
            return current;
        }

        const value_type * operator -> () const
        {
            return &current;
        }

        //! \brief Standard stream method
        make_tuple & operator ++ ()
        {
            ++i1;
            ++i2;
            ++i3;
            ++i4;
            ++i5;

            if (!empty())
                current = value_type(*i1, *i2, *i3, *i4, *i5);


            return *this;
        }

        //! \brief Standard stream method
        bool empty() const
        {
            return i1.empty() || i2.empty() || i3.empty() ||
                   i4.empty() || i5.empty();
        }
    };


    ////////////////////////////////////////////////////////////////////////
    //     CHOOSE                                                         //
    ////////////////////////////////////////////////////////////////////////

    template <class Input_, int Which>
    class choose
    { };

    //! \brief Creates stream from a tuple stream taking the first component of each tuple
    //!
    //! Template parameters:
    //! - \c Input_ type of the input tuple stream
    //!
    //! \remark Tuple stream is a stream which \c value_type is \c stxxl::tuple .
    template <class Input_>
    class choose<Input_, 1>
    {
        Input_ & in;

        typedef typename Input_::value_type tuple_type;

    public:
        //! \brief Standard stream typedef
        typedef typename tuple_type::first_type value_type;

    private:
        value_type current;

    public:
        //! \brief Construction
        choose(Input_ & in_) :
            in(in_),
            current((*in_).first) { }

        //! \brief Standard stream method
        const value_type & operator * () const
        {
            return current;
        }

        const value_type * operator -> () const
        {
            return &current;
        }

        //! \brief Standard stream method
        choose & operator ++ ()
        {
            ++in;

            if (!empty())
                current = (*in).first;


            return *this;
        }

        //! \brief Standard stream method
        bool empty() const
        {
            return in.empty();
        }
    };

    //! \brief Creates stream from a tuple stream taking the second component of each tuple
    //!
    //! Template parameters:
    //! - \c Input_ type of the input tuple stream
    //!
    //! \remark Tuple stream is a stream which \c value_type is \c stxxl::tuple .
    template <class Input_>
    class choose<Input_, 2>
    {
        Input_ & in;

        typedef typename Input_::value_type tuple_type;

    public:
        //! \brief Standard stream typedef
        typedef typename tuple_type::second_type value_type;

    private:
        value_type current;

    public:
        //! \brief Construction
        choose(Input_ & in_) :
            in(in_),
            current((*in_).second) { }

        //! \brief Standard stream method
        const value_type & operator * () const
        {
            return current;
        }

        const value_type * operator -> () const
        {
            return &current;
        }

        //! \brief Standard stream method
        choose & operator ++ ()
        {
            ++in;

            if (!empty())
                current = (*in).second;


            return *this;
        }

        //! \brief Standard stream method
        bool empty() const
        {
            return in.empty();
        }
    };

    //! \brief Creates stream from a tuple stream taking the third component of each tuple
    //!
    //! Template parameters:
    //! - \c Input_ type of the input tuple stream
    //!
    //! \remark Tuple stream is a stream which \c value_type is \c stxxl::tuple .
    template <class Input_>
    class choose<Input_, 3>
    {
        Input_ & in;

        typedef typename Input_::value_type tuple_type;

    public:
        //! \brief Standard stream typedef
        typedef typename tuple_type::third_type value_type;

    private:
        value_type current;

    public:
        //! \brief Construction
        choose(Input_ & in_) :
            in(in_),
            current((*in_).third) { }

        //! \brief Standard stream method
        const value_type & operator * () const
        {
            return current;
        }

        const value_type * operator -> () const
        {
            return &current;
        }

        //! \brief Standard stream method
        choose & operator ++ ()
        {
            ++in;

            if (!empty())
                current = (*in).third;


            return *this;
        }

        //! \brief Standard stream method
        bool empty() const
        {
            return in.empty();
        }
    };

    //! \brief Creates stream from a tuple stream taking the fourth component of each tuple
    //!
    //! Template parameters:
    //! - \c Input_ type of the input tuple stream
    //!
    //! \remark Tuple stream is a stream which \c value_type is \c stxxl::tuple .
    template <class Input_>
    class choose<Input_, 4>
    {
        Input_ & in;

        typedef typename Input_::value_type tuple_type;

    public:
        //! \brief Standard stream typedef
        typedef typename tuple_type::fourth_type value_type;

    private:
        value_type current;

    public:
        //! \brief Construction
        choose(Input_ & in_) :
            in(in_),
            current((*in_).fourth) { }

        //! \brief Standard stream method
        const value_type & operator * () const
        {
            return current;
        }

        const value_type * operator -> () const
        {
            return &current;
        }

        //! \brief Standard stream method
        choose & operator ++ ()
        {
            ++in;

            if (!empty())
                current = (*in).fourth;


            return *this;
        }

        //! \brief Standard stream method
        bool empty() const
        {
            return in.empty();
        }
    };

    //! \brief Creates stream from a tuple stream taking the fifth component of each tuple
    //!
    //! Template parameters:
    //! - \c Input_ type of the input tuple stream
    //!
    //! \remark Tuple stream is a stream which \c value_type is \c stxxl::tuple .
    template <class Input_>
    class choose<Input_, 5>
    {
        Input_ & in;

        typedef typename Input_::value_type tuple_type;

    public:
        //! \brief Standard stream typedef
        typedef typename tuple_type::fifth_type value_type;

    private:
        value_type current;

    public:
        //! \brief Construction
        choose(Input_ & in_) :
            in(in_),
            current((*in_).fifth) { }

        //! \brief Standard stream method
        const value_type & operator * () const
        {
            return current;
        }

        const value_type * operator -> () const
        {
            return &current;
        }

        //! \brief Standard stream method
        choose & operator ++ ()
        {
            ++in;

            if (!empty())
                current = (*in).fifth;


            return *this;
        }

        //! \brief Standard stream method
        bool empty() const
        {
            return in.empty();
        }
    };

    //! \brief Creates stream from a tuple stream taking the sixth component of each tuple
    //!
    //! Template parameters:
    //! - \c Input_ type of the input tuple stream
    //!
    //! \remark Tuple stream is a stream which \c value_type is \c stxxl::tuple .
    template <class Input_>
    class choose<Input_, 6>
    {
        Input_ & in;

        typedef typename Input_::value_type tuple_type;

    public:
        //! \brief Standard stream typedef
        typedef typename tuple_type::sixth_type value_type;

    private:
        value_type current;

    public:
        //! \brief Construction
        choose(Input_ & in_) :
            in(in_),
            current((*in_).sixth) { }

        //! \brief Standard stream method
        const value_type & operator * () const
        {
            return current;
        }

        const value_type * operator -> () const
        {
            return &current;
        }

        //! \brief Standard stream method
        choose & operator ++ ()
        {
            ++in;

            if (!empty())
                current = (*in).sixth;


            return *this;
        }

        //! \brief Standard stream method
        bool empty() const
        {
            return in.empty();
        }
    };


    ////////////////////////////////////////////////////////////////////////
    //     UNIQUE                                                         //
    ////////////////////////////////////////////////////////////////////////

    //! \brief Equivalent to std::unique algorithms
    //!
    //! Removes consecutive duplicates from the stream.
    //! Uses BinaryPredicate to compare elements of the stream
    template <class Input, class BinaryPredicate = Stopper>
    class unique
    {
        Input & input;
        BinaryPredicate binary_pred;
        typename Input::value_type current;

    public:
        typedef typename Input::value_type value_type;
        unique(Input & input_, BinaryPredicate binary_pred_) : input(input_), binary_pred(binary_pred_)
        {
            if (!input.empty())
                current = *input;
        }

        //! \brief Standard stream method
        unique & operator ++ ()
        {
            value_type old_value = current;
            ++input;
            while (!input.empty() && (binary_pred(current = *input, old_value)))
                ++input;
        }
        //! \brief Standard stream method
        const value_type & operator * () const
        {
            return current;
        }

        //! \brief Standard stream method
        const value_type * operator -> () const
        {
            return &current;
        }

        //! \brief Standard stream method
        bool empty() const { return input.empty(); }
    };

    //! \brief Equivalent to std::unique algorithms
    //!
    //! Removes consecutive duplicates from the stream.
    template <class Input>
    class unique<Input, Stopper>
    {
        Input & input;
        typename Input::value_type current;

    public:
        typedef typename Input::value_type value_type;
        unique(Input & input_) : input(input_)
        {
            if (!input.empty())
                current = *input;
        }
        //! \brief Standard stream method
        unique & operator ++ ()
        {
            value_type old_value = current;
            ++input;
            while (!input.empty() && ((current = *input) == old_value))
                ++input;
        }
        //! \brief Standard stream method
        value_type & operator * () const
        {
            return current;
        }

        //! \brief Standard stream method
        const value_type * operator -> () const
        {
            return &current;
        }

        //! \brief Standard stream method
        bool empty() const { return input.empty(); }
    };


//! \}
}

__STXXL_END_NAMESPACE

#endif // !STXXL_STREAM_HEADER
