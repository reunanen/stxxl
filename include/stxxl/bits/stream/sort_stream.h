#ifndef SORT_STREAM_HEADER
#define SORT_STREAM_HEADER

/***************************************************************************
 *            sort_stream.h
 *
 *  Thu Oct  2 12:09:50 2003
 *  Copyright  2003  Roman Dementiev
 *  dementiev@mpi-sb.mpg.de
 *  Copyright  2007  Johannes Singler
 *  singler@ira.uka.de
 ****************************************************************************/

#ifdef __MCSTL__
 #include <mcstl.h>
#endif

#include "stxxl/bits/stream/stream.h"
#include "stxxl/sort"

#ifdef STXXL_BOOST_CONFIG
 #include <boost/config.hpp>
#endif


__STXXL_BEGIN_NAMESPACE

/* deprecated
   template < unsigned blk_sz,class Y,class X >
   struct bid_iterator_traits<
    __gnu_cxx::__normal_iterator<  sort_local::trigger_entry<BID<blk_sz>,Y> * ,  X> >
        {
                enum
                {
                        block_size = blk_sz
                };
        };
 */

namespace stream
{
    //! \addtogroup streampack Stream package
    //! \{
    
    template <unsigned_type modulo>
    class blocked_index
    {
      unsigned_type pos;
      unsigned_type block;
      unsigned_type offset;

      void set(unsigned_type pos)
      {
        this->pos = pos;
        block = pos / modulo;
        offset = pos % modulo;
      }

    public:
      blocked_index()
      {
        set(0);
      }

      blocked_index(unsigned_type pos)
      {
        set(pos);
      }

      void operator=(unsigned_type pos)
      {
        set(pos);
      }

      blocked_index& operator++()
      {
        ++pos;
        ++offset;
        if(offset == modulo)
        {
          offset = 0;
          block++;
        }
        return *this;
      }

      blocked_index& operator+=(unsigned_type addend)
      {
        set(pos + addend);
        return *this;
      }

      operator unsigned_type() const
      {
        return pos;
      }

      const unsigned_type& get_block() const
      {
        return block;
      }

      const unsigned_type& get_offset() const
      {
        return offset;
      }
    };

    template <class ValueType, class TriggerEntryType>
    struct sorted_runs
    {
        typedef TriggerEntryType trigger_entry_type;
        typedef ValueType value_type;
        typedef typename trigger_entry_type::bid_type bid_type;
        typedef stxxl::int64 size_type;
        typedef std::vector<trigger_entry_type> run_type;
        typedef typed_block<bid_type::size, value_type> block_type;
        size_type elements;
        std::vector<run_type> runs;
        std::vector<unsigned_type> runs_sizes;

        // Optimization:
        // if the input is small such that its total size is
        // less than B (block_type::size)
        // then input is sorted internally
        // and kept in the array "small"
        std::vector<ValueType> small_;

        sorted_runs() : elements(0) { }

        //! \brief Deallocates the blocks which the runs occupy
        //!
        //! \remark Usually there is no need in calling this method,
        //! since the \c runs_merger calls it when it is being destructed
        void deallocate_blocks()
        {
            block_manager * bm = block_manager::get_instance();
            for (unsigned_type i = 0; i < runs.size(); ++i)
                bm->delete_blocks(
                    trigger_entry_iterator < typename run_type::iterator, block_type::raw_size > (runs[i].begin()),
                    trigger_entry_iterator < typename run_type::iterator, block_type::raw_size > (runs[i].end()) );


            runs.clear();
        }
    };

    //! \brief Forms sorted runs of data from a stream
    //!
    //! Template parameters:
    //! - \c Input_ type of the input stream
    //! - \c Cmp_ type of comparison object used for sorting the runs
    //! - \c BlockSize_ size of blocks used to store the runs
    //! - \c AllocStr_ functor that defines allocation strategy for the runs
    template <
              class Input_,
              class Cmp_,
              unsigned BlockSize_ = STXXL_DEFAULT_BLOCK_SIZE (typename Input_::value_type),
              class AllocStr_ = STXXL_DEFAULT_ALLOC_STRATEGY>
    class basic_runs_creator
    {
    protected:
        Input_ &input;
        Cmp_ cmp;
    public:
        typedef Cmp_ cmp_type;
        typedef typename Input_::value_type value_type;
        typedef BID<BlockSize_> bid_type;
        typedef typed_block<BlockSize_, value_type> block_type;
        typedef sort_local::trigger_entry<bid_type, value_type> trigger_entry_type;
        typedef sorted_runs<value_type, trigger_entry_type> sorted_runs_type;
    private:
        typedef typename sorted_runs_type::run_type run_type;
        sorted_runs_type result_; // stores the result (sorted runs)
        unsigned_type m_; // memory for internal use in blocks
        bool result_computed; // true result is already computed (used in 'result' method)

        basic_runs_creator(); // default construction is forbidden
        basic_runs_creator(const basic_runs_creator & ); // copy construction is forbidden
        basic_runs_creator & operator = (const basic_runs_creator &); // copying is forbidden

#if STXXL_STREAM_SORT_ASYNCHRONOUS_READ
        pthread_t waiter_and_fetcher;
        pthread_mutex_t mutex;
        pthread_cond_t cond;
        volatile bool work_done, terminate_requested;
#endif

        void compute_result();
        void sort_run(block_type * run, unsigned_type elements)
        {
            if (block_type::has_filler)
                std::sort(
                    TwoToOneDimArrayRowAdaptor <
                                                block_type,
                                                value_type,
                                                block_type::size > (run, 0 ),
                    TwoToOneDimArrayRowAdaptor<
                                               block_type,
                                               value_type,
                                               block_type::size > (run,
                                                                   elements ),
                    cmp);

            else
                std::sort(run[0].elem, run[0].elem + elements, cmp);
        }
    protected:
        virtual blocked_index<block_type::size> fetch(block_type * Blocks, blocked_index<block_type::size> begin, unsigned_type limit) = 0;

#if STXXL_STREAM_SORT_ASYNCHRONOUS_READ
        void start_waiting_and_fetching();

        void join_waiting_and_fetching()
        {
            void* res;
            terminate_requested = true;
            pthread_cond_signal(&cond);
            pthread_join(waiter_and_fetcher, &res);
        }

        struct WaitFetchJob
        {
            request_ptr* write_reqs;
            block_type* Blocks;
            blocked_index<block_type::size> begin;
            unsigned_type wait_run_size;
            unsigned_type read_run_size;
            blocked_index<block_type::size> end;	//return value
        };

        WaitFetchJob wait_fetch_job;
#endif

        const unsigned_type el_in_run; // number of elements in a run

    public:
#if STXXL_STREAM_SORT_ASYNCHRONOUS_READ
        void async_wait_and_fetch()
        {
            while(true)
            {
                pthread_mutex_lock(&mutex);
                while(work_done && !terminate_requested)
                    pthread_cond_wait(&cond, &mutex);
                if(terminate_requested)
                {
                    pthread_mutex_unlock(&mutex);
                    return;
                }
                pthread_mutex_unlock(&mutex);

                wait_fetch_job.end = wait_fetch_job.begin;
                unsigned_type i;
                for (i = 0; i < wait_fetch_job.wait_run_size; ++i)
                {   //for each block to wait for
                    //wait for only next block
                    wait_all(wait_fetch_job.write_reqs + i, wait_fetch_job.write_reqs + i + 1);
                    //in the mean time, next write request will advance
                    wait_fetch_job.end =
                        fetch(wait_fetch_job.Blocks, wait_fetch_job.end, wait_fetch_job.end +  block_type::size);
                }
                for (; i < wait_fetch_job.read_run_size; ++i)
                {
                    wait_fetch_job.end =
                        fetch(wait_fetch_job.Blocks, wait_fetch_job.end, wait_fetch_job.end +  block_type::size);
                }

                pthread_mutex_lock(&mutex);
                work_done = true;
                pthread_cond_signal(&cond);	//wake up other thread
                pthread_mutex_unlock(&mutex);
            }
        }

        //the following two functions form a asynchronous fetch()
        void start_write_read(request_ptr* write_reqs, block_type* Blocks, unsigned_type wait_run_size, unsigned_type read_run_size)
        {
            wait_fetch_job.write_reqs = write_reqs;
            wait_fetch_job.Blocks = Blocks;
            wait_fetch_job.wait_run_size = wait_run_size;
            wait_fetch_job.read_run_size = read_run_size;

            pthread_mutex_lock(&mutex);
            work_done = false;
            pthread_mutex_unlock(&mutex);
            pthread_cond_signal(&cond);	//wake up other thread
        }

        blocked_index<block_type::size> wait_write_read()
        {
            pthread_mutex_lock(&mutex);
            while(!work_done)
                pthread_cond_wait(&cond, &mutex);	//wait for other thread
            pthread_mutex_unlock(&mutex);

            return wait_fetch_job.end;
        }
#endif

        //! \brief Creates the object
        //! \param i input stream
        //! \param c comparator object
        //! \param memory_to_use memory amount that is allowed to used by the sorter in bytes
        basic_runs_creator(Input_ & i, Cmp_ c, unsigned_type memory_to_use) :
            input(i), cmp(c), m_(memory_to_use / BlockSize_ / sort_memory_usage_factor()), result_computed(false), el_in_run((m_ / 2) * block_type::size)
        {
#if STXXL_STREAM_SORT_ASYNCHRONOUS_READ
            pthread_mutex_init(&mutex, 0);
            pthread_cond_init(&cond, 0);
#endif
            assert (2 * BlockSize_ * sort_memory_usage_factor() <= memory_to_use);
        }

        virtual ~basic_runs_creator()
        {
#if STXXL_STREAM_SORT_ASYNCHRONOUS_READ
            pthread_mutex_destroy(&mutex);
            pthread_cond_destroy(&cond);
#endif
        }

        //! \brief Returns the sorted runs object
        //! \return Sorted runs object. The result is computed lazily, i.e. on the first call
        //! \remark Returned object is intended to be used by \c runs_merger object as input
        const sorted_runs_type & result()
        {
            if (!result_computed)
            {
                input.start();
                compute_result();
                result_computed = true;
#ifdef STXXL_PRINT_STAT_AFTER_RF
                STXXL_MSG(*stats::get_instance());
#endif
            }
            return result_;
        }

    };

    template <class Input_, class Cmp_, unsigned BlockSize_, class AllocStr_>
    void basic_runs_creator<Input_, Cmp_, BlockSize_, AllocStr_>::
    compute_result()
    {
        unsigned_type i = 0;
        unsigned_type m2 = m_ / 2;
        STXXL_VERBOSE1("runs_creator::compute_result m2=" << m2);
        blocked_index<block_type::size> blocks1_length = 0, blocks2_length = 0;

#ifndef STXXL_SMALL_INPUT_PSORT_OPT
        block_type * Blocks1 = new block_type[m2 * 2];
#else

        //read first block
        while (!input.empty() && blocks1_length != block_type::size)
        {
             result_.small_.push_back(*input);
             ++input;
             ++blocks1_length;
        }


        block_type * Blocks1;

        if ( blocks1_length == block_type::size && !input.empty() )
        {      // enlarge/reallocate Blocks1 array
            Blocks1 = new block_type[m2 * 2];
            std::copy(result_.small_.begin(), result_.small_.end(), Blocks1[0].begin());
            result_.small_.clear();
        }
        else
        {
            STXXL_VERBOSE1("runs_creator: Small input optimization, input length: " << blocks1_length);
            result_.elements = blocks1_length;
            std::sort(result_.small_.begin(), result_.small_.end(), cmp);
#if STXXL_STREAM_SORT_ASYNCHRONOUS_READ
            join_waiting_and_fetching();
#endif
            return;
        }
#endif

        blocks1_length = fetch(Blocks1, blocks1_length, el_in_run);	//fetch rest, first block already in place
        bool already_empty_after_1_block = input.empty();

        block_type * Blocks2 = Blocks1 + m2;	//second half

#if STXXL_STREAM_SORT_ASYNCHRONOUS_READ
        start_write_read(NULL, Blocks2, 0, m2);	//no write, just read
#endif

        // sort first run
        sort_run(Blocks1, blocks1_length);
        result_.elements = blocks1_length;

        if (blocks1_length < block_type::size && already_empty_after_1_block) // small input, do not flush it on the disk(s)
        {
            STXXL_VERBOSE1("runs_creator: Small input optimization, input length: " << blocks1_length);
            result_.small_.resize(blocks1_length);
            std::copy(Blocks1[0].begin(), Blocks1[0].begin() + blocks1_length, result_.small_.begin());
            delete [] Blocks1;
#if STXXL_STREAM_SORT_ASYNCHRONOUS_READ
            join_waiting_and_fetching();
#endif
            return;
        }


        block_manager * bm = block_manager::get_instance();
        request_ptr * write_reqs = new request_ptr[m2];
        run_type run;


        unsigned_type cur_run_size = div_and_round_up(blocks1_length, block_type::size); // in blocks

        run.resize(cur_run_size);
        bm->new_blocks(AllocStr_(),
                       trigger_entry_iterator < typename run_type::iterator, block_type::raw_size > (run.begin()),
                       trigger_entry_iterator < typename run_type::iterator, block_type::raw_size > (run.end())
        );

        disk_queues::get_instance ()->set_priority_op(disk_queue::WRITE);


        // fill the rest of the last block with max values
        for (blocked_index<block_type::size> rest = blocks1_length; rest != el_in_run; ++rest)
            Blocks1[rest.get_block()][rest.get_offset()] = cmp.max_value();

        //write first run
        for (i = 0; i < cur_run_size; ++i)
        {
            run[i].value = Blocks1[i][0];
            write_reqs[i] = Blocks1[i].write(run[i].bid);
            //STXXL_MSG("BID: "<<run[i].bid<<" val: "<<run[i].value);
        }

        result_.runs_sizes.push_back(blocks1_length);
        result_.runs.push_back(run); // #

        bool already_empty_after_2_blocks;

#if STXXL_STREAM_SORT_ASYNCHRONOUS_READ
        blocks2_length = wait_write_read();
        already_empty_after_2_blocks = input.empty();

        start_write_read(write_reqs, Blocks1, cur_run_size, m2);
#endif

        if (already_empty_after_1_block)
        {
#if STXXL_STREAM_SORT_ASYNCHRONOUS_READ
            wait_write_read();	//for Blocks1
#else
            wait_all(write_reqs, write_reqs + cur_run_size);
#endif
            delete [] write_reqs;
            delete [] Blocks1;
#if STXXL_STREAM_SORT_ASYNCHRONOUS_READ
            join_waiting_and_fetching();
#endif
            return;
        }

        STXXL_VERBOSE1("Filling the second part of the allocated blocks");

#if !STXXL_STREAM_SORT_ASYNCHRONOUS_READ
        blocks2_length = fetch(Blocks2, 0, el_in_run);
        already_empty_after_2_blocks = input.empty();
#endif

        result_.elements += blocks2_length;

        if (already_empty_after_2_blocks)
        {   //optimization if whole set fits into both halves
            // (re)sort internally and return
            sort_run(Blocks1, result_.elements); // sort first an second run together
#if STXXL_STREAM_SORT_ASYNCHRONOUS_READ
            wait_write_read();	//for Blocks1
#else
            wait_all(write_reqs, write_reqs + cur_run_size);
#endif
            bm->delete_blocks(trigger_entry_iterator < typename run_type::iterator, block_type::raw_size > (run.begin()),
                              trigger_entry_iterator < typename run_type::iterator, block_type::raw_size > (run.end()) );

            cur_run_size = div_and_round_up(result_.elements, block_type::size);
            run.resize(cur_run_size);
            bm->new_blocks(AllocStr_(),
                           trigger_entry_iterator < typename run_type::iterator, block_type::raw_size > (run.begin()),
                           trigger_entry_iterator < typename run_type::iterator, block_type::raw_size > (run.end())
            );

            // fill the rest of the last block with max values
            for (blocked_index<block_type::size> rest = result_.elements; rest != 2 * el_in_run; ++rest)
                Blocks1[rest.get_block()][rest.get_offset()] = cmp.max_value();

            assert(cur_run_size > m2);

            for (i = 0; i < m2; ++i)
            {
                run[i].value = Blocks1[i][0];
#if !STXXL_STREAM_SORT_ASYNCHRONOUS_READ
                write_reqs[i]->wait();
#endif
                write_reqs[i] = Blocks1[i].write(run[i].bid);
            }

            request_ptr * write_reqs1 = new request_ptr[cur_run_size - m2];

            for ( ; i < cur_run_size; ++i)
            {
                run[i].value = Blocks1[i][0];
                write_reqs1[i - m2] = Blocks1[i].write(run[i].bid);
            }

            result_.runs_sizes[0] = result_.elements;
            result_.runs[0] = run;

            wait_all(write_reqs, write_reqs + m2);
            wait_all(write_reqs1, write_reqs1 + cur_run_size - m2);

            delete [] write_reqs;
            delete [] write_reqs1;

            delete [] Blocks1;

#if STXXL_STREAM_SORT_ASYNCHRONOUS_READ
            join_waiting_and_fetching();
#endif
            return;
        }

        sort_run(Blocks2, blocks2_length);

        cur_run_size = div_and_round_up(blocks2_length, block_type::size); // in blocks
        run.resize(cur_run_size);
        bm->new_blocks(AllocStr_(),
                       trigger_entry_iterator < typename run_type::iterator, block_type::raw_size > (run.begin()),
                       trigger_entry_iterator < typename run_type::iterator, block_type::raw_size > (run.end())
        );

#if STXXL_STREAM_SORT_ASYNCHRONOUS_READ
        blocks1_length = wait_write_read();
#endif

        for (i = 0; i < cur_run_size; ++i)
        {
            run[i].value = Blocks2[i][0];
#if !STXXL_STREAM_SORT_ASYNCHRONOUS_READ
            write_reqs[i]->wait();
#endif
            write_reqs[i] = Blocks2[i].write(run[i].bid);
        }

#if STXXL_STREAM_SORT_ASYNCHRONOUS_READ
        start_write_read(write_reqs, Blocks2, cur_run_size, m2);
#endif

        result_.runs_sizes.push_back(blocks2_length);
        result_.runs.push_back(run);

#if STXXL_STREAM_SORT_ASYNCHRONOUS_READ
        while (blocks1_length > 0)
        {
#else
        while (!input.empty())
        {
            blocks1_length = fetch(Blocks1, 0, el_in_run);
#endif
            sort_run(Blocks1, blocks1_length);
            cur_run_size = div_and_round_up(blocks1_length, block_type::size); // in blocks
            run.resize(cur_run_size);
            bm->new_blocks(AllocStr_(),
                           trigger_entry_iterator < typename run_type::iterator, block_type::raw_size > (run.begin()),
                           trigger_entry_iterator < typename run_type::iterator, block_type::raw_size > (run.end())
            );

            // fill the rest of the last block with max values (occurs only on the last run)
            for (blocked_index<block_type::size> rest = blocks1_length ; rest != el_in_run; ++rest)
                Blocks1[rest.get_block()][rest.get_offset()] = cmp.max_value();

#if STXXL_STREAM_SORT_ASYNCHRONOUS_READ
            blocks2_length = wait_write_read();
#endif

            for (i = 0; i < cur_run_size; ++i)
            {
                run[i].value = Blocks1[i][0];
#if !STXXL_STREAM_SORT_ASYNCHRONOUS_READ
                write_reqs[i]->wait();
#endif
                write_reqs[i] = Blocks1[i].write(run[i].bid);
            }

#if STXXL_STREAM_SORT_ASYNCHRONOUS_READ
            start_write_read(write_reqs, Blocks1, cur_run_size, m2);
#endif

            result_.runs_sizes.push_back(blocks1_length);
            result_.elements += blocks1_length;
            result_.runs.push_back(run); // #

            std::swap(Blocks1, Blocks2);
            std::swap(blocks1_length, blocks2_length);
        }

#if STXXL_STREAM_SORT_ASYNCHRONOUS_READ
        wait_write_read();
#else
        wait_all(write_reqs, write_reqs + cur_run_size);
#endif
        delete [] write_reqs;
        delete [] ((Blocks1 < Blocks2) ? Blocks1 : Blocks2);
        
#if STXXL_STREAM_SORT_ASYNCHRONOUS_READ
        join_waiting_and_fetching();
#endif
    }

#if STXXL_STREAM_SORT_ASYNCHRONOUS_READ
//! \brief Helper function to call basic_pull_stage::async_pull() in a thread.
template <
              class Input_,
              class Cmp_,
              unsigned BlockSize_,
              class AllocStr_>
void* call_async_wait_and_fetch(void* param)
{
	static_cast<basic_runs_creator<Input_, Cmp_, BlockSize_, AllocStr_>*>(param)->async_wait_and_fetch();
	return NULL;
}

//! \brief Start pulling data asynchronously.
template <
              class Input_,
              class Cmp_,
              unsigned BlockSize_,
              class AllocStr_>
void basic_runs_creator<Input_, Cmp_, BlockSize_, AllocStr_>::start_waiting_and_fetching()
{
       work_done = true; //so far, nothing to do
       terminate_requested = false;
       pthread_create(&waiter_and_fetcher, NULL, call_async_wait_and_fetch<Input_, Cmp_, BlockSize_, AllocStr_>, this);
}
#endif

    //! \brief Forms sorted runs of data from a stream
    //!
    //! Template parameters:
    //! - \c Input_ type of the input stream
    //! - \c Cmp_ type of omparison object used for sorting the runs
    //! - \c BlockSize_ size of blocks used to store the runs
    //! - \c AllocStr_ functor that defines allocation strategy for the runs
    template <
              class Input_,
              class Cmp_,
              unsigned BlockSize_ = STXXL_DEFAULT_BLOCK_SIZE (typename Input_::value_type),
              class AllocStr_ = STXXL_DEFAULT_ALLOC_STRATEGY>
    class runs_creator : public basic_runs_creator<Input_, Cmp_, BlockSize_, AllocStr_>
    {
    private:
      typedef basic_runs_creator<Input_, Cmp_, BlockSize_, AllocStr_> base;
    public:
      typedef typename base::block_type block_type;
    private:

      using base::input;

      virtual blocked_index<block_type::size> fetch(block_type * Blocks, blocked_index<block_type::size> pos, unsigned_type limit)
      {
        while (!input.empty() && pos != limit)
        {
            Blocks[pos.get_block()][pos.get_offset()] = *input;
            ++input;
            ++pos;
        }
        return pos;
      }

      runs_creator(); // default construction is forbidden
      runs_creator(const runs_creator & ); // copy construction is forbidden
      runs_creator & operator = (const runs_creator &); // copying is forbidden

    public:
        //! \brief Creates the object
        //! \param i input stream
        //! \param c comparator object
        //! \param memory_to_use memory amount that is allowed to used by the sorter in bytes
        runs_creator(Input_ & i, Cmp_ c, unsigned_type memory_to_use) : base(i, c, memory_to_use)
        {
#if STXXL_STREAM_SORT_ASYNCHRONOUS_READ
            base::start_waiting_and_fetching();
#endif
        }

    };

    //! \brief Forms sorted runs of data from a stream
    //!
    //! Template parameters:
    //! - \c Input_ type of the input stream
    //! - \c Cmp_ type of comparison object used for sorting the runs
    //! - \c BlockSize_ size of blocks used to store the runs
    //! - \c AllocStr_ functor that defines allocation strategy for the runs
    template <
              class Input_,
              class Cmp_,
              unsigned BlockSize_ = STXXL_DEFAULT_BLOCK_SIZE (typename Input_::value_type),
              class AllocStr_ = STXXL_DEFAULT_ALLOC_STRATEGY>
    class runs_creator_batch : public basic_runs_creator<Input_, Cmp_, BlockSize_, AllocStr_>
    {
    private:
      typedef basic_runs_creator<Input_, Cmp_, BlockSize_, AllocStr_> base;
      typedef typename base::block_type block_type;

      using base::input;

      virtual blocked_index<block_type::size> fetch(block_type * Blocks, blocked_index<block_type::size> pos, unsigned_type limit)
      {
        unsigned_type length, pos_in_block = pos.get_offset(), block_no = pos.get_block();
        while(((length = input.batch_length()) > 0) && pos != limit)
        {
          length = std::min<unsigned_type>(length, std::min(limit - pos, block_type::size - pos_in_block));
          typename block_type::iterator bi = Blocks[block_no].begin() + pos_in_block;
          for(typename Input_::const_iterator i = input.batch_begin(), end = input.batch_begin() + length; i != end; ++i)
          {
            *bi = *i;
            ++bi;
          }
          input += length;
          pos += length;
          pos_in_block += length;
          if(pos_in_block == block_type::size)
          {
            block_no++;
            pos_in_block = 0;
          }
        }
        return pos;
      }

      runs_creator_batch(); // default construction is forbidden
      runs_creator_batch(const runs_creator_batch & ); // copy construction is forbidden
      runs_creator_batch & operator = (const runs_creator_batch &); // copying is forbidden

    public:
        //! \brief Creates the object
        //! \param i input stream
        //! \param c comparator object
        //! \param memory_to_use memory amount that is allowed to used by the sorter in bytes
        runs_creator_batch(Input_ & i, Cmp_ c, unsigned_type memory_to_use) : base(i, c, memory_to_use)
        {
#if STXXL_STREAM_SORT_ASYNCHRONOUS_READ
            base::start_waiting_and_fetching();
#endif
        }

    };


    //! \brief Input strategy for \c runs_creator class
    //!
    //! This strategy together with \c runs_creator class
    //! allows to create sorted runs
    //! data structure usable for \c runs_merger
    //! pushing elements into the sorter
    //! (using runs_creator::push())
    template <class ValueType_>
    struct use_push
    {
        typedef ValueType_ value_type;
    };

    //! \brief Forms sorted runs of elements passed in push() method
    //!
    //! A specialization of \c runs_creator that
    //! allows to create sorted runs
    //! data structure usable for \c runs_merger from
    //! elements passed in sorted push() method. <BR>
    //! Template parameters:
    //! - \c ValueType_ type of values (parameter for \c use_push strategy)
    //! - \c Cmp_ type of comparison object used for sorting the runs
    //! - \c BlockSize_ size of blocks used to store the runs
    //! - \c AllocStr_ functor that defines allocation strategy for the runs
    template <
              class ValueType_,
              class Cmp_,
              unsigned BlockSize_,
              class AllocStr_>
    class runs_creator<
                       use_push<ValueType_>,
                       Cmp_,
                       BlockSize_,
                       AllocStr_>
    {
        Cmp_ cmp;
    public:
        typedef Cmp_ cmp_type;
        typedef ValueType_ value_type;
        typedef BID<BlockSize_> bid_type;
        typedef typed_block<BlockSize_, value_type> block_type;
        typedef sort_local::trigger_entry<bid_type, value_type> trigger_entry_type;
        typedef sorted_runs<value_type, trigger_entry_type> sorted_runs_type;
        typedef sorted_runs_type result_type;
    private:
        typedef typename sorted_runs_type::run_type run_type;
        sorted_runs_type result_; // stores the result (sorted runs)
        unsigned_type m_; // memory for internal use in blocks

        bool output_requested; // true after the result() method was called for the first time

        const unsigned_type m2;
        const unsigned_type el_in_run;
        blocked_index<block_type::size> cur_el;
        block_type * Blocks1;
        block_type * Blocks2;
        request_ptr * write_reqs;
        run_type run;

	//! \brief Mutex variable, to mutual exclude the other thread.
	pthread_mutex_t mutex;
	//! \brief Condition variable, to wait for the other thread.
	pthread_cond_t cond;
        volatile bool result_ready;


        runs_creator(); // default construction is forbidden
        runs_creator(const runs_creator & ); // copy construction is forbidden
        runs_creator & operator = (const runs_creator &); // copying is forbidden


        void sort_run(block_type * run, unsigned_type elements)
        {
            if (block_type::has_filler)
                std::sort(
                    TwoToOneDimArrayRowAdaptor <
                                                block_type,
                                                value_type,
                                                block_type::size > (run, 0 ),
                    TwoToOneDimArrayRowAdaptor<
                                               block_type,
                                               value_type,
                                               block_type::size > (run,
                                                                   elements ),
                    cmp);

            else
                std::sort(run[0].elem, run[0].elem + elements, cmp);
        }

        void finish_result()
        {
            STXXL_VERBOSE0("runs_creator " << this << " finish_result");
            if (cur_el == 0)
                return;

            blocked_index<block_type::size> cur_el_reg = cur_el;
            sort_run(Blocks1, cur_el_reg);
            result_.elements += cur_el_reg;
            if (cur_el_reg < unsigned_type(block_type::size) &&
                unsigned_type(result_.elements) == cur_el_reg)         // small input, do not flush it on the disk(s)
            {
                STXXL_VERBOSE1("runs_creator(use_push): Small input optimization, input length: " << cur_el_reg);
                result_.small_.resize(cur_el_reg);
                std::copy(Blocks1[0].begin(), Blocks1[0].begin() + cur_el_reg, result_.small_.begin());
                return;
            }

            const unsigned_type cur_run_size = div_and_round_up(cur_el_reg, block_type::size);     // in blocks
            run.resize(cur_run_size);
            block_manager * bm = block_manager::get_instance();
            bm->new_blocks(AllocStr_(),
                           trigger_entry_iterator < typename run_type::iterator, block_type::raw_size > (run.begin()),
                           trigger_entry_iterator < typename run_type::iterator, block_type::raw_size > (run.end())
            );

            disk_queues::get_instance ()->set_priority_op(disk_queue::WRITE);

            result_.runs_sizes.push_back(cur_el_reg);

            // fill the rest of the last block with max values
            for (blocked_index<block_type::size> rest = cur_el_reg; rest != el_in_run; ++rest)
                Blocks1[rest.get_block()][rest.get_offset()] = cmp.max_value();


            unsigned_type i = 0;
            for ( ; i < cur_run_size; ++i)
            {
                run[i].value = Blocks1[i][0];
                if (write_reqs[i].get())
                    write_reqs[i]->wait();

                write_reqs[i] = Blocks1[i].write(run[i].bid);
            }
            result_.runs.push_back(run);

            for (i = 0; i < m2; ++i)
                if (write_reqs[i].get())
                    write_reqs[i]->wait();
        }
        void cleanup()
        {
            delete [] write_reqs;
            delete [] ((Blocks1 < Blocks2) ? Blocks1 : Blocks2);
            write_reqs = NULL;
            Blocks1 = Blocks2 = NULL;
        }
    public:
        //! \brief Creates the object
        //! \param c comparator object
        //! \param memory_to_use memory amount that is allowed to used by the sorter in bytes
        runs_creator(Cmp_ c, unsigned_type memory_to_use) :
            cmp(c), m_(memory_to_use / BlockSize_ / sort_memory_usage_factor()), output_requested(false),
            m2(m_ / 2),
            el_in_run(m2 * block_type::size),
            cur_el(0),
            Blocks1(new block_type[m2 * 2]),
            Blocks2(Blocks1 + m2),
            write_reqs(new request_ptr[m2]),
            result_ready(false)
        {
            assert (2 * BlockSize_ * sort_memory_usage_factor() <= memory_to_use);
            pthread_mutex_init(&mutex, 0);
            pthread_cond_init(&cond, 0);
        }

        ~runs_creator()
        {
            pthread_mutex_destroy(&mutex);
            pthread_cond_destroy(&cond);
        }

        void start()
        {
            STXXL_VERBOSE0("runs_creator " << this << " starts.");
            //do nothing
        }

        void start_push()
        {
            STXXL_VERBOSE0("runs_creator " << this << " starts push.");
            //do nothing
        }

        ~runs_creator()
        {
            if (!output_requested)
                cleanup();
        }

        //! \brief Adds new element to the sorter
        //! \param val value to be added
        void push(const value_type & val)
        {
            assert(output_requested == false);
            if (cur_el < el_in_run)
            {
                Blocks1[cur_el.get_block()][cur_el.get_offset()] = val;
                ++cur_el;
                return;
            }

            assert(el_in_run == cur_el);
            cur_el = 0;

            //sort and store Blocks1
            sort_run(Blocks1, el_in_run);
            result_.elements += el_in_run;

            const unsigned_type cur_run_size = div_and_round_up(el_in_run, block_type::size);    // in blocks
            run.resize(cur_run_size);
            block_manager * bm = block_manager::get_instance();
            bm->new_blocks(AllocStr_(),
                           trigger_entry_iterator < typename run_type::iterator, block_type::raw_size > (run.begin()),
                           trigger_entry_iterator < typename run_type::iterator, block_type::raw_size > (run.end())
            );

            disk_queues::get_instance ()->set_priority_op(disk_queue::WRITE);

            result_.runs_sizes.push_back(el_in_run);

            for (unsigned_type i = 0; i < cur_run_size; ++i)
            {
                run[i].value = Blocks1[i][0];
                if ( write_reqs[i].get() )
                    write_reqs[i]->wait();

                write_reqs[i] = Blocks1[i].write(run[i].bid);
            }

            result_.runs.push_back(run);

            std::swap(Blocks1, Blocks2);

            //remaining element
            push(val);	//recursive call
        }

        unsigned_type push_batch_length()
        {
          return el_in_run - cur_el + 1;
        }

        //! \brief Adds new element to the sorter
        //! \param val value to be added
        template<class Iterator>
        void push_batch(Iterator batch_begin, Iterator batch_end)
        {
            assert(output_requested == false);
            assert((batch_end - batch_begin) > 0);

            --batch_end;	//save last element
            unsigned_type block_no = cur_el.get_block(), pos_in_block = cur_el.get_offset();
            while(batch_begin < batch_end)
            {
              unsigned_type length = std::min<unsigned_type>(batch_end - batch_begin, block_type::size - pos_in_block);
              typename block_type::iterator bi = Blocks1[block_no].begin() + pos_in_block;
              for(Iterator end = batch_begin + length; batch_begin != end; ++batch_begin)
              {
                *bi = *batch_begin;
                ++bi;
              }
              cur_el += length;
              pos_in_block += length;
              assert(pos_in_block <= block_type::size);
              if(pos_in_block == block_type::size)
              {
                block_no++;
                pos_in_block = 0;
              }
            }
            assert(batch_begin == batch_end);
            push(*batch_end);

            return;
        }

        void stop_push()
        {
            STXXL_VERBOSE0("runs_creator use_push " << this << " stops pushing.");
            if (!output_requested)
            {
                finish_result();
                output_requested = true;
                cleanup();
#ifdef STXXL_PRINT_STAT_AFTER_RF
                STXXL_MSG(*stats::get_instance());
#endif
                pthread_mutex_lock(&mutex);
                result_ready = true;
                pthread_cond_signal(&cond);
                pthread_mutex_unlock(&mutex);
            }
        }

        //! \brief Returns the sorted runs object
        //! \return Sorted runs object.
        //! \remark Returned object is intended to be used by \c runs_merger object as input
        const sorted_runs_type & result()
        {
            pthread_mutex_lock(&mutex);
            while(!result_ready)
                pthread_cond_wait(&cond, &mutex);
            pthread_mutex_unlock(&mutex);
            return result_;
        }
    };



    //! \brief Input strategy for \c runs_creator class
    //!
    //! This strategy together with \c runs_creator class
    //! allows to create sorted runs
    //! data structure usable for \c runs_merger from
    //! sequences of elements in sorted order
    template <class ValueType_>
    struct from_sorted_sequences
    {
        typedef ValueType_ value_type;
    };

    //! \brief Forms sorted runs of data taking elements in sorted order (element by element)
    //!
    //! A specialization of \c runs_creator that
    //! allows to create sorted runs
    //! data structure usable for \c runs_merger from
    //! sequences of elements in sorted order. <BR>
    //! Template parameters:
    //! - \c ValueType_ type of values (parameter for \c from_sorted_sequences strategy)
    //! - \c Cmp_ type of comparison object used for sorting the runs
    //! - \c BlockSize_ size of blocks used to store the runs
    //! - \c AllocStr_ functor that defines allocation strategy for the runs
    template <
              class ValueType_,
              class Cmp_,
              unsigned BlockSize_,
              class AllocStr_>
    class runs_creator<
                       from_sorted_sequences<ValueType_>,
                       Cmp_,
                       BlockSize_,
                       AllocStr_>
    {
        typedef ValueType_ value_type;
        typedef BID<BlockSize_> bid_type;
        typedef typed_block<BlockSize_, value_type> block_type;
        typedef sort_local::trigger_entry<bid_type, value_type> trigger_entry_type;
        typedef AllocStr_ alloc_strategy_type;
        Cmp_ cmp;

    public:
        typedef Cmp_ cmp_type;
        typedef sorted_runs<value_type, trigger_entry_type> sorted_runs_type;
        typedef sorted_runs_type result_type;
    private:
        typedef typename sorted_runs_type::run_type run_type;
        sorted_runs_type result_; // stores the result (sorted runs)
        unsigned_type m_; // memory for internal use in blocks
        buffered_writer<block_type>  writer;
        block_type * cur_block;
        unsigned_type offset;
        unsigned_type iblock;
        unsigned_type irun;
        alloc_strategy_type alloc_strategy;

        runs_creator(); // default construction is forbidden
        runs_creator(const runs_creator & ); // copy construction is forbidden
        runs_creator & operator = (const runs_creator &); // copying is forbidden

    public:
        //! \brief Creates the object
        //! \param c comparator object
        //! \param memory_to_use memory amount that is allowed to used by the sorter in bytes.
        //! Recommended value: 2 * block_size * D
        runs_creator(Cmp_ c, unsigned_type memory_to_use) :
            cmp(c),
            m_(memory_to_use / BlockSize_ / sort_memory_usage_factor()),
            writer(m_, m_ / 2),
            cur_block(writer.get_free_block()),
            offset(0),
            iblock(0),
            irun(0)
        {
            assert (2 * BlockSize_ * sort_memory_usage_factor() <= memory_to_use);
        }

        //! \brief Adds new element to the current run
        //! \param val value to be added to the current run
        void push(const value_type & val)
        {
            assert(offset < block_type::size );

            (*cur_block)[offset] = val;
            ++offset;

            if (offset == block_type::size)
            {
                // write current block

                block_manager * bm = block_manager::get_instance ();
                // allocate space for the block
                result_.runs.resize(irun + 1);
                result_.runs[irun].resize( iblock + 1 );
                bm->new_blocks(
                    offset_allocator < alloc_strategy_type > (iblock, alloc_strategy),
                    trigger_entry_iterator < typename run_type::iterator, block_type::raw_size > (result_.runs[irun].begin()
                                                                                                  + iblock),
                    trigger_entry_iterator < typename run_type::iterator, block_type::raw_size > (result_.runs[irun].end())
                );

                result_.runs[irun][iblock].value = (*cur_block)[0];         // init trigger
                cur_block = writer.write(cur_block, result_.runs[irun][iblock].bid);
                ++iblock;

                offset = 0;
            }

            ++result_.elements;
        }

        //! \brief Finishes current run and begins new one
        void finish()
        {
            if (offset == 0 && iblock == 0)  // current run is empty
                return;


            result_.runs_sizes.resize(irun + 1);
            result_.runs_sizes.back() = iblock * block_type::size + offset;

            if (offset)    // if current block is partially filled
            {
                while (offset != block_type::size)
                {
                    (*cur_block)[offset] = cmp.max_value();
                    ++offset;
                }
                offset = 0;

                block_manager *  bm = block_manager::get_instance ();
                // allocate space for the block
                result_.runs.resize(irun + 1);
                result_.runs[irun].resize( iblock + 1 );
                bm->new_blocks(
                    offset_allocator < alloc_strategy_type > (iblock, alloc_strategy),
                    trigger_entry_iterator < typename run_type::iterator, block_type::raw_size > (result_.runs[irun].begin()
                                                                                                  + iblock),
                    trigger_entry_iterator < typename run_type::iterator, block_type::raw_size > (result_.runs[irun].end())
                );

                result_.runs[irun][iblock].value = (*cur_block)[0];         // init trigger
                cur_block = writer.write(cur_block, result_.runs[irun][iblock].bid);
            }
            else
            { }

            iblock = 0;
            ++irun;
        }

        //! \brief Returns the sorted runs object
        //! \return Sorted runs object
        //! \remark Returned object is intended to be used by \c runs_merger object as input
        const sorted_runs_type & result()
        {
            finish();
            writer.flush();

            return result_;
        }
    };



    //! \brief Checker for the sorted runs object created by the \c runs_creator .
    //! \param sruns sorted runs object
    //! \param cmp comparison object used for checking the order of elements in runs
    //! \return \c true if runs are sorted, \c false otherwise
    template <class RunsType_, class Cmp_>
    bool check_sorted_runs(RunsType_ & sruns, Cmp_ cmp)
    {
        typedef typename RunsType_::block_type block_type;
        typedef typename block_type::value_type value_type;
        STXXL_VERBOSE1("Elements: " << sruns.elements);
        unsigned_type nruns = sruns.runs.size();
        STXXL_VERBOSE1("Runs: " << nruns);
        unsigned_type irun = 0;
        for (irun = 0; irun < nruns; ++irun)
        {
            const unsigned_type nblocks = sruns.runs[irun].size();
            block_type * blocks = new block_type[nblocks];
            request_ptr * reqs = new request_ptr[nblocks];
            for (unsigned_type j = 0; j < nblocks; ++j)
            {
                reqs[j] = blocks[j].read(sruns.runs[irun][j].bid);
            }
            wait_all(reqs, reqs + nblocks);
            for (unsigned_type j = 0; j < nblocks; ++j)
            {
                if (blocks[j][0] != sruns.runs[irun][j].value)
                {
                    STXXL_ERRMSG("check_sorted_runs  wrong trigger in the run");
                    return false;
                }
            }
            if (!is_sorted(
                    TwoToOneDimArrayRowAdaptor <
                                                block_type,
                                                value_type,
                                                block_type::size > (blocks, 0 ),
                    TwoToOneDimArrayRowAdaptor<
                                               block_type,
                                               value_type,
                                               block_type::size > (blocks,
                                                                   //nblocks*block_type::size
                                                                   //(irun<nruns-1)?(nblocks*block_type::size): (sruns.elements%(nblocks*block_type::size))
                                                                   sruns.runs_sizes[irun]
                    ), cmp) )
            {
                STXXL_ERRMSG("check_sorted_runs  wrong order in the run");
                return false;
            }

            delete [] reqs;
            delete [] blocks;
        }

        STXXL_MSG("Checking runs finished successfully");

        return true;
    }



    //! \brief Merges sorted runs
    //!
    //! Template parameters:
    //! - \c RunsType_ type of the sorted runs, available as \c runs_creator::sorted_runs_type ,
    //! - \c Cmp_ type of comparison object used for merging
    //! - \c AllocStr_ allocation strategy used to allocate the blocks for
    //! storing intermediate results if several merge passes are required
    template <  class RunsType_,
              class Cmp_,
              class AllocStr_ = STXXL_DEFAULT_ALLOC_STRATEGY >
    class basic_runs_merger
    {
    protected:
        typedef RunsType_ sorted_runs_type;
        typedef AllocStr_ alloc_strategy;
        typedef typename sorted_runs_type::size_type size_type;
        typedef Cmp_ value_cmp;
        typedef typename sorted_runs_type::run_type run_type;
        typedef typename sorted_runs_type::block_type block_type;
        typedef typename block_type::bid_type bid_type;
        typedef block_prefetcher < block_type, typename run_type::iterator > prefetcher_type;
        typedef run_cursor2<block_type, prefetcher_type> run_cursor_type;
        typedef sort_local::run_cursor2_cmp<block_type, prefetcher_type, value_cmp> run_cursor2_cmp_type;
        typedef loser_tree<run_cursor_type, run_cursor2_cmp_type, block_type::size> loser_tree_type;

        typedef stxxl::int64 diff_type;
        typedef std::pair < typename block_type::iterator, typename block_type::iterator > sequence;
        typedef typename std::vector<sequence>::size_type seqs_size_type;
        std::vector<sequence>* seqs;
        std::vector<block_type *>* buffers;



        sorted_runs_type sruns;
        unsigned_type m_; //  blocks to use - 1
        value_cmp cmp;
        size_type elements_remaining;
        unsigned_type buffer_pos;
        block_type * current_block;
        run_type consume_seq;
        prefetcher_type * prefetcher;
        loser_tree_type * losers;
        int_type * prefetch_seq;
        unsigned_type nruns;
#ifdef STXXL_CHECK_ORDER_IN_SORTS
        typename block_type::value_type last_element;
#endif

        void merge_recursively();

        basic_runs_merger(); // forbidden
        basic_runs_merger(const basic_runs_merger &); // forbidden
        basic_runs_merger & operator = (const basic_runs_merger &); // copying is forbidden
        void deallocate_prefetcher()
        {
            if (prefetcher)
            {
                delete losers;
                delete seqs;
                delete buffers;
                delete prefetcher;
                delete [] prefetch_seq;
                prefetcher = NULL;
            }
            // free blocks in runs , (or the user should do it?)
            sruns.deallocate_blocks();
        }

        void initialize_current_block()
        {
#if defined (__MCSTL__) && defined (STXXL_PARALLEL_MULTIWAY_MERGE)

// begin of STL-style merging

            //task: merge

            if (!stxxl::SETTINGS::native_merge && mcstl::HEURISTIC::num_threads >= 1)
            {
                seqs = new std::vector<sequence>(nruns);
                buffers = new std::vector<block_type *> (nruns);

                for (unsigned_type i = 0; i < nruns; i++)            //initialize sequences
                {
                    (*buffers)[i] = prefetcher->pull_block();           //get first block of each run
                    (*seqs)[i] = std::make_pair((*buffers)[i]->begin(), (*buffers)[i]->end());          //this memory location stays the same, only the data is exchanged
                }

// end of STL-style merging
            }
            else
            {
#endif

// begin of native merging procedure

            losers = new loser_tree_type(prefetcher, nruns, run_cursor2_cmp_type(cmp));

// end of native merging procedure
#if defined (__MCSTL__) && defined (STXXL_PARALLEL_MULTIWAY_MERGE)
        }
#endif
        }

        void fill_current_block()
        {
#if defined (__MCSTL__) && defined (STXXL_PARALLEL_MULTIWAY_MERGE)

// begin of STL-style merging

            //taks: merge

            if (!stxxl::SETTINGS::native_merge && mcstl::HEURISTIC::num_threads >= 1)
            {
 #ifdef STXXL_CHECK_ORDER_IN_SORTS
                value_type last_elem;
 #endif

                diff_type rest = block_type::size;                      //elements still to merge for this output block

                do              //while rest > 0 and still elements available
                {
                    value_type * min_last_element = NULL;               //no element found yet
                    diff_type total_size = 0;

                    for (seqs_size_type i = 0; i < (*seqs).size(); i++)
                    {
                        if ((*seqs)[i].first == (*seqs)[i].second)
                            continue; //run empty

                        if (min_last_element == NULL)
                            min_last_element = &(*((*seqs)[i].second - 1));
                        else
                            min_last_element = cmp(*min_last_element, *((*seqs)[i].second - 1)) ? min_last_element : &(*((*seqs)[i].second - 1));

                        total_size += (*seqs)[i].second - (*seqs)[i].first;
                        STXXL_VERBOSE1("last " << *((*seqs)[i].second - 1) << " block size " << ((*seqs)[i].second - (*seqs)[i].first));
                    }

                    assert(min_last_element != NULL);           //there must be some element

                    STXXL_VERBOSE1("min_last_element " << min_last_element << " total size " << total_size + (block_type::size - rest));

                    diff_type less_equal_than_min_last = 0;
                    //locate this element in all sequences
                    for (seqs_size_type i = 0; i < (*seqs).size(); i++)
                    {
                        if ((*seqs)[i].first == (*seqs)[i].second)
                            continue; //empty subsequence

                        typename block_type::iterator position = std::upper_bound((*seqs)[i].first, (*seqs)[i].second, *min_last_element, cmp);
                        STXXL_VERBOSE1("greater equal than " << position - (*seqs)[i].first);
                        less_equal_than_min_last += position - (*seqs)[i].first;
                    }

                    STXXL_VERBOSE1("finished loop");

                    ptrdiff_t output_size = (std::min)(less_equal_than_min_last, rest);                           //at most rest elements

                    STXXL_VERBOSE1("before merge" << output_size);

                    mcstl::multiway_merge((*seqs).begin(), (*seqs).end(), current_block->end() - rest, cmp, output_size, false);                         //sequence iterators are progressed appropriately

                    STXXL_VERBOSE1("after merge");

                    rest -= output_size;

                    STXXL_VERBOSE1("so long");

                    for (seqs_size_type i = 0; i < (*seqs).size(); i++)
                    {
                        if ((*seqs)[i].first == (*seqs)[i].second)                            //run empty
                        {
                            if (prefetcher->block_consumed((*buffers)[i]))
                            {
                                (*seqs)[i].first = (*buffers)[i]->begin();                                    //reset iterator
                                (*seqs)[i].second = (*buffers)[i]->end();
                                STXXL_VERBOSE1("block ran empty " << i);
                            }
                            else
                            {
                                (*seqs).erase((*seqs).begin() + i);                                   //remove this sequence
                                (*buffers).erase((*buffers).begin() + i);
                                STXXL_VERBOSE1("seq removed " << i);
                            }
                        }
                    }
                } while (rest > 0 && (*seqs).size() > 0);

 #ifdef STXXL_CHECK_ORDER_IN_SORTS
                if (!stxxl::is_sorted(current_block->begin(), current_block->end(), cmp))
                {
                    for (value_type * i = current_block->begin() + 1; i != current_block->end(); i++)
                        if (cmp(*i, *(i - 1)))
                        {
                            STXXL_VERBOSE1("Error at position " << (i - current_block->begin()));
                        }
                    assert(false);
                }



 #endif


// end of STL-style merging
            }
            else
            {
#endif

// begin of native merging procedure

            losers->multi_merge(current_block->elem);

#if defined (__MCSTL__) && defined (STXXL_PARALLEL_MULTIWAY_MERGE)
// end of native merging procedure
        }
#endif
        }

    public:
        //! \brief Standard stream typedef
        typedef typename sorted_runs_type::value_type value_type;
        typedef const value_type* const_iterator;

        //! \brief Creates a runs merger object
        //! \param r input sorted runs object
        //! \param c comparison object
        //! \param memory_to_use amount of memory available for the merger in bytes
        basic_runs_merger(const sorted_runs_type & r, value_cmp c, unsigned_type memory_to_use) :
            sruns(r),
            m_(memory_to_use / block_type::raw_size / sort_memory_usage_factor() /* - 1 */), cmp(c),
            current_block(NULL),
#ifdef STXXL_CHECK_ORDER_IN_SORTS
            last_element(cmp.min_value()),
#endif
            prefetcher(NULL)
        {
            initialize(r);
        }

    protected:
        //! \brief Creates a runs merger object
        //! \param r input sorted runs object
        //! \param c comparison object
        //! \param memory_to_use amount of memory available for the merger in bytes
        basic_runs_merger(value_cmp c, unsigned_type memory_to_use) :
            m_(memory_to_use / block_type::raw_size / sort_memory_usage_factor() /* - 1 */), cmp(c),
            current_block(NULL),
#ifdef STXXL_CHECK_ORDER_IN_SORTS
            last_element(cmp.min_value()),
#endif
            prefetcher(NULL)
        {
        }

        void initialize(const sorted_runs_type & r)
        {
            sruns = r;
            elements_remaining = r.elements;

            if (empty())
                return;



            if (!sruns.small_.empty()) // we have a small input < B,
            // that is kept in the main memory
            {
                STXXL_VERBOSE1("basic_runs_merger: small input optimization, input length: " << elements_remaining);
                assert(elements_remaining == size_type(sruns.small_.size()));
                current_block = new block_type;
                std::copy(sruns.small_.begin(), sruns.small_.end(), current_block->begin());
                current_value = current_block->elem[0];
                buffer_pos = 1;

                return;
            }

#ifdef STXXL_CHECK_ORDER_IN_SORTS
            assert(check_sorted_runs(r, c));
#endif

            current_block = new block_type;

            disk_queues::get_instance ()->set_priority_op (disk_queue::WRITE);

            nruns = sruns.runs.size();

            if (m_ < nruns)
            {
                // can not merge runs in one pass
                // merge recursively:
                STXXL_ERRMSG("The implementation of sort requires more than one merge pass, therefore for a better");
                STXXL_ERRMSG("efficiency decrease block size of run storage (a parameter of the run_creator)");
                STXXL_ERRMSG("or increase the amount memory dedicated to the merger.");
                STXXL_ERRMSG("m = " << m_ << " nruns=" << nruns);

                merge_recursively();

                nruns = sruns.runs.size();
            }

            assert(nruns <= m_);

            unsigned_type i;
            /*
               const unsigned_type out_run_size =
                  div_and_round_up(elements_remaining,size_type(block_type::size));
             */
            unsigned_type prefetch_seq_size = 0;
            for (i = 0; i < nruns; i++)
            {
                prefetch_seq_size += sruns.runs[i].size();
            }

            consume_seq.resize(prefetch_seq_size);

            prefetch_seq = new int_type[prefetch_seq_size];

            typename run_type::iterator copy_start = consume_seq.begin ();
            for (i = 0; i < nruns; i++)
            {
                copy_start = std::copy(
                    sruns.runs[i].begin (),
                    sruns.runs[i].end (),
                    copy_start      );
            }

            std::stable_sort(consume_seq.begin (), consume_seq.end (),
                             sort_local::trigger_entry_cmp<bid_type, value_type, value_cmp>(cmp));

            int_type disks_number = config::get_instance ()->disks_number ();

            const int_type n_prefetch_buffers = STXXL_MAX( 2 * disks_number, (int_type(m_) - int_type(nruns)) );


#ifdef SORT_OPTIMAL_PREFETCHING
            // heuristic
            const int_type n_opt_prefetch_buffers = 2 * disks_number + (3 * (n_prefetch_buffers - 2 * disks_number)) / 10;

            compute_prefetch_schedule(
                consume_seq,
                prefetch_seq,
                n_opt_prefetch_buffers,
                disks_number );
#else
            for (i = 0; i < prefetch_seq_size; ++i)
                prefetch_seq[i] = i;

#endif


            prefetcher = new prefetcher_type(
                consume_seq.begin(),
                consume_seq.end(),
                prefetch_seq,
                nruns + n_prefetch_buffers);

            losers = NULL;
            seqs = NULL;
            buffers = NULL;

            initialize_current_block();
            fill_current_block();


            current_value = current_block->elem[0];
            buffer_pos = 1;

            if (elements_remaining <= block_type::size)
                deallocate_prefetcher();
        }

    public:
        //! \brief Standard stream method
        bool empty() const
        {
            return elements_remaining == 0;
        }

        //! \brief Standard stream method
        basic_runs_merger & operator ++() // preincrement operator
        {
            assert(!empty());

            --elements_remaining;

            if (buffer_pos != block_type::size)
            {
//              printf("single %lld\n", block_type::size - buffer_pos);
              current_value = current_block->elem[buffer_pos];
              ++buffer_pos;
            }
            else
            {
                if (!empty())
                {
                    fill_current_block();

#ifdef STXXL_CHECK_ORDER_IN_SORTS
                    assert(is_sorted(current_block->elem, current_block->elem + current_block->size, cmp));
                    assert(!cmp(current_block->elem[0], current_value));
#endif
                    current_value = current_block->elem[0];
                    buffer_pos = 1;
                }
                if (elements_remaining <= block_type::size)
                    deallocate_prefetcher();
            }


#ifdef STXXL_CHECK_ORDER_IN_SORTS
            if (!empty())
            {
                assert(!cmp(current_value, last_element));
                last_element = current_value;
            }
#endif

            return *this;
        }
        //! \brief Standard stream method
        const value_type & operator * () const
        {
            assert(!empty());
            return current_value;
        }

        //! \brief Standard stream method
        const value_type * operator -> () const
        {
            assert(!empty());
            return &current_value;
        }

        //! \brief Batched stream method.
        unsigned_type batch_length()
        {
          return std::min<unsigned_type>(block_type::size - buffer_pos + 1, elements_remaining);
        }

        //! \brief Batched stream method.
        basic_runs_merger& operator += (unsigned_type length)
        {
          assert(length > 0);

          elements_remaining -= (length - 1);
          buffer_pos += (length - 1);

          operator++();

          return *this;
        }

        //! \brief Batched stream method.
        const_iterator batch_begin()
        {
            return current_block->elem + buffer_pos - 1;
        }

        //! \brief Batched stream method.
        const value_type& operator[](unsigned_type index)
        {
            assert(index < batch_length());
            return *(current_block->elem + buffer_pos - 1 + index);
        }

        //! \brief Destructor
        //! \remark Deallocates blocks of the input sorted runs object
        virtual ~basic_runs_merger()
        {
            deallocate_prefetcher();

            if (current_block)
                delete current_block;

            // free blocks in runs , (or the user should do it?)
            sruns.deallocate_blocks();
        }
    private:
        // cache for the current value
        value_type current_value;
    };


    template <class RunsType_, class Cmp_, class AllocStr_>
    void basic_runs_merger<RunsType_, Cmp_, AllocStr_>::merge_recursively()
    {
        block_manager * bm = block_manager::get_instance();
        unsigned_type ndisks = config::get_instance ()->disks_number ();
        unsigned_type nwrite_buffers = 2 * ndisks;

        unsigned_type nruns = sruns.runs.size();
        const unsigned_type merge_factor = static_cast<unsigned_type>(ceil(pow(nruns, 1. / ceil(
                                                                                   log(double (nruns)) / log(double (m_))))));
        assert(merge_factor <= m_);
        while (nruns > m_)
        {
            unsigned_type new_nruns = div_and_round_up(nruns, merge_factor);
            STXXL_VERBOSE("Starting new merge phase: nruns: " << nruns <<
                          " opt_merge_factor: " << merge_factor << " m:" << m_ << " new_nruns: " << new_nruns);

            sorted_runs_type new_runs;
            new_runs.runs.resize(new_nruns);
            new_runs.runs_sizes.resize(new_nruns);
            new_runs.elements = sruns.elements;

            unsigned_type runs_left = nruns;
            unsigned_type cur_out_run = 0;
            unsigned_type elements_in_new_run = 0;
            //unsigned_type blocks_in_new_run = 0;


            while (runs_left > 0)
            {
                int_type runs2merge = STXXL_MIN(runs_left, merge_factor);
                //blocks_in_new_run = 0 ;
                elements_in_new_run = 0;
                for (unsigned_type i = nruns - runs_left; i < (nruns - runs_left + runs2merge); ++i)
                {
                    elements_in_new_run += sruns.runs_sizes[i];
                    //blocks_in_new_run += sruns.runs[i].size();
                }
                const unsigned_type blocks_in_new_run1 = div_and_round_up(elements_in_new_run, block_type::size);
                //assert(blocks_in_new_run1 == blocks_in_new_run);

                new_runs.runs_sizes[cur_out_run] = elements_in_new_run;
                // allocate run
                new_runs.runs[cur_out_run++].resize(blocks_in_new_run1);
                runs_left -= runs2merge;
            }

            // allocate blocks for the new runs
            for (unsigned_type i = 0; i < new_runs.runs.size(); ++i)
                bm->new_blocks( alloc_strategy(),
                                trigger_entry_iterator < typename run_type::iterator, block_type::raw_size > (new_runs.runs[i].begin()),
                                trigger_entry_iterator < typename run_type::iterator, block_type::raw_size > (new_runs.runs[i].end()) );


            // merge all
            runs_left = nruns;
            cur_out_run = 0;
            size_type elements_left = sruns.elements;

            while (runs_left > 0)
            {
                unsigned_type runs2merge = STXXL_MIN(runs_left, merge_factor);
                STXXL_VERBOSE("Merging " << runs2merge << " runs");

                sorted_runs_type cur_runs;
                cur_runs.runs.resize(runs2merge);
                cur_runs.runs_sizes.resize(runs2merge);

                std::copy(sruns.runs.begin() + nruns - runs_left,
                          sruns.runs.begin() + nruns - runs_left + runs2merge,
                          cur_runs.runs.begin() );
                std::copy(sruns.runs_sizes.begin() + nruns - runs_left,
                          sruns.runs_sizes.begin() + nruns - runs_left + runs2merge,
                          cur_runs.runs_sizes.begin() );

                runs_left -= runs2merge;
                /*
                   cur_runs.elements = (runs_left)?
                   (new_runs.runs[cur_out_run].size()*block_type::size):
                   (elements_left);
                 */
                cur_runs.elements = new_runs.runs_sizes[cur_out_run];
                elements_left -= cur_runs.elements;

                if (runs2merge > 1)
                {
                    basic_runs_merger<RunsType_, Cmp_, AllocStr_> merger(cur_runs, cmp, m_ * block_type::raw_size);

                    { // make sure everything is being destroyed in right time
                        buf_ostream < block_type, typename run_type::iterator > out(
                            new_runs.runs[cur_out_run].begin(),
                            nwrite_buffers );

                        blocked_index<block_type::size> cnt = 0;
                        const unsigned_type cnt_max = cur_runs.elements;

                        while (cnt != cnt_max)
                        {
                            *out = *merger;
                            if ( (cnt.get_offset()) == 0)  // have to write the trigger value
                                new_runs.runs[cur_out_run][cnt.get_block()].value = *merger;


                            ++cnt;
                            ++out;
                            ++merger;
                        }
                        assert(merger.empty());

                        while (cnt.get_offset())
                        {
                            *out = cmp.max_value();
                            ++out;
                            ++cnt;
                        }
                    }
                }
                else
                {
                    bm->delete_blocks(
                        trigger_entry_iterator < typename run_type::iterator, block_type::raw_size > (
                            new_runs.runs.back().begin()),
                        trigger_entry_iterator < typename run_type::iterator, block_type::raw_size > (
                            new_runs.runs.back().end()) );

                    assert(cur_runs.runs.size() == 1);

                    std::copy(cur_runs.runs.front().begin(),
                              cur_runs.runs.front().end(),
                              new_runs.runs.back().begin());
                    new_runs.runs_sizes.back() = cur_runs.runs_sizes.back();
                }

                ++cur_out_run;
            }
            assert(elements_left == 0);

            nruns = new_nruns;
            sruns = new_runs;
        }
    }


    //! \brief Merges sorted runs
    //!
    //! Template parameters:
    //! - \c RunsType_ type of the sorted runs, available as \c runs_creator::sorted_runs_type ,
    //! - \c Cmp_ type of comparison object used for merging
    //! - \c AllocStr_ allocation strategy used to allocate the blocks for
    //! storing intermediate results if several merge passes are required
    template <  class RunsCreator_,
              class Cmp_,
              class AllocStr_ = STXXL_DEFAULT_ALLOC_STRATEGY >
    class runs_merger : public basic_runs_merger<typename RunsCreator_::sorted_runs_type, Cmp_, AllocStr_>
    {
    private:
        typedef basic_runs_merger<typename RunsCreator_::sorted_runs_type, Cmp_, AllocStr_> base;
        typedef typename base::value_cmp value_cmp;
        typedef typename base::block_type block_type;

        RunsCreator_& rc;

    public:
        //! \brief Creates a runs merger object
        //! \param r input sorted runs object
        //! \param c comparison object
        //! \param memory_to_use amount of memory available for the merger in bytes
        runs_merger(RunsCreator_& rc, value_cmp c, unsigned_type memory_to_use) :
            base(c, memory_to_use),
            rc(rc)
        {
#if !STXXL_START_PIPELINE
            base::initialize(rc.result());
#endif
        }

        void start()
        {
            STXXL_VERBOSE0("runs_merger " << this << " starts.");
            base::initialize(rc.result());
        }
    };

    //! \brief Produces sorted stream from input stream
    //!
    //! Template parameters:
    //! - \c Input_ type of the input stream
    //! - \c Cmp_ type of comparison object used for sorting the runs
    //! - \c BlockSize_ size of blocks used to store the runs
    //! - \c AllocStr_ functor that defines allocation strategy for the runs
    //! \remark Implemented as the composition of \c runs_creator and \c runs_merger .
    template <  class Input_,
              class Cmp_,
              unsigned BlockSize_ = STXXL_DEFAULT_BLOCK_SIZE (typename Input_::value_type),
              class AllocStr_ = STXXL_DEFAULT_ALLOC_STRATEGY,
              class runs_creator_type = runs_creator<Input_, Cmp_, BlockSize_, AllocStr_> >
    class sort
    {
        typedef typename runs_creator_type::sorted_runs_type sorted_runs_type;
        typedef runs_merger<runs_creator_type, Cmp_, AllocStr_> runs_merger_type;

        runs_creator_type creator;
        runs_merger_type merger;
        Cmp_& c;
        unsigned_type memory_to_use_m;
        Input_& input;

        sort(); // forbidden
        sort(const sort &); // forbidden
    public:
        //! \brief Standard stream typedef
        typedef typename Input_::value_type value_type;
        typedef typename runs_merger_type::const_iterator const_iterator;

        typedef sorted_runs_type result_type;

        //! \brief Creates the object
        //! \param in input stream
        //! \param c comparator object
        //! \param memory_to_use memory amount that is allowed to used by the sorter in bytes
        sort(Input_ & in, Cmp_ c, unsigned_type memory_to_use) :
            creator(in, c, memory_to_use),
            merger(creator, c, memory_to_use),
            c(c),
            memory_to_use_m(memory_to_use),
            input(in)
        {
        }

        //! \brief Creates the object
        //! \param in input stream
        //! \param c comparator object
        //! \param memory_to_use_rc memory amount that is allowed to used by the runs creator in bytes
        //! \param memory_to_use_m memory amount that is allowed to used by the merger in bytes
        sort(Input_ & in, Cmp_ c, unsigned_type memory_to_use_rc, unsigned_type memory_to_use_m) :
            creator(in, c, memory_to_use_rc),
            merger(creator, c, memory_to_use_m),
            c(c),
            memory_to_use_m(memory_to_use_m)
        {
        }

        //! \brief Standard stream method
        void start()
        {
            STXXL_VERBOSE0("sort " << this << " starts.");
            merger.start();
        }

        //! \brief Standard stream method
        const value_type & operator * () const
        {
            assert(!empty());
            return *merger;
        }

        const value_type * operator -> () const
        {
            assert(!empty());
            return merger.operator->();
        }

        //! \brief Standard stream method
        sort & operator ++()
        {
            ++merger;
            return *this;
        }

        //! \brief Standard stream method
        bool empty() const
        {
            return merger.empty();
        }

        //! \brief Batched stream method.
        unsigned_type batch_length()
        {
          return merger.batch_length();
        }

        //! \brief Batched stream method.
        sort& operator += (unsigned_type size)
        {
                merger += size;
                return *this;
        }

        //! \brief Batched stream method.
        const_iterator batch_begin()
        {       return merger.batch_begin();
        }

        //! \brief Batched stream method.
        const value_type& operator[](unsigned_type index)
        {
                return merger[index];
        }

    };

    //! \brief Computes sorted runs type from value type and block size
    //!
    //! Template parameters
    //! - \c ValueType_ type of values ins sorted runs
    //! - \c BlockSize_ size of blocks where sorted runs stored
    template <
              class ValueType_,
              unsigned BlockSize_>
    class compute_sorted_runs_type
    {
        typedef ValueType_ value_type;
        typedef BID<BlockSize_> bid_type;
        typedef sort_local::trigger_entry<bid_type, value_type> trigger_entry_type;
    public:
        typedef sorted_runs<value_type, trigger_entry_type> result;
    };

//! \}
}

//! \addtogroup stlalgo
//! \{

//! \brief Sorts range of any random access iterators externally

//! \param begin iterator pointing to the first element of the range
//! \param end iterator pointing to the last+1 element of the range
//! \param cmp comparison object
//! \param MemSize memory to use for sorting (in bytes)
//! \param AS allocation strategy
//!
//! The \c BlockSize template parameter defines the block size to use (in bytes)
//! \warning Slower than External Iterator Sort
template <      unsigned BlockSize,
          class RandomAccessIterator,
          class CmpType,
          class AllocStr>
void sort(RandomAccessIterator begin,
          RandomAccessIterator end,
          CmpType cmp,
          unsigned_type MemSize,
          AllocStr AS)
{
    UNUSED(AS);
#ifdef BOOST_MSVC
    typedef typename streamify_traits<RandomAccessIterator>::stream_type InputType;
#else
    typedef typeof(stream::streamify(begin, end)) InputType;
#endif
    InputType Input(begin, end);
    typedef stream::sort<InputType, CmpType, BlockSize, AllocStr> sorter_type;
    sorter_type Sort(Input, cmp, MemSize);
    stream::materialize(Sort, begin);
}

//! \}

__STXXL_END_NAMESPACE

#endif
