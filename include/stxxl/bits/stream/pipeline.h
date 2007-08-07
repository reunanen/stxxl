/***************************************************************************
 *   Copyright (C) 2007 by Johannes Singler                                *
 *   singler@ira.uka.de                                                    *
 *   Distributed under the Boost Software License, Version 1.0.            *
 *   (See accompanying file LICENSE_1_0.txt or copy at                     *
 *   http://www.boost.org/LICENSE_1_0.txt)                                 *
 *   Part of the STXXL http://stxxl.sourceforge.net                        *
 ***************************************************************************/

//! \file pipeline.h 
//! \brief Pipeline functionality for asynchronous computation.

#ifndef PIPELINE_HEADER
#define PIPELINE_HEADER

#include "stxxl/bits/stream/stream.h"

__STXXL_BEGIN_NAMESPACE

namespace stream
{

//! \brief Sub-namespace for providing parallel pipelined stream processing.
namespace pipeline
{

//! \addtogroup streampack Stream package
//! \{

//! \addtogroup pipelinepack Pipeline package
//! \{

//! \brief Helper class encapsuling a buffer.
template<typename value_type>
class buffer
{
private:
	buffer(const buffer&) { }
	buffer& operator=(const buffer&) { return *this; }

public:
	//! \brief Begin iterator of the buffer.
	value_type* begin;
	//! \brief End iterator of the buffer.
	value_type* end;
	//! \brief In case the buffer is not full, stop differs from end.
	value_type* stop;
	//! \brief Currrent read or write position.
	value_type* current;
	
	//! \brief Constructor.
	//! \param byte_size Size of the buffer in number of bytes.
	buffer(unsigned_type byte_size)
	{
		unsigned_type size = byte_size / sizeof(value_type);
		assert(size > 0);
		begin = new value_type[size];
		stop = end = begin + size;
	}
	
	//! \brief Destructor.
	~buffer()
	{
		delete[] begin;
	}
};

//! \brief Asynchronous stage wrapper to allow concurrent pipelining.
//!
//! This wrapper is for regular pipeline stages, for which the stream operations are called.
template<class StreamOperation>
class pull_stage : private StreamOperation
{
public:
	typedef typename StreamOperation::value_type value_type;
	typedef buffer<value_type> buffer;
	
private:
	//! \brief First double buffering buffer.
	buffer block1;
	//! \brief Second double buffering buffer.
	buffer block2;
	//! \brief Buffer that is currently input to.
	mutable buffer* incoming_buffer;
	//! \brief Buffer that is currently output from.
	mutable buffer* outgoing_buffer;
	//! \brief The incoming buffer has been filled (completely, or the input stream has run empty).
	mutable volatile bool input_buffer_filled;
	//! \brief The outgoing buffer has been consumed.
	mutable volatile bool output_buffer_consumed;
	//! \brief The input stream has run empty, the last swap_buffers() has been performed already.
	mutable volatile bool last_swap_done;
	//! \brief Mutex variable, to mutual exclude the other thread.
	mutable pthread_mutex_t mutex;
	//! \brief Condition variable, to wait for the other thread.
	mutable pthread_cond_t cond;
	//! \brief Asynchronously pulling thread.
	pthread_t puller;

	//! \brief Initialize object. Common code for all constructor variants.
	void init()
	{
		incoming_buffer->current = incoming_buffer->begin;
		input_buffer_filled = false;
		
		outgoing_buffer->current = outgoing_buffer->end;
		output_buffer_consumed = true;
		
		last_swap_done = StreamOperation::empty();
		
		pthread_mutex_init(&mutex, 0);
		pthread_cond_init(&cond, 0);
		
		start_pulling();
	}

public:
	//! \brief Generic Constructor for zero passed arguments.
	//! \param buffer_size Total size of the buffers in bytes.
	pull_stage(unsigned_type buffer_size) :
		StreamOperation(),
		block1(buffer_size / 2), block2(buffer_size / 2), incoming_buffer(&block1), outgoing_buffer(&block2)
	{
		init();
	}
	
	//! \brief Generic Constructor for one passed argument.
	//! \param buffer_size Total size of the buffers in bytes.
	//! \param t1 First constructor parameter, passed by reference.
	template<typename T1>
	pull_stage(unsigned_type buffer_size, T1& t1) :
		StreamOperation(t1),
		block1(buffer_size / 2), block2(buffer_size / 2), incoming_buffer(&block1), outgoing_buffer(&block2)
	{
		init();
	}
	
	//! \brief Generic Constructor for two passed arguments.
	//! \param buffer_size Total size of the buffers in bytes.
	//! \param t1 First constructor parameter, passed by reference.
	//! \param t2 Second constructor parameter, passed by reference.
	template<typename T1, typename T2>
	pull_stage(unsigned_type buffer_size, T1& t1, T2& t2) :
		StreamOperation(t1, t2),
		block1(buffer_size / 2), block2(buffer_size / 2), incoming_buffer(&block1), outgoing_buffer(&block2)
	{
		init();
	}
	
	//! \brief Generic Constructor for three passed arguments.
	//! \param buffer_size Total size of the buffers in bytes.
	//! \param t1 First constructor parameter, passed by reference.
	//! \param t2 Second constructor parameter, passed by reference.
	//! \param t3 Third constructor parameter, passed by reference.
	template<typename T1, typename T2, typename T3>
	pull_stage(unsigned_type buffer_size, T1& t1, T2& t2, T3& t3) :
		StreamOperation(t1, t2, t3),
		block1(buffer_size / 2), block2(buffer_size / 2), incoming_buffer(&block1), outgoing_buffer(&block2)
	{
		init();
	}
	
	//! \brief Generic Constructor for four passed arguments.
	//! \param buffer_size Total size of the buffers in bytes.
	//! \param t1 First constructor parameter, passed by reference.
	//! \param t2 Second constructor parameter, passed by reference.
	//! \param t3 Third constructor parameter, passed by reference.
	//! \param t4 Fourth constructor parameter, passed by reference.
	template<typename T1, typename T2, typename T3, typename T4>
	pull_stage(unsigned_type buffer_size, T1& t1, T2& t2, T3& t3, T4& t4) :
		StreamOperation(t1, t2, t3, t4),
		block1(buffer_size / 2), block2(buffer_size / 2), incoming_buffer(&block1), outgoing_buffer(&block2)
	{
		init();
	}
	
	//! \brief Generic Constructor for five passed arguments.
	//! \param buffer_size Total size of the buffers in bytes.
	//! \param t1 First constructor parameter, passed by reference.
	//! \param t2 Second constructor parameter, passed by reference.
	//! \param t3 Third constructor parameter, passed by reference.
	//! \param t4 Fourth constructor parameter, passed by reference.
	//! \param t5 Fifth constructor parameter, passed by reference.
	template<typename T1, typename T2, typename T3, typename T4, typename T5>
	pull_stage(unsigned_type buffer_size, T1& t1, T2& t2, T3& t3, T4& t4, T5& t5) :
		StreamOperation(t1, t2, t3, t4, t5),
		block1(buffer_size / 2), block2(buffer_size / 2), incoming_buffer(&block1), outgoing_buffer(&block2)
	{
		init();
	}
	
	//! \brief Generic Constructor for six passed arguments.
	//! \param buffer_size Total size of the buffers in bytes.
	//! \param t1 First constructor parameter, passed by reference.
	//! \param t2 Second constructor parameter, passed by reference.
	//! \param t3 Third constructor parameter, passed by reference.
	//! \param t4 Fourth constructor parameter, passed by reference.
	//! \param t5 Fifth constructor parameter, passed by reference.
	//! \param t6 Sixth constructor parameter, passed by reference.
	template<typename T1, typename T2, typename T3, typename T4, typename T5, typename T6>
	pull_stage(unsigned_type buffer_size, T1& t1, T2& t2, T3& t3, T4& t4, T5& t5, T6& t6) :
		StreamOperation(t1, t2, t3, t4, t5, t6),
		block1(buffer_size / 2), block2(buffer_size / 2), incoming_buffer(&block1), outgoing_buffer(&block2)
	{
		init();
	}
	
	//! \brief Destructor.
	~pull_stage()
	{
		pthread_mutex_destroy(&mutex);
		pthread_cond_destroy(&cond);
	}

private:
	//! \brief Swap input and output buffers.
	void swap_buffers() const
	{
		assert(output_buffer_consumed);
		assert(input_buffer_filled);
	
		std::swap(incoming_buffer, outgoing_buffer);

		incoming_buffer->current = incoming_buffer->begin;
		outgoing_buffer->current = outgoing_buffer->begin;
		
		input_buffer_filled = false;
		output_buffer_consumed = false;
		
		last_swap_done = StreamOperation::empty();
	}
	
	//! \brief Check whether outgoing buffer has run empty and possibly wait for new data to come in.
	//!
	//! Should not be called in operator++(), but in empty() (or operator*()),
	//! because should not block if iterator is only advanced, but not accessed.
	void reload() const
	{
		if(outgoing_buffer->current >= outgoing_buffer->stop)
		{
			pthread_mutex_lock(&mutex);
			output_buffer_consumed = true;
			
			if(input_buffer_filled)
			{
				swap_buffers();
				
				pthread_mutex_unlock(&mutex);
				pthread_cond_signal(&cond);	//wake up other thread
			}
			else if(!last_swap_done)
			{
				while(output_buffer_consumed)	//to be swapped by other thread
					pthread_cond_wait(&cond, &mutex);
				pthread_mutex_unlock(&mutex);
			}
			else	//no further input, empty will return true
				pthread_mutex_unlock(&mutex);
		}
	}	

public:
	//! \brief Standard stream method.
	const value_type& operator * () const
	{
		return *outgoing_buffer->current;
	}
	
	//! \brief Standard stream method.
	const value_type* operator -> () const
	{
		return &(operator*());
	}
	
	//! \brief Standard stream method.
	pull_stage<StreamOperation>& operator ++ ()
	{
		++outgoing_buffer->current;
		return *this;
	}
	
	//! \brief Standard stream method.
	bool empty() const
	{
		reload();
	
		return last_swap_done && output_buffer_consumed;
	}
	
	//! \brief Asynchronous method that keeps trying to fill the incoming buffer.
	void pull()
	{
		while(!StreamOperation::empty())
		{
			while(incoming_buffer->current < incoming_buffer->end && !StreamOperation::empty())
			{
				*incoming_buffer->current = StreamOperation::operator*();
				++incoming_buffer->current;
				StreamOperation::operator++();
			}
			incoming_buffer->stop = incoming_buffer->current;
			
			pthread_mutex_lock(&mutex);
			input_buffer_filled = true;
			
			if(output_buffer_consumed)
			{
				swap_buffers();
				
				pthread_mutex_unlock(&mutex);
				pthread_cond_signal(&cond);	//wake up other thread
			}
			else
			{
				while(input_buffer_filled)	//to be swapped by other thread
					pthread_cond_wait(&cond, &mutex);
				pthread_mutex_unlock(&mutex);
			}
		}
	}
	
	void start_pulling();
};

//! \brief Helper function to call pull_stage::pull() in a thread.
template<class StreamOperation>
void* call_pull(void* param)
{
	static_cast<pull_stage<StreamOperation>*>(param)->pull();
	return NULL;
}

//! \brief Start pulling data asynchronously.
template<class StreamOperation>
void pull_stage<StreamOperation>::start_pulling()
{
	pthread_create(&puller, NULL, call_pull<StreamOperation>, this);
}


//! \brief Asynchronous stage wrapper to allow concurrent pipelining.
//!
//! This wrapper is for stages which are pushed into, i. e. push() and result() are called.
template<class StreamOperation>
class push_stage : private StreamOperation
{
public:
	typedef typename StreamOperation::value_type value_type;
	typedef buffer<value_type> buffer;
	typedef typename StreamOperation::result_type result_type;
	
private:
	//! \brief First double buffering buffer.
	buffer block1;
	//! \brief Second double buffering buffer.
	buffer block2;
	//! \brief Buffer that is currently input to.
	mutable buffer* incoming_buffer;
	//! \brief Buffer that is currently output from.
	mutable buffer* outgoing_buffer;
	//! \brief The incoming buffer has been filled (completely, or the input stream has run empty).
	mutable volatile bool input_buffer_filled;
	//! \brief The outgoing buffer has been consumed.
	mutable volatile bool output_buffer_consumed;
	//! \brief The input had finished, the last swap_buffers() has been performed already.
	mutable volatile bool last_swap_done;
	//! \brief The input is finished.
	mutable volatile bool input_finished;
	//! \brief Mutex variable, to mutual exclude the other thread.
	mutable pthread_mutex_t mutex;
	//! \brief Condition variable, to wait for the other thread.
	mutable pthread_cond_t cond;
	//! \brief Asynchronously pushing thread.
	pthread_t pusher;

	//! \brief Initialize object. Common code for all constructor variants.
	void init()
	{
		incoming_buffer->current = incoming_buffer->begin;
		input_buffer_filled = false;
		
		outgoing_buffer->current = outgoing_buffer->end;
		output_buffer_consumed = true;
		
		input_finished = false;
		last_swap_done = input_finished;
		
		pthread_mutex_init(&mutex, 0);
		pthread_cond_init(&cond, 0);
		
		start_pushing();
	}

public:
	//! \brief Generic Constructor for zero passed arguments.
	//! \param buffer_size Total size of the buffers in bytes.
	push_stage(unsigned_type buffer_size) :
		StreamOperation(),
		block1(buffer_size / 2), block2(buffer_size / 2), incoming_buffer(&block1), outgoing_buffer(&block2)
	{
		init();
	}
	
	//! \brief Generic Constructor for one passed argument.
	//! \param buffer_size Total size of the buffers in bytes.
	//! \param t1 First constructor parameter, passed by reference.
	template<typename T1>
	push_stage(unsigned_type buffer_size, T1& t1) :
		StreamOperation(t1),
		block1(buffer_size / 2), block2(buffer_size / 2), incoming_buffer(&block1), outgoing_buffer(&block2)
	{
		init();
	}
	
	//! \brief Generic Constructor for two passed arguments.
	//! \param buffer_size Total size of the buffers in bytes.
	//! \param t1 First constructor parameter, passed by reference.
	//! \param t2 Second constructor parameter, passed by reference.
	template<typename T1, typename T2>
	push_stage(unsigned_type buffer_size, T1& t1, T2& t2) :
		StreamOperation(t1, t2),
		block1(buffer_size / 2), block2(buffer_size / 2), incoming_buffer(&block1), outgoing_buffer(&block2)
	{
		init();
	}
	
	//! \brief Generic Constructor for three passed arguments.
	//! \param buffer_size Total size of the buffers in bytes.
	//! \param t1 First constructor parameter, passed by reference.
	//! \param t2 Second constructor parameter, passed by reference.
	//! \param t3 Third constructor parameter, passed by reference.
	template<typename T1, typename T2, typename T3>
	push_stage(unsigned_type buffer_size, T1& t1, T2& t2, T3& t3) :
		StreamOperation(t1, t2, t3),
		block1(buffer_size / 2), block2(buffer_size / 2), incoming_buffer(&block1), outgoing_buffer(&block2)
	{
		init();
	}
	
	//! \brief Generic Constructor for four passed arguments.
	//! \param buffer_size Total size of the buffers in bytes.
	//! \param t1 First constructor parameter, passed by reference.
	//! \param t2 Second constructor parameter, passed by reference.
	//! \param t3 Third constructor parameter, passed by reference.
	//! \param t4 Fourth constructor parameter, passed by reference.
	template<typename T1, typename T2, typename T3, typename T4>
	push_stage(unsigned_type buffer_size, T1& t1, T2& t2, T3& t3, T4& t4) :
		StreamOperation(t1, t2, t3, t4),
		block1(buffer_size / 2), block2(buffer_size / 2), incoming_buffer(&block1), outgoing_buffer(&block2)
	{
		init();
	}
	
	//! \brief Generic Constructor for five passed arguments.
	//! \param buffer_size Total size of the buffers in bytes.
	//! \param t1 First constructor parameter, passed by reference.
	//! \param t2 Second constructor parameter, passed by reference.
	//! \param t3 Third constructor parameter, passed by reference.
	//! \param t4 Fourth constructor parameter, passed by reference.
	//! \param t5 Fifth constructor parameter, passed by reference.
	template<typename T1, typename T2, typename T3, typename T4, typename T5>
	push_stage(unsigned_type buffer_size, T1& t1, T2& t2, T3& t3, T4& t4, T5& t5) :
		StreamOperation(t1, t2, t3, t4, t5),
		block1(buffer_size / 2), block2(buffer_size / 2), incoming_buffer(&block1), outgoing_buffer(&block2)
	{
		init();
	}
	
	//! \brief Generic Constructor for six passed arguments.
	//! \param buffer_size Total size of the buffers in bytes.
	//! \param t1 First constructor parameter, passed by reference.
	//! \param t2 Second constructor parameter, passed by reference.
	//! \param t3 Third constructor parameter, passed by reference.
	//! \param t4 Fourth constructor parameter, passed by reference.
	//! \param t5 Fifth constructor parameter, passed by reference.
	//! \param t6 Sixth constructor parameter, passed by reference.
	template<typename T1, typename T2, typename T3, typename T4, typename T5, typename T6>
	push_stage(unsigned_type buffer_size, T1& t1, T2& t2, T3& t3, T4& t4, T5& t5, T6& t6) :
		StreamOperation(t1, t2, t3, t4, t5, t6),
		block1(buffer_size / 2), block2(buffer_size / 2), incoming_buffer(&block1), outgoing_buffer(&block2)
	{
		init();
	}
	
	//! \brief Destructor.
	~push_stage()
	{
		pthread_mutex_destroy(&mutex);
		pthread_cond_destroy(&cond);
	}

private:
	//! \brief Swap input and output buffers.
	void swap_buffers() const
	{
		assert(output_buffer_consumed);
		assert(input_buffer_filled);
	
		std::swap(incoming_buffer, outgoing_buffer);

		incoming_buffer->current = incoming_buffer->begin;
		outgoing_buffer->current = outgoing_buffer->begin;
		
		input_buffer_filled = false;
		output_buffer_consumed = false;
		
		last_swap_done = input_finished;
	}
	
	//! \brief Check whether incoming buffer has run full and possibly wait for data being pushed forward.
	void unload() const
	{
		if(incoming_buffer->current >= incoming_buffer->end || input_finished)
		{
			incoming_buffer->stop = incoming_buffer->current;
		
			pthread_mutex_lock(&mutex);
			input_buffer_filled = true;
			
			if(output_buffer_consumed)
			{
				swap_buffers();
				
				pthread_mutex_unlock(&mutex);
				pthread_cond_signal(&cond);	//wake up other thread
			}
			else
			{
				while(input_buffer_filled)	//to be swapped by other thread
					pthread_cond_wait(&cond, &mutex);
				pthread_mutex_unlock(&mutex);
			}
		}
	}	

public:
	//! \brief Standard push stream method.
	//! \param val value to be pushed
	void push(const value_type & val)
	{
		*incoming_buffer->current = val;
		++incoming_buffer->current;
		
		unload();
	}
	
	//! \brief Returns result.
	const typename StreamOperation::result_type& result()
	{
		if(!input_finished)
		{
			input_finished = true;
			
			unload();
			
			void* return_code;
			pthread_join(pusher, &return_code);
		}
		
		return StreamOperation::result();
	}

	//! \brief Asynchronous method that keeps trying to push from the outgoing buffer.
	void push()
	{
		while(true)
		{
			while(outgoing_buffer->current < outgoing_buffer->stop)
			{
				StreamOperation::push(*outgoing_buffer->current);
				++outgoing_buffer->current;
			}
			
			if(last_swap_done)
				break;
			
			pthread_mutex_lock(&mutex);
			output_buffer_consumed = true;
			
			if(input_buffer_filled)
			{
				swap_buffers();
				
				pthread_mutex_unlock(&mutex);
				pthread_cond_signal(&cond);	//wake up other thread
			}
			else
			{
				while(output_buffer_consumed && !last_swap_done)	//to be swapped by other thread
					pthread_cond_wait(&cond, &mutex);
				pthread_mutex_unlock(&mutex);
			}
		}
	}

	void start_pushing();
};

//! \brief Helper function to call push_stage::push() in a Pthread thread.
template<class StreamOperation>
void* call_push(void* param)
{
	static_cast<push_stage<StreamOperation>*>(param)->push();
	return NULL;
}

//! \brief Start pushing data asynchronously.
template<class StreamOperation>
void push_stage<StreamOperation>::start_pushing()
{
	pthread_create(&pusher, NULL, call_push<StreamOperation>, this);
}

//! \brief Dummy stage wrapper switch of pipelining by a define.
template<class StreamOperation>
class dummy_stage : public StreamOperation
{
public:
	typedef typename StreamOperation::value_type value_type;
	
public:
	//! \brief Generic Constructor for zero passed arguments.
	//! \param buffer_size Total size of the buffers in bytes.
	dummy_stage(unsigned_type buffer_size) :
		StreamOperation()
	{
	}
	
	//! \brief Generic Constructor for one passed argument.
	//! \param buffer_size Total size of the buffers in bytes.
	//! \param t1 First constructor parameter, passed by reference.
	template<typename T1>
	dummy_stage(unsigned_type buffer_size, T1& t1) :
		StreamOperation(t1)
	{
	}
	
	//! \brief Generic Constructor for two passed arguments.
	//! \param buffer_size Total size of the buffers in bytes.
	//! \param t1 First constructor parameter, passed by reference.
	//! \param t2 Second constructor parameter, passed by reference.
	template<typename T1, typename T2>
	dummy_stage(unsigned_type buffer_size, T1& t1, T2& t2) :
		StreamOperation(t1, t2)
	{
	}
	
	//! \brief Generic Constructor for three passed arguments.
	//! \param buffer_size Total size of the buffers in bytes.
	//! \param t1 First constructor parameter, passed by reference.
	//! \param t2 Second constructor parameter, passed by reference.
	//! \param t3 Third constructor parameter, passed by reference.
	template<typename T1, typename T2, typename T3>
	dummy_stage(unsigned_type buffer_size, T1& t1, T2& t2, T3& t3) :
		StreamOperation(t1, t2, t3)
	{
	}
	
	//! \brief Generic Constructor for three passed arguments.
	//! \param buffer_size Total size of the buffers in bytes.
	//! \param t1 First constructor parameter, passed by reference.
	//! \param t2 Second constructor parameter, passed by reference.
	//! \param t3 Third constructor parameter, passed by reference.
	//! \param t4 Fourth constructor parameter, passed by reference.
	template<typename T1, typename T2, typename T3, typename T4>
	dummy_stage(unsigned_type buffer_size, T1& t1, T2& t2, T3& t3, T4& t4) :
		StreamOperation(t1, t2, t3, t4)
	{
	}
	
	//! \brief Generic Constructor for three passed arguments.
	//! \param buffer_size Total size of the buffers in bytes.
	//! \param t1 First constructor parameter, passed by reference.
	//! \param t2 Second constructor parameter, passed by reference.
	//! \param t3 Third constructor parameter, passed by reference.
	//! \param t4 Fourth constructor parameter, passed by reference.
	//! \param t5 Fifth constructor parameter, passed by reference.
	template<typename T1, typename T2, typename T3, typename T4, typename T5>
	dummy_stage(unsigned_type buffer_size, T1& t1, T2& t2, T3& t3, T4& t4, T5& t5) :
		StreamOperation(t1, t2, t3, t4, t5)
	{
	}
	
	//! \brief Generic Constructor for three passed arguments.
	//! \param buffer_size Total size of the buffers in bytes.
	//! \param t1 First constructor parameter, passed by reference.
	//! \param t2 Second constructor parameter, passed by reference.
	//! \param t3 Third constructor parameter, passed by reference.
	//! \param t4 Fourth constructor parameter, passed by reference.
	//! \param t5 Fifth constructor parameter, passed by reference.
	//! \param t6 Sixth constructor parameter, passed by reference.
	template<typename T1, typename T2, typename T3, typename T4, typename T5, typename T6>
	dummy_stage(unsigned_type buffer_size, T1& t1, T2& t2, T3& t3, T4& t4, T5& t5, T6& t6) :
		StreamOperation(t1, t2, t3, t4, t5, t6)
	{
	}
};

//! \}

//! \}

}	//namespace pipeline

}	//namespace stream

__STXXL_END_NAMESPACE

#endif
