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

//! \brief Sub-namespace for providing pipelined stream processing.
namespace pipeline
{

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
	//! \param size Size of the buffer in number of elements.
	buffer(unsigned_type size)
	{
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
template<class StreamOperation>
class stage : private StreamOperation
{
public:
	typedef typename StreamOperation::value_type value_type;
	typedef buffer<value_type> buffer;
	
private:
	//! \brief First double buffering buffer.
	buffer block1;
	//! \brief Second double buffering buffer.
	buffer block2;
	//! \brief Buffer that is currently input from.
	buffer* input_buffer;
	//! \brief Buffer that is currently output to.
	buffer* output_buffer;
	//! \brief The input buffer has been filled (completely, or the input stream has run empty).
	volatile bool input_buffer_filled;
	//! \brief The output buffer has been consumed.
	volatile bool output_buffer_consumed;
	//! \brief The input stream has run empty, the last swap_buffers() has been performed already.
	volatile bool last_swap_done;
	//! \brief Mutex variable, to mutual exclude the other thread.
	pthread_mutex_t mutex;
	//! \brief Condition variable, to wait for the other thread.
	pthread_cond_t cond;
	//! \brief Asynchronously pulling thread.
	pthread_t puller;

	//! \brief Initialize object. Common code for all constructor variants.
	void init()
	{
		input_buffer->current = input_buffer->begin;
		input_buffer_filled = false;
		
		output_buffer->current = input_buffer->end;
		output_buffer_consumed = true;
		
		last_swap_done = false;
		
		pthread_mutex_init(&mutex, 0);
		pthread_cond_init(&cond, 0);
		
		start_pulling();
	}

public:
	//! \brief Generic Constructor for zero passed arguments.
	//! \param buffer_size Size of each of the two buffers in number of elements. The total consumed memory will be \c 2*buffer_size*sizeof(value_type).
	stage(unsigned_type buffer_size) :
		StreamOperation(),
		block1(buffer_size), block2(buffer_size), input_buffer(&block1), output_buffer(&block2)
	{
		init();
	}
	
	//! \brief Generic Constructor for one passed argument.
	//! \param buffer_size Size of each of the two buffers in number of elements. The total consumed memory will be \c 2*buffer_size*sizeof(value_type).
	template<typename T1>
	stage(unsigned_type buffer_size, T1& t1) :
		StreamOperation(t1),
		block1(buffer_size), block2(buffer_size), input_buffer(&block1), output_buffer(&block2)
	{
		init();
	}
	
	//! \brief Generic Constructor for two passed arguments.
	//! \param buffer_size Size of each of the two buffers in number of elements. The total consumed memory will be \c 2*buffer_size*sizeof(value_type).
	template<typename T1, typename T2>
	stage(unsigned_type buffer_size, T1& t1, T2& t2) :
		StreamOperation(t1, t2),
		block1(buffer_size), block2(buffer_size), input_buffer(&block1), output_buffer(&block2)
	{
		init();
	}
	
	//! \brief Generic Constructor for three passed arguments.
	//! \param buffer_size Size of each of the two buffers in number of elements. The total consumed memory will be \c 2*buffer_size*sizeof(value_type).
	template<typename T1, typename T2, typename T3>
	stage(unsigned_type buffer_size, T1& t1, T2& t2, T3& t3) :
		StreamOperation(t1, t2, t3),
		block1(buffer_size), block2(buffer_size), input_buffer(&block1), output_buffer(&block2)
	{
		init();
	}
	
	//! \brief Generic Constructor for three passed arguments.
	//! \param buffer_size Size of each of the two buffers in number of elements. The total consumed memory will be \c 2*buffer_size*sizeof(value_type).
	template<typename T1, typename T2, typename T3, typename T4>
	stage(unsigned_type buffer_size, T1& t1, T2& t2, T3& t3, T4& t4) :
		StreamOperation(t1, t2, t3, t4),
		block1(buffer_size), block2(buffer_size), input_buffer(&block1), output_buffer(&block2)
	{
		init();
	}
	
	//! \brief Generic Constructor for three passed arguments.
	//! \param buffer_size Size of each of the two buffers in number of elements. The total consumed memory will be \c 2*buffer_size*sizeof(value_type).
	template<typename T1, typename T2, typename T3, typename T4, typename T5>
	stage(unsigned_type buffer_size, T1& t1, T2& t2, T3& t3, T4& t4, T5& t5) :
		StreamOperation(t1, t2, t3, t4, t5),
		block1(buffer_size), block2(buffer_size), input_buffer(&block1), output_buffer(&block2)
	{
		init();
	}
	
	//! \brief Generic Constructor for three passed arguments.
	//! \param buffer_size Size of each of the two buffers in number of elements. The total consumed memory will be \c 2*buffer_size*sizeof(value_type).
	template<typename T1, typename T2, typename T3, typename T4, typename T5, typename T6>
	stage(unsigned_type buffer_size, T1& t1, T2& t2, T3& t3, T4& t4, T5& t5, T6& t6) :
		StreamOperation(t1, t2, t3, t4, t5, t6),
		block1(buffer_size), block2(buffer_size), input_buffer(&block1), output_buffer(&block2)
	{
		init();
	}
	
	~stage()
	{
		pthread_mutex_destroy(&mutex);
		pthread_cond_destroy(&cond);
	}

private:
	//! \brief Swap input and output buffers.
	void swap_buffers()
	{
		std::swap(input_buffer, output_buffer);

		input_buffer->current = input_buffer->begin;
		output_buffer->current = output_buffer->begin;
		input_buffer_filled = false;
		output_buffer_consumed = false;
		last_swap_done = StreamOperation::empty();
	}
	
	void reload()
	{
		//should not check this in operator++(), because should not block if iterator is only advanced, but not accessed.
		if(output_buffer->current >= output_buffer->stop)
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
	const value_type& operator * ()
	{
		return *output_buffer->current;
	}
	
	//! \brief Standard stream method.
	const value_type* operator -> () const
	{
		return &(operator*());
	}
	
	//! \brief Standard stream method.
	stage<StreamOperation>& operator ++ ()
	{
		++output_buffer->current;
		return *this;
	}
	
	//! \brief Standard stream method
	bool empty()
	{
		reload();
	
		return last_swap_done && output_buffer_consumed;
	}
	
	//! \brief Asynchronous method that always tries fill the input stage.
	void pull()
	{
		while(!StreamOperation::empty())
		{
			while(input_buffer->current < input_buffer->end && !StreamOperation::empty())
			{
				*input_buffer->current = StreamOperation::operator*();
				++input_buffer->current;
				StreamOperation::operator++();
			}
			input_buffer->stop = input_buffer->current;
			
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

//! \brief Helper function to call stage::pull() in a Pthread thread.
template<class StreamOperation>
void* call_pull(void* param)
{
	static_cast<stage<StreamOperation>*>(param)->pull();
	return NULL;
}

//! \brief Start pulling data asynchronously.
template<class StreamOperation>
void stage<StreamOperation>::stage::start_pulling()
{
	pthread_create(&puller, NULL, call_pull<StreamOperation>, this);
}

}	//namespace pipeline

}	//namespace stream

__STXXL_END_NAMESPACE

#endif
