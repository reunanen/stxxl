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

namespace pipeline
{

//! \brief Asynchronous stage to allow concurrent pipelining.
//! This class should be derived from.
template<class StreamOperation>
class stage
{
public:
	typedef typename StreamOperation::value_type value_type;
	
private:
	//! \brief Helper class encapsuling a buffer.
	class buffer
	{
	private:
		buffer(const buffer&) { }
		buffer& operator=(const buffer&) { return *this; }
	
	public:
		value_type* begin;
		value_type* end;
		//! \brief In case the buffer is not full, stop differs from end.
		value_type* stop;
		value_type* current;
		
		buffer(unsigned_type size)
		{
			begin = new value_type[size];
			stop = end = begin + size;
		}
		
		~buffer()
		{
			delete[] begin;
		}
	};
	
	//! \brief Stream operation that is extended to a pipelined one.
	StreamOperation& stream_operation;
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
	pthread_mutex_t mtx;
	//! \brief Condition variable, to wait for the other thread.
	pthread_cond_t cond;
	//! \brief Asynchronously pulling thread.
	pthread_t puller;

	void start_pulling();
	
public:
	//! \brief Constructor.
	//! \param buffer_size Size of each of the two buffers in number of elements. The total consumed memory will be \c 2*buffer_size*sizeof(value_type).
	//! \param stream_operation Stream operation that is extended to a pipelined one.
	stage(unsigned_type buffer_size, StreamOperation& stream_operation) : stream_operation(stream_operation), block1(buffer_size), block2(buffer_size), input_buffer(&block1), output_buffer(&block2)
	{
		input_buffer->current = input_buffer->begin;
		input_buffer_filled = false;
		
		output_buffer->current = input_buffer->end;
		output_buffer_consumed = true;
		
		last_swap_done = false;
		
		pthread_mutex_init(&mtx, 0);
		pthread_cond_init(&cond, 0);

		start_pulling();
	}
	
	~stage()
	{
		pthread_mutex_destroy(&mtx);
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
		last_swap_done = stream_operation.empty();
	}
	
	void reload()
	{
		//cannot check this in operator++(), because should not block if iterator is only advanced, but not accessed.
		if(output_buffer->current >= output_buffer->stop)
		{
			pthread_mutex_lock(&mtx);
			output_buffer_consumed = true;
			
			if(input_buffer_filled)
			{
				swap_buffers();
				
				pthread_mutex_unlock(&mtx);
				pthread_cond_signal(&cond);	//wake up other thread
			}
			else if(!last_swap_done)
			{
				while(output_buffer_consumed)	//to be swapped by other thread
					pthread_cond_wait(&cond, &mtx);
				pthread_mutex_unlock(&mtx);
			}
			else	//no further input, empty will return true
				pthread_mutex_unlock(&mtx);
		}
	}	

public:
	//! \brief Standard stream method.
	const value_type& operator * ()
	{
		//reload();
		
		return *output_buffer->current;
	}
	
	//! \brief Standard stream method.
	const value_type* operator -> () const
	{
		return &(operator*());
	}
	
	//! \brief Standard stream method.
	void operator ++ ()
	{
		++output_buffer->current;
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
		while(!stream_operation.empty())
		{
			while(input_buffer->current < input_buffer->end && !stream_operation.empty())
			{
				*input_buffer->current = *stream_operation;
				++input_buffer->current;
				++stream_operation;
			}
			input_buffer->stop = input_buffer->current;
			
			pthread_mutex_lock(&mtx);
			input_buffer_filled = true;
			
			if(output_buffer_consumed)
			{
				swap_buffers();
				
				pthread_mutex_unlock(&mtx);
				pthread_cond_signal(&cond);	//wake up other thread
			}
			else
			{
				while(input_buffer_filled)	//to be swapped by other thread
					pthread_cond_wait(&cond, &mtx);
				pthread_mutex_unlock(&mtx);
			}
		}
	}
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
void stage<StreamOperation>::start_pulling()
{
	pthread_create(&puller, NULL, call_pull<StreamOperation>, this);
}

struct Stopper {};

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
template <class Operation_, class Input1_, class Input2_ = Stopper, class Input3_ = Stopper, class Input4_ = Stopper, class Input5_ = Stopper, class Input6_ = Stopper>
class transform : public stxxl::stream::transform<Operation_, Input1_, Input2_, Input3_, Input4_, Input5_, Input6_>
{
	typedef typename stxxl::stream::transform<Operation_, Input1_, Input2_, Input3_, Input4_, Input5_, Input6_> StreamOperation;
	typedef stage<StreamOperation> stage;
	StreamOperation stream_operation;

public:
	transform(Operation_ o, Input1_ & i1_, Input2_ & i2_, Input3_ & i3_, Input4_ & i4_, Input5_ & i5_, Input6_ & i6_, unsigned_type buffer_size = 1000000) : stage(buffer_size, stream_operation), stream_operation(o, i1_, i2_, i3_, i4_, i5_, i6_)
	{
	}

	//! \brief Standard stream method
	//!
	//! Cannot be inherited from base class since it must return @c *this.
	transform&  operator ++()
	{
		stage::operator++();
		return *this;
	}
};


//! \brief Processes an input stream using given operation functor.
//!
//! Template parameters:
//! - \c Operation_ type of the operation (type of an
//! adaptable functor that takes 1 parameter)
//! - \c Input1_ type of the input
//! \remark This is a specialization of \c transform .
template <class Operation_, class Input1_ >
class transform<Operation_, Input1_, Stopper, Stopper, Stopper, Stopper, Stopper> : public stage<stxxl::stream::transform<Operation_, Input1_> >
{
	typedef typename stxxl::stream::transform<Operation_, Input1_> StreamOperation;
	typedef stage<StreamOperation> stage;
	StreamOperation stream_operation;

public:
	transform(Operation_ o, Input1_ & i1_, unsigned_type buffer_size = 1048576) : stage(buffer_size, stream_operation), stream_operation(o, i1_)
	{
	}

	//! \brief Standard stream method
	//!
	//! Cannot be inherited from base class since it must return @c *this.
	transform&  operator ++()
	{
		stage::operator++();
		return *this;
	}
};

}	//namespace pipeline

}	//namespace stream

__STXXL_END_NAMESPACE

#endif
