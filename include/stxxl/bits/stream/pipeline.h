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

//! \brief Asynchronous stage to allow concurrent pipelining.
//!
//! This wrapper pulls asynchronously, and writes the data to a buffer.
template<class StreamOperation>
class basic_pull_stage
{
public:
	typedef typename StreamOperation::value_type value_type;
	typedef buffer<value_type> typed_buffer;
	typedef const value_type* const_iterator;
	
	StreamOperation& so;
	
protected:
	//! \brief First double buffering buffer.
	typed_buffer block1;
	//! \brief Second double buffering buffer.
	typed_buffer block2;
	//! \brief Buffer that is currently input to.
	mutable typed_buffer* incoming_buffer;
	//! \brief Buffer that is currently output from.
	mutable typed_buffer* outgoing_buffer;
	//! \brief The incoming buffer has been filled (completely, or the input stream has run empty).
	mutable volatile bool input_buffer_filled;
	//! \brief The outgoing buffer has been consumed.
	mutable volatile bool output_buffer_consumed;
	//! \brief The output is finished.
	mutable bool output_finished;
	//! \brief The input stream has run empty, the last swap_buffers() has been performed already.
	mutable volatile bool last_swap_done;
	//! \brief Mutex variable, to mutual exclude the other thread.
	mutable pthread_mutex_t mutex;
	//! \brief Condition variable, to wait for the other thread.
	mutable pthread_cond_t cond;
	//! \brief Asynchronously pulling thread.
	pthread_t puller;

public:
	//! \brief Generic Constructor for zero passed arguments.
	//! \param buffer_size Total size of the buffers in bytes.
	basic_pull_stage(unsigned_type buffer_size, StreamOperation& so) :
		so(so),
		block1(buffer_size / 2), block2(buffer_size / 2), incoming_buffer(&block1), outgoing_buffer(&block2)
	{
		incoming_buffer->current = incoming_buffer->begin;
		input_buffer_filled = false;
		
		outgoing_buffer->current = outgoing_buffer->end;
		output_buffer_consumed = true;
		
		output_finished = false;
		last_swap_done = output_finished || so.empty();
		
		pthread_mutex_init(&mutex, 0);
		pthread_cond_init(&cond, 0);
	}
	
	//! \brief Destructor.
	~basic_pull_stage()
	{
		output_finished = true;
		
		reload();
		
		void* return_code;
		pthread_join(puller, &return_code);
			
		pthread_mutex_destroy(&mutex);
		pthread_cond_destroy(&cond);
	}

protected:
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
		
		last_swap_done = output_finished || so.empty();
	}
	
	//! \brief Check whether outgoing buffer has run empty and possibly wait for new data to come in.
	//!
	//! Should not be called in operator++(), but in empty() (or operator*()),
	//! because should not block if iterator is only advanced, but not accessed.
	void reload() const
	{
		if(outgoing_buffer->current >= outgoing_buffer->stop || output_finished)
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
		assert(!empty());
		return *outgoing_buffer->current;
	}
	
	//! \brief Standard stream method.
	const value_type* operator -> () const
	{
		return &(operator*());
	}
	
	//! \brief Standard stream method.
	basic_pull_stage<StreamOperation>& operator ++ ()
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
	
	//! \brief Batched stream method.
	unsigned_type batch_length() const
	{
		reload();

		return outgoing_buffer->stop - outgoing_buffer->current;
	}
	
	//! \brief Batched stream method.
	basic_pull_stage<StreamOperation>& operator += (unsigned_type length)
	{
		outgoing_buffer->current += length;
		assert(outgoing_buffer->current <= outgoing_buffer->stop);
		return *this;
	}
	
	//! \brief Batched stream method.
	const_iterator batch_begin() const
	{
		return outgoing_buffer->current;
	}
	
	//! \brief Batched stream method.
	const value_type& operator[](unsigned_type index) const
	{
		assert(outgoing_buffer->current + index < outgoing_buffer->stop);
		return *(outgoing_buffer->current + index);
	}
	
	virtual void async_pull() = 0;

protected:
	void start_pulling();
};

//! \brief Helper function to call basic_pull_stage::async_pull() in a thread.
template<class StreamOperation>
void* call_async_pull(void* param)
{
	static_cast<basic_pull_stage<StreamOperation>*>(param)->async_pull();
	return NULL;
}

//! \brief Start pulling data asynchronously.
template<class StreamOperation>
void basic_pull_stage<StreamOperation>::start_pulling()
{
	pthread_create(&puller, NULL, call_async_pull<StreamOperation>, this);
}

//! \brief Asynchronous stage to allow concurrent pipelining.
//!
//! This wrapper pulls asynchronously, one element at a time, and writes the data to a buffer.
template<class StreamOperation>
class pull_stage : public basic_pull_stage<StreamOperation>
{
public:
	pull_stage(unsigned_type buffer_size, StreamOperation& so) : basic_pull_stage<StreamOperation>(buffer_size, so)
	{
		basic_pull_stage<StreamOperation>::start_pulling();
	}

protected:
	using basic_pull_stage<StreamOperation>::so;
	using basic_pull_stage<StreamOperation>::incoming_buffer;
	using basic_pull_stage<StreamOperation>::outgoing_buffer;
	using basic_pull_stage<StreamOperation>::mutex;
	using basic_pull_stage<StreamOperation>::cond;
	using basic_pull_stage<StreamOperation>::input_buffer_filled;
	using basic_pull_stage<StreamOperation>::output_buffer_consumed;
	using basic_pull_stage<StreamOperation>::swap_buffers;
	using basic_pull_stage<StreamOperation>::last_swap_done;
	
	//! \brief Asynchronous method that keeps trying to fill the incoming buffer.
	virtual void async_pull()
	{
		while(!last_swap_done)
		{
			while(incoming_buffer->current < incoming_buffer->end && !so.empty())
			{
				*incoming_buffer->current = *so;
				++incoming_buffer->current;
				++so;
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
};



//! \brief Asynchronous stage to allow concurrent pipelining.
//!
//! This wrapper pulls asynchronously, one batch of elements at a time, and writes the data to a buffer.
template<class StreamOperation>
class pull_stage_batch : public basic_pull_stage<StreamOperation>
{
public:
	pull_stage_batch(unsigned_type buffer_size, StreamOperation& so) : basic_pull_stage<StreamOperation>(buffer_size, so)
	{
		basic_pull_stage<StreamOperation>::start_pulling();
	}
	
	typedef typename StreamOperation::value_type value_type;

protected:
	using basic_pull_stage<StreamOperation>::so;
	using basic_pull_stage<StreamOperation>::incoming_buffer;
	using basic_pull_stage<StreamOperation>::outgoing_buffer;
	using basic_pull_stage<StreamOperation>::mutex;
	using basic_pull_stage<StreamOperation>::cond;
	using basic_pull_stage<StreamOperation>::input_buffer_filled;
	using basic_pull_stage<StreamOperation>::output_buffer_consumed;
	using basic_pull_stage<StreamOperation>::swap_buffers;
	using basic_pull_stage<StreamOperation>::last_swap_done;
	
	//! \brief Asynchronous method that keeps trying to fill the incoming buffer.
	virtual void async_pull()
	{
		unsigned_type length;
		while(!last_swap_done)
		{
			while(incoming_buffer->current < incoming_buffer->end && (length = so.batch_length()) > 0)
			{
				length = std::min<unsigned_type>(length, incoming_buffer->end - incoming_buffer->current);
				
				for(typename StreamOperation::const_iterator i = so.batch_begin(), end = so.batch_begin() + length; i != end; ++i)
				{
					*incoming_buffer->current = *i;
					++incoming_buffer->current;
				}
				so += length;
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
};



//! \brief Asynchronous stage to allow concurrent pipelining.
//!
//! This wrapper reads the data from a buffer asynchronously and pushes.
template<class StreamOperation>
class basic_push_stage
{
public:
	typedef typename StreamOperation::value_type value_type;
	typedef buffer<value_type> typed_buffer;
	typedef typename StreamOperation::result_type result_type;
	
	StreamOperation& so;
	
protected:
	//! \brief First double buffering buffer.
	typed_buffer block1;
	//! \brief Second double buffering buffer.
	typed_buffer block2;
	//! \brief Buffer that is currently input to.
	mutable typed_buffer* incoming_buffer;
	//! \brief Buffer that is currently output from.
	mutable typed_buffer* outgoing_buffer;
	//! \brief The incoming buffer has been filled (completely, or the input stream has run empty).
	mutable volatile bool input_buffer_filled;
	//! \brief The outgoing buffer has been consumed.
	mutable volatile bool output_buffer_consumed;
	//! \brief The input had finished, the last swap_buffers() has been performed already.
	mutable volatile bool last_swap_done;
	//! \brief The input is finished.
	mutable bool input_finished;
	//! \brief Mutex variable, to mutual exclude the other thread.
	mutable pthread_mutex_t mutex;
	//! \brief Condition variable, to wait for the other thread.
	mutable pthread_cond_t cond;
	//! \brief Asynchronously pushing thread.
	pthread_t pusher;

public:
	//! \brief Generic Constructor for zero passed arguments.
	//! \param buffer_size Total size of the buffers in bytes.
	basic_push_stage(unsigned_type buffer_size, StreamOperation& so) :
		so(so),
		block1(buffer_size / 2), block2(buffer_size / 2), incoming_buffer(&block1), outgoing_buffer(&block2)
	{
		incoming_buffer->current = incoming_buffer->begin;
		input_buffer_filled = false;
		
		outgoing_buffer->current = outgoing_buffer->end;
		output_buffer_consumed = true;
		
		input_finished = false;
		last_swap_done = input_finished;
		
		pthread_mutex_init(&mutex, 0);
		pthread_cond_init(&cond, 0);
	}
	
	//! \brief Destructor.
	~basic_push_stage()
	{
		pthread_mutex_destroy(&mutex);
		pthread_cond_destroy(&cond);
	}

protected:
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
	const result_type& result() const
	{
		if(!input_finished)
		{
			input_finished = true;
			
			unload();
			
			void* return_code;
			pthread_join(pusher, &return_code);
		}
		
		return so.result();
	}

	virtual void async_push() = 0;

protected:
	void start_pushing();
};

//! \brief Helper function to call basic_push_stage::push() in a Pthread thread.
template<class StreamOperation>
void* call_async_push(void* param)
{
	static_cast<basic_push_stage<StreamOperation>*>(param)->async_push();
	return NULL;
}

//! \brief Start pushing data asynchronously.
template<class StreamOperation>
void basic_push_stage<StreamOperation>::start_pushing()
{
	pthread_create(&pusher, NULL, call_async_push<StreamOperation>, this);
}

//! \brief Asynchronous stage to allow concurrent pipelining.
//!
//! This wrapper reads the data from a buffer asynchronously, one element at a time, and pushes.
template<class StreamOperation>
class push_stage : public basic_push_stage<StreamOperation>
{
public:
	push_stage(unsigned_type buffer_size, StreamOperation& so) : basic_push_stage<StreamOperation>(buffer_size, so)
	{
		basic_push_stage<StreamOperation>::start_pushing();
	}

protected:
	using basic_push_stage<StreamOperation>::so;
	using basic_push_stage<StreamOperation>::incoming_buffer;
	using basic_push_stage<StreamOperation>::outgoing_buffer;
	using basic_push_stage<StreamOperation>::mutex;
	using basic_push_stage<StreamOperation>::cond;
	using basic_push_stage<StreamOperation>::input_buffer_filled;
	using basic_push_stage<StreamOperation>::output_buffer_consumed;
	using basic_push_stage<StreamOperation>::swap_buffers;
	using basic_push_stage<StreamOperation>::last_swap_done;
	
	//! \brief Asynchronous method that keeps trying to push from the outgoing buffer.
	virtual void async_push()
	{
		while(true)
		{
			while(outgoing_buffer->current < outgoing_buffer->stop)
			{
				so.push(*outgoing_buffer->current);
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

};



//! \brief Asynchronous stage to allow concurrent pipelining.
//!
//! This wrapper reads the data from a buffer asynchronously, one batch of elements at a time, and pushes.
template<class StreamOperation>
class push_stage_batch : public basic_push_stage<StreamOperation>
{
public:
	push_stage_batch(unsigned_type buffer_size, StreamOperation& so) : basic_push_stage<StreamOperation>(buffer_size, so)
	{
		basic_push_stage<StreamOperation>::start_pushing();
	}

protected:
	using basic_push_stage<StreamOperation>::so;
	using basic_push_stage<StreamOperation>::incoming_buffer;
	using basic_push_stage<StreamOperation>::outgoing_buffer;
	using basic_push_stage<StreamOperation>::mutex;
	using basic_push_stage<StreamOperation>::cond;
	using basic_push_stage<StreamOperation>::input_buffer_filled;
	using basic_push_stage<StreamOperation>::output_buffer_consumed;
	using basic_push_stage<StreamOperation>::swap_buffers;
	using basic_push_stage<StreamOperation>::last_swap_done;
	
	//! \brief Asynchronous method that keeps trying to push from the outgoing buffer.
	virtual void async_push()
	{
		while(true)
		{
			while(outgoing_buffer->current < outgoing_buffer->stop)
			{
				unsigned_type length = std::min<unsigned_type>(so.push_batch_length(), outgoing_buffer->stop - outgoing_buffer->current);
				assert(length > 0);
				so.push_batch(outgoing_buffer->current, outgoing_buffer->current + length);
				outgoing_buffer->current += length;
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

};



//! \brief Dummy stage wrapper switch of pipelining by a define.
template<class StreamOperation>
class dummy_pull_stage
{
public:
	typedef typename StreamOperation::value_type value_type;
	typedef const value_type* const_iterator;
		
	StreamOperation& so;
	
public:
	//! \brief Generic Constructor for zero passed arguments.
	//! \param buffer_size Total size of the buffers in bytes.
	dummy_pull_stage(unsigned_type buffer_size, StreamOperation& so) :
		so(so)
	{
	}
	
	//! \brief Standard stream method.
	const value_type& operator * () const
	{
		return *so;
	}
	
	//! \brief Standard stream method.
	const value_type* operator -> () const
	{
		return &(operator*());
	}
	
	//! \brief Standard stream method.
	dummy_pull_stage<StreamOperation>& operator ++ ()
	{
		++so;
		return *this;
	}
	
	//! \brief Standard stream method.
	bool empty() const
	{
		return so.empty();
	}
	
	//! \brief Batched stream method.
	unsigned_type batch_length() const
	{
		return 1;
	}
	
	//! \brief Batched stream method.
	dummy_pull_stage<StreamOperation>& operator += (unsigned_type length)
	{
		assert(length == 1);
		return operator++();
	}
	
	//! \brief Batched stream method.
	const_iterator batch_begin() const
	{
		return &(operator*());
	}
	
	//! \brief Batched stream method.
	const value_type& operator[](unsigned_type index) const
	{
		assert(index == 0);
		return operator*();
	}
};

//! \brief Dummy stage wrapper switch of pipelining by a define.
template<class StreamOperation>
class dummy_push_stage
{
public:
	typedef typename StreamOperation::value_type value_type;
	typedef typename StreamOperation::result_type result_type;
	
	StreamOperation& so;
	
public:
	//! \brief Generic Constructor for zero passed arguments.
	//! \param buffer_size Total size of the buffers in bytes.
	dummy_push_stage(unsigned_type buffer_size, StreamOperation& so) :
		so(so)
	{
	}
	
	//! \brief Standard stream method.
	void push(const value_type& val)
	{
		so.push(val);
	}
	
	//! \brief Batched stream method.
	void push_batch(value_type* batch_begin, value_type* batch_end)
	{
		assert((batch_end - batch_begin) == 1);
		push(*batch_begin);
	}
	
	//! \brief Batched stream method.
	unsigned_type push_batch_length() const
	{
		return 1;
	}
	
	const result_type& result() const
	{
		return so.result();
	}
};

//! \}

//! \}

}	//namespace pipeline

}	//namespace stream

__STXXL_END_NAMESPACE

#endif
