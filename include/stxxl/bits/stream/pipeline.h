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

//! \addtogroup streampack Stream package
//! \{

//! \brief Sub-namespace for providing parallel pipelined stream processing.
namespace pipeline
{

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
class basic_pull_empty_stage
{
public:
	StreamOperation& so;
	
protected:
	//! \brief Asynchronously pulling thread.
	pthread_t puller_thread;

public:
	//! \brief Generic Constructor for zero passed arguments.
	//! \param so Input stream operation.
	basic_pull_empty_stage(StreamOperation& so) :
		so(so)
	{
	}
	
public:
	//! \brief Standard stream method.
	void start()
	{
#if STXXL_START_PIPELINE
		STXXL_VERBOSE0("basic_pull_empty_stage " << this << " starts.");
		start_pulling();
#endif
	}

	virtual void async_pull() = 0;

protected:
	void start_pulling();
};

//! \brief Helper function to call basic_pull_empty_stage::async_pull() in a thread.
template<class StreamOperation>
void* call_async_pull_empty(void* param)
{
	static_cast<basic_pull_empty_stage<StreamOperation>*>(param)->async_pull();
	return NULL;
}

//! \brief Start pulling data asynchronously.
template<class StreamOperation>
void basic_pull_empty_stage<StreamOperation>::start_pulling()
{
	pthread_create(&puller_thread, NULL, call_async_pull_empty<StreamOperation>, this);
}


//! \brief Asynchronous stage to allow concurrent pipelining.
//!
//! This wrapper pulls asynchronously, one element at a time, and writes the data to a buffer.
template<class StreamOperation>
class pull_empty_stage : public basic_pull_empty_stage<StreamOperation>
{
public:
	pull_empty_stage(StreamOperation& so) :
		basic_pull_empty_stage<StreamOperation>(so)
	{
#if !STXXL_START_PIPELINE
		basic_pull_empty_stage<StreamOperation>::start_pulling();
#endif
	}

	typedef typename StreamOperation::value_type value_type;
	
protected:
	typedef basic_pull_empty_stage<StreamOperation> base;

	using base::so;

	//! \brief Asynchronous method that keeps trying to fill the incoming buffer.
	virtual void async_pull()
	{
		STXXL_VERBOSE0("pull_empty_stage " << this << " starts pulling.")
#if STXXL_START_PIPELINE
		so.start();
#endif
		while(!so.empty())
		{
  			*so;
			++so;
		}
		
		STXXL_VERBOSE0("pull_empty_stage " << this << " stops pulling.")
	}
};



//! \brief Asynchronous stage to allow concurrent pipelining.
//!
//! This wrapper pulls asynchronously, one batch of elements at a time, and writes the data to a buffer.
template<class StreamOperation>
class pull_empty_stage_batch : public basic_pull_empty_stage<StreamOperation>
{
public:
	pull_empty_stage_batch(StreamOperation& so) :
		basic_pull_empty_stage<StreamOperation>(so)
	{
#if !STXXL_START_PIPELINE
		basic_pull_empty_stage<StreamOperation>::start_pulling();
#endif
	}
	
	typedef typename StreamOperation::value_type value_type;
	
protected:
	typedef basic_pull_empty_stage<StreamOperation> base;
	typedef typename StreamOperation::const_iterator const_iterator;

	using base::so;
	
	//! \brief Asynchronous method that keeps trying to fill the incoming buffer.
	virtual void async_pull()
	{
		STXXL_VERBOSE0("pull_empty_stage_batch " << this << " starts pulling.")
#if STXXL_START_PIPELINE
		so.start();
#endif
		unsigned_type length;
		while((length = so.batch_length()) > 0)
		{
			for(const_iterator i = so.batch_begin(); i != so.batch_begin() + length; ++i)
				*i;
			so.operator+=(length);
		}
		STXXL_VERBOSE0("pull_empty_stage_batch " << this << " stops pulling.")
	}
};









//! \brief Asynchronous stage to allow concurrent pipelining.
//!
//! This wrapper pulls asynchronously, and writes the data to a buffer.
template<class ValueType>
class push_pull_stage
{
public:
	typedef ValueType value_type;
	typedef buffer<value_type> typed_buffer;
	typedef const value_type* const_iterator;
	
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
	//! \brief The input is finished.
	mutable bool input_finished;
	//! \brief The output is finished.
	mutable bool output_finished;
	//! \brief The input stream has run empty, the last swap_buffers() has been performed already.
	mutable volatile bool last_swap_done;
	//! \brief Mutex variable, to mutual exclude the other thread.
	mutable pthread_mutex_t mutex;
	//! \brief Condition variable, to wait for the other thread.
	mutable pthread_cond_t cond;

	void update_input_buffer_filled() const
	{
		input_buffer_filled = (incoming_buffer->current == incoming_buffer->stop);
	}

	void update_output_buffer_consumed() const
	{
		output_buffer_consumed = (outgoing_buffer->current == outgoing_buffer->stop);
	}
	
	void update_last_swap_done() const
	{
		last_swap_done = input_finished || output_finished;
	}

public:
	//! \brief Generic Constructor for zero passed arguments.
	//! \param buffer_size Total size of the buffers in bytes.
	push_pull_stage(unsigned_type buffer_size) :
		block1(buffer_size / 2),
		block2(buffer_size / 2),
		incoming_buffer(&block1),
		outgoing_buffer(&block2)
	{
		assert(buffer_size > 0);
		
		incoming_buffer->current = incoming_buffer->begin;
		incoming_buffer->stop = incoming_buffer->end;
		update_input_buffer_filled();
		
		outgoing_buffer->stop = outgoing_buffer->end;
		outgoing_buffer->current = outgoing_buffer->stop;
		update_output_buffer_consumed();
		
		output_finished = false;
		input_finished = false;
		update_last_swap_done();
		
		pthread_mutex_init(&mutex, 0);
		pthread_cond_init(&cond, 0);
	}
	
	//! \brief Destructor.
	virtual ~push_pull_stage()
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
		
		update_input_buffer_filled();
		update_output_buffer_consumed();
		update_last_swap_done();
	}
	
	//! \brief Check whether outgoing buffer has been consumed and possibly wait for new data to come in.
	//!
	//! Should not be called in operator++(), but in empty() (or operator*()),
	//! because should not block if iterator is only advanced, but not accessed.
	void reload() const
	{
		if(outgoing_buffer->current == outgoing_buffer->stop)
		{
			pthread_mutex_lock(&mutex);
			
			update_output_buffer_consumed();	//sets true
			
			if(input_buffer_filled)
			{
				swap_buffers();
				pthread_cond_signal(&cond);	//wake up other thread
			}
			else
				while(!last_swap_done && output_buffer_consumed)	//to be swapped by other thread
					pthread_cond_wait(&cond, &mutex);	//wait for other thread to swap in some input
			
			pthread_mutex_unlock(&mutex);
		}
		//otherwise, at least one element available
	}	

public:
	//! \brief Standard stream method.
	void start()
	{
#if STXXL_START_PIPELINE
		STXXL_VERBOSE0("push_pull_stage " << this << " starts.");
#endif
		//do nothing
	}

	//! \brief Standard stream method.
	bool empty() const
	{
		reload();

		return batch_length() == 0;//output_buffer_consumed;
	}
	
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
	push_pull_stage<value_type>& operator ++ ()
	{
		++outgoing_buffer->current;
		return *this;
	}
	
	
	//! \brief Batched stream method.
	unsigned_type batch_length() const
	{
		reload();

		return outgoing_buffer->stop - outgoing_buffer->current;
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
	
	//! \brief Batched stream method.
	push_pull_stage<value_type>& operator += (unsigned_type length)
	{
		assert(outgoing_buffer->stop - outgoing_buffer->current);
		outgoing_buffer->current += length;
		return *this;
	}

	//! \brief Standard stream method.
	void stop_pull()
	{
		if(!output_finished)
		{
			output_finished = true;
			update_last_swap_done();
		
			pthread_cond_signal(&cond);	//wake up other thread
		}
	}

protected:
	//! \brief Check whether incoming buffer has run full and possibly wait for data being pushed forward.
	void offload() const
	{
		if(incoming_buffer->current == incoming_buffer->end || input_finished)
		{
			incoming_buffer->stop = incoming_buffer->current;
		
			pthread_mutex_lock(&mutex);
			
			update_input_buffer_filled(); //sets true
			
			if(output_buffer_consumed)
			{
				swap_buffers();
				pthread_cond_signal(&cond);	//wake up other thread
			}
			else
				while(!last_swap_done && input_buffer_filled)	//to be swapped by other thread
					pthread_cond_wait(&cond, &mutex);	//wait for other thread to take the input
			
			pthread_mutex_unlock(&mutex);
		}
	}	

public:
	void start_push()
	{
#if STXXL_START_PIPELINE
		STXXL_VERBOSE0("push_pull_stage " << this << " starts push.");
#endif
		//do nothing
	}
	
	//! \brief Standard push stream method.
	//! \param val value to be pushed
	void push(const value_type & val)
	{
		*incoming_buffer->current = val;
		++incoming_buffer->current;
		
		offload();
	}
	
	//! \brief Batched stream method.
	unsigned_type push_batch_length() const
	{
		return incoming_buffer->end - incoming_buffer->current;
	}
	
    //! \brief Batched stream method.
    void push_batch(const value_type* batch_begin, const value_type* batch_end)
    {
        assert(static_cast<unsigned_type>(batch_end - batch_begin) <= push_batch_length());
        
        incoming_buffer->current = std::copy(batch_begin, batch_end, incoming_buffer->current);
        
        offload();
    }
    
    //! \brief Batched stream method.
    template<typename InputIterator>
    void push_batch(const InputIterator& batch_begin, const InputIterator& batch_end)
    {
        incoming_buffer->current = std::copy(batch_begin, batch_end, incoming_buffer->current);
        
        offload();
    }
    
	//! \brief Standard stream method.
	void stop_push() const
	{
#if STXXL_START_PIPELINE
		STXXL_VERBOSE0("general push_pull_stage " << this << " stops push.");
#endif
		if(!input_finished)
		{
			input_finished = true;
		
			offload();
		}
	}
};

//! \brief Asynchronous stage to allow concurrent pipelining.
//!
//! This wrapper pulls asynchronously, and writes the data to a buffer.
template<class StreamOperation>
class basic_pull_stage : public push_pull_stage<typename StreamOperation::value_type>
{
protected:
	typedef push_pull_stage<typename StreamOperation::value_type> base;

public:
	StreamOperation& so;
	
protected:
	//! \brief Asynchronously pulling thread.
	pthread_t puller_thread;

public:
	//! \brief Generic Constructor for zero passed arguments.
	//! \param buffer_size Total size of the buffers in bytes.
	//! \param so Input stream operation.
	basic_pull_stage(unsigned_type buffer_size, StreamOperation& so) :
		base(buffer_size),
		so(so)
	{
	}
	
	//! \brief Destructor.
	virtual ~basic_pull_stage()
	{
		base::stop_pull();
		
		void* return_code;
		pthread_join(puller_thread, &return_code);
	}

public:
	//! \brief Standard stream method.
	void start()
	{
#if STXXL_START_PIPELINE
		STXXL_VERBOSE0("basic_pull_stage " << this << " starts.");
		start_pulling();
#endif
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
	pthread_create(&puller_thread, NULL, call_async_pull<StreamOperation>, this);
}

//! \brief Asynchronous stage to allow concurrent pipelining.
//!
//! This wrapper pulls asynchronously, one element at a time, and writes the data to a buffer.
template<class StreamOperation>
class pull_stage : public basic_pull_stage<StreamOperation>
{
public:
	pull_stage(unsigned_type buffer_size, StreamOperation& so) :
		basic_pull_stage<StreamOperation>(buffer_size, so)
	{
#if !STXXL_START_PIPELINE
		basic_pull_stage<StreamOperation>::start_pulling();
#endif
	}

	typedef typename StreamOperation::value_type value_type;
	
protected:
	typedef push_pull_stage<value_type> base;

	using basic_pull_stage<StreamOperation>::so;

	//! \brief Asynchronous method that keeps trying to fill the incoming buffer.
	virtual void async_pull()
	{
		STXXL_VERBOSE0("pull_stage " << this << " starts pulling.")
#if STXXL_START_PIPELINE
		so.start();
#endif
		while(!so.empty() && !base::output_finished)
		{
			base::push(*so);
			++so;
		}
		base::stop_push();
		STXXL_VERBOSE0("pull_stage " << this << " stops pulling.")
	}
};



//! \brief Asynchronous stage to allow concurrent pipelining.
//!
//! This wrapper pulls asynchronously, one batch of elements at a time, and writes the data to a buffer.
template<class StreamOperation>
class pull_stage_batch : public basic_pull_stage<StreamOperation>
{
public:
	pull_stage_batch(unsigned_type buffer_size, StreamOperation& so) :
		basic_pull_stage<StreamOperation>(buffer_size, so)
	{
#if !STXXL_START_PIPELINE
		basic_pull_stage<StreamOperation>::start_pulling();
#endif
	}
	
	typedef typename StreamOperation::value_type value_type;
	
protected:
	typedef push_pull_stage<value_type> base;

	using basic_pull_stage<StreamOperation>::so;
	
	//! \brief Asynchronous method that keeps trying to fill the incoming buffer.
	virtual void async_pull()
	{
		STXXL_VERBOSE0("pull_stage_batch " << this << " starts pulling.")
#if STXXL_START_PIPELINE
		so.start();
#endif
		unsigned_type length;
		while((length = so.batch_length()) > 0 && !base::output_finished)
		{
			length = std::min(length, base::push_batch_length());
			base::push_batch(so.batch_begin(), so.batch_begin() + length);
			so.operator+=(length);
		}
		base::stop_push();
		STXXL_VERBOSE0("pull_stage_batch " << this << " stops pulling.")
	}
};



//! \brief Asynchronous stage to allow concurrent pipelining.
//!
//! This wrapper reads the data from a buffer asynchronously and pushes.
template<class StreamOperation>
class basic_push_stage : public push_pull_stage<typename StreamOperation::value_type>
{
public:
	StreamOperation& so;
	
protected:
	typedef push_pull_stage<typename StreamOperation::value_type> base;
	//! \brief Asynchronously pushing thread.
	pthread_t pusher_thread;

public:
	//! \brief Generic Constructor for zero passed arguments.
	//! \param buffer_size Total size of the buffers in bytes.
	//! \param so Input stream operation.
	basic_push_stage(unsigned_type buffer_size, StreamOperation& so) :
		base(buffer_size),
		so(so)
	{
	}
	
	//! \brief Destructor.
	virtual ~basic_push_stage()
	{
	}

public:
	//! \brief Standard push stream method.
	void start_push()
	{
#if STXXL_START_PIPELINE
		STXXL_VERBOSE0("basic_push_stage " << this << " starts.");
		start_pushing();
#endif
	}
	
	//! \brief Stream method.
	void stop_push() const
	{
		if(!base::input_finished)
		{
			base::stop_push();
			
			void* return_code;
			pthread_join(pusher_thread, &return_code);
		}
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
	pthread_create(&pusher_thread, NULL, call_async_push<StreamOperation>, this);
}

//! \brief Asynchronous stage to allow concurrent pipelining.
//!
//! This wrapper reads the data from a buffer asynchronously, one element at a time, and pushes.
template<class StreamOperation>
class push_stage : public basic_push_stage<StreamOperation>
{
public:
	push_stage(unsigned_type buffer_size, StreamOperation& so) :
		basic_push_stage<StreamOperation>(buffer_size, so)
	{
#if !STXXL_START_PIPELINE
		basic_push_stage<StreamOperation>::start_pushing();
#endif
	}

protected:
	typedef basic_push_stage<StreamOperation> base;
	using basic_push_stage<StreamOperation>::so;
	
	//! \brief Asynchronous method that keeps trying to push from the outgoing buffer.
	virtual void async_push()
	{
#if STXXL_START_PIPELINE
		STXXL_VERBOSE0("push_stage " << this << " starts pushing.")
		so.start_push();
#endif
		while(!base::empty())
		{
			so.push(base::operator*());
			base::operator++();
		}
		so.stop_push();
		STXXL_VERBOSE0("push_stage " << this << " stops pushing.")
	}

};



//! \brief Asynchronous stage to allow concurrent pipelining.
//!
//! This wrapper reads the data from a buffer asynchronously, one batch of elements at a time, and pushes.
template<class StreamOperation>
class push_stage_batch : public basic_push_stage<StreamOperation>
{
public:
	push_stage_batch(unsigned_type buffer_size, StreamOperation& so) :
		basic_push_stage<StreamOperation>(buffer_size, so)
	{
#if !STXXL_START_PIPELINE
		basic_push_stage<StreamOperation>::start_pushing();
#endif
	}

protected:
	typedef basic_push_stage<StreamOperation> base;
	using basic_push_stage<StreamOperation>::so;
	
	//! \brief Asynchronous method that keeps trying to push from the outgoing buffer.
	virtual void async_push()
	{
#if STXXL_START_PIPELINE
		STXXL_VERBOSE0("push_stage_batch " << this << " starts pushing.")
		so.start_push();
#endif
		unsigned_type length;
		while((length = base::batch_length()) > 0)
		{
			length = std::min(length, so.push_batch_length());
			so.push_batch(base::batch_begin(), base::batch_begin() + length);
			base::operator+=(length);
		}
		so.stop_push();
		STXXL_VERBOSE0("push_stage_batch " << this << " stops pushing.")
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
	//! \param so Input stream operation.
	dummy_pull_stage(unsigned_type buffer_size, StreamOperation& so) :
		so(so)
	{
		UNUSED(buffer_size);
	}
	
	//! \brief Standard stream method.
	void start()
	{
		so.start();
	}
	
	//! \brief Standard stream method.
	bool empty() const
	{
		return so.empty();
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


	//! \brief Batched stream method.
	unsigned_type batch_length() const
	{
		return 1;
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
	
	//! \brief Batched stream method.
	dummy_pull_stage<StreamOperation>& operator += (unsigned_type length)
	{
		assert(length == 1);
		return operator++();
	}
};

//! \brief Dummy stage wrapper switch of pipelining by a define.
template<class StreamOperation, class ConnectedStreamOperation>
class connect_pull_stage
{
public:
	typedef typename StreamOperation::value_type value_type;
	typedef const value_type* const_iterator;
		
	StreamOperation& so;
	ConnectedStreamOperation& cso;
	
public:
	//! \brief Generic Constructor for zero passed arguments.
	//! \param so Input stream operation.
	//! \param cso Stream operation to connect to.
	connect_pull_stage(StreamOperation& so, ConnectedStreamOperation& cso) :
		so(so), cso(cso)
	{
#if !STXXL_START_PIPELINE
		start();
#endif
	}
	
	//! \brief Standard stream method.
	void start()
	{
		STXXL_VERBOSE0("connect_pull_stage " << this << " starts.")
		cso.start();
		STXXL_VERBOSE0("connect_pull_stage " << this << " inter.")
		so.start();
	}
	
	//! \brief Standard stream method.
	bool empty() const
	{
		return so.empty();
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
	connect_pull_stage<StreamOperation, ConnectedStreamOperation>& operator ++ ()
	{
		++so;
		return *this;
	}


	//! \brief Batched stream method.
	unsigned_type batch_length() const
	{
		return so.batch_length();
	}
	
	//! \brief Batched stream method.
	const_iterator batch_begin() const
	{
		return so.batch_begin();
	}
	
	//! \brief Batched stream method.
	const value_type& operator[](unsigned_type index) const
	{
		return so[index];
	}
	
	//! \brief Batched stream method.
	connect_pull_stage<StreamOperation, ConnectedStreamOperation>& operator += (unsigned_type length)
	{
		so += length;
		return *this;
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
	//! \param so Input stream operation.
	dummy_push_stage(unsigned_type buffer_size, StreamOperation& so) :
		so(so)
	{
	}
	
	//! \brief Standard stream method.
	void start_push()
	{
		so.start_push();
	}
	
	//! \brief Standard stream method.
	void push(const value_type& val)
	{
		so.push(val);
	}
	
	//! \brief Batched stream method.
	unsigned_type push_batch_length() const
	{
		return 1;
	}
	
	//! \brief Batched stream method.
	void push_batch(const value_type* batch_begin, const value_type* batch_end)
	{
		assert((batch_end - batch_begin) == 1);
		push(*batch_begin);
	}
	
	//! \brief Standard stream method.
	void stop_push() const
	{
		so.stop_push();
	}
	
	const result_type& result() const
	{
		return so.result();
	}
};

//! \}

}	//namespace pipeline

//! \}

}	//namespace stream

__STXXL_END_NAMESPACE

#endif
