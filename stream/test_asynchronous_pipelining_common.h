/***************************************************************************
 *  stream/test_asynchronous_pipelining_common.h
 *
 *  Part of the STXXL. See http://stxxl.sourceforge.net
 *
 *  Copyright (C) 2009 Johannes Singler <singler@ira.uka.de>
 *
 *  Distributed under the Boost Software License, Version 1.0.
 *  (See accompanying file LICENSE_1_0.txt or copy at
 *  http://www.boost.org/LICENSE_1_0.txt)
 **************************************************************************/

#include <limits>
#include <stxxl/bits/mng/mng.h>
#include <stxxl/vector>
#include <stxxl/bits/algo/sort.h>
#include <stxxl/bits/stream/sort_stream.h>
#include <stxxl/bits/stream/async.h>
#include <stxxl/algorithm>

#include <algorithm>
#include <functional>

const unsigned long long megabyte = 1024 * 1024;
const int block_size = 1 * megabyte;

#define RECORD_SIZE 16
#define MAGIC 123

struct my_type
{
    typedef unsigned long long key_type;

    key_type _key;
    key_type _load;
    //char _data[RECORD_SIZE - 2 * sizeof(key_type)];
    key_type key() const { return _key; }

    my_type() { }
    my_type(key_type __key) : _key(__key) { }
    my_type(key_type __key, key_type __load) : _key(__key), _load(__load) { }

    void operator = (const key_type & __key)
    {
        _key = __key;
    }

    void operator = (const my_type & mt)
    {
        _key = mt._key;
        _load = mt._load;
    }
};

std::ostream & operator << (std::ostream & o, const my_type & obj);

typedef stxxl::tuple<my_type, my_type> my_tuple;

std::ostream & operator << (std::ostream & o, const my_tuple & obj);

bool operator < (const my_type & a, const my_type & b);

bool operator > (const my_type & a, const my_type & b);

bool operator != (const my_type & a, const my_type & b);

struct cmp_less_key : public std::less<my_type>
{
    my_type min_value() const { return my_type((std::numeric_limits<my_type::key_type>::min)(), MAGIC); }
    my_type max_value() const { return my_type((std::numeric_limits<my_type::key_type>::max)(), MAGIC); }
};

struct cmp_greater_key : public std::greater<my_type>
{
    my_type min_value() const { return my_type((std::numeric_limits<my_type::key_type>::max)(), MAGIC); }
    my_type max_value() const { return my_type((std::numeric_limits<my_type::key_type>::min)(), MAGIC); }
};

struct cmp_less_load : public std::binary_function<my_type, my_type, bool>
{
    my_type min_value() const { return my_type(MAGIC, (std::numeric_limits<my_type::key_type>::min)()); }
    my_type max_value() const { return my_type(MAGIC, (std::numeric_limits<my_type::key_type>::max)()); }

    bool operator () (const my_type & mt1, const my_type & mt2) const
    {
        return mt1._load < mt2._load;
    }
};

struct cmp_greater_load : public std::binary_function<my_type, my_type, bool>
{
    my_type min_value() const { return my_type(MAGIC, (std::numeric_limits<my_type::key_type>::max)()); }
    my_type max_value() const { return my_type(MAGIC, (std::numeric_limits<my_type::key_type>::min)()); }

    bool operator () (const my_type & mt1, const my_type & mt2) const
    {
        return mt1._load > mt2._load;
    }
};

struct cmp_less_tuple : public std::binary_function<my_tuple, my_tuple, bool>
{
    bool operator () (const my_tuple & t1, const my_tuple & t2);
};

struct random_my_type
{
private:
    stxxl::random_number32 rng;
    my_type mt;

public:
    random_my_type(unsigned long long seed)
    {
        stxxl::srandom_number32(seed);     //global variable
    }

    const my_type & operator () ()
    {
        mt._key = static_cast<unsigned long long>(rng()) << 32 | rng();
        mt._load = static_cast<unsigned long long>(rng()) << 32 | rng();
        return mt;
    }
};

#if PIPELINED
#define PULL pull
#define PUSH push
#if BATCHED
#define PULL_BATCH pull_batch
#define PUSH_BATCH push_batch
#else
#define PULL_BATCH pull
#define PUSH_BATCH push
#endif
#else
#define PULL dummy_pull
#define PUSH dummy_push
#define PULL_BATCH dummy_pull
#define PUSH_BATCH dummy_push
#endif
#define PUSH_PULL push_pull

extern stxxl::unsigned_type run_size;

typedef stxxl::vector<my_type, 4, stxxl::lru_pager<8>, block_size, STXXL_DEFAULT_ALLOC_STRATEGY> vector_type;

typedef stxxl::vector<my_tuple, 4, stxxl::lru_pager<8>, block_size, STXXL_DEFAULT_ALLOC_STRATEGY> vector_tuple_type;

/** @brief Do nothing, just return argument. */
template <typename T>
class identity : public std::unary_function<T, void>
{
public:
    typedef T value_type;

    const T & operator () (const T & t) const
    {
        return t;
    }

    void stop_push()
    { }

    void start_push()
    { }
};

/** @brief Accumulate arguments. */
template <typename T>
class accumulate : public std::unary_function<T, void>
{
private:
    mutable unsigned long long sum, number;

public:
    typedef T value_type;

    accumulate() : sum(0), number(0) { }

    const T & operator () (const T & t) const
    {
        sum += t._key;
        ++number;
        //std::cerr << "+ " << t._key << " " << sum << std::endl;
        assert(t._key != 0);
        assert(t._load != 0);
        assert(t._key != (std::numeric_limits<my_type::key_type>::min)());
        assert(t._key != (std::numeric_limits<my_type::key_type>::max)());
        assert(t._load != (std::numeric_limits<my_type::key_type>::min)());
        assert(t._load != (std::numeric_limits<my_type::key_type>::max)());
        return t;
    }

    unsigned long long result()
    {
        return sum;
    }

    void stop_push()
    { }

    void start_push()
    { }
};


/** @brief Accumulate arguments. */
template <typename T, class Op = identity<T> >
class accumulate_tuple : public std::unary_function<stxxl::tuple<T, T>, void>
{
private:
    mutable unsigned long long sum1, sum2;
    Op op;

public:
    typedef stxxl::tuple<T, T> value_type;

    accumulate_tuple() : sum1(0), sum2(0) { }
    accumulate_tuple(Op & op) : sum1(0), sum2(0), op(op) { }

    inline const stxxl::tuple<T, T> & operator () (const stxxl::tuple<T, T> & t) const
    {
        sum1 += op(t.first)._key;
        sum2 += op(t.second)._key;
        assert(op(t.first)._key != 0);
        assert(op(t.first)._load != 0);
        assert(op(t.second)._key != 0);
        assert(op(t.second)._load != 0);
        //std::cerr << "+ " << t.first._key << " " << sum1 << " " << "+ " << t.second._key << " " << sum2 << std::endl;
        return t;
    }

    stxxl::tuple<unsigned long long, unsigned long long> result()
    {
        return stxxl::tuple<unsigned long long, unsigned long long>(sum1, sum2);
    }

    void stop_push()
    { }

    void start_push()
    { }
};

/** @brief Code snippet to keep the compiler from optimizing away the operations on @c res. */
#define ANTI_DEAD_CODE_ELIMINATION volatile unsigned long long * memory = new unsigned long long; *memory = res; delete memory;

/** @brief Delayed by a user-defined amount of time. */
template <typename T>
class some_delay : public std::unary_function<T, void>
{
private:
    unsigned long long duration;

public:
    typedef T value_type;

    some_delay(unsigned long long duration) : duration(duration)
    { }

    const T & operator () (const T & t) const
    {
        unsigned long long res = 0;
        for (unsigned long long i = 0; i < duration; i++)
            res += i;

        ANTI_DEAD_CODE_ELIMINATION

        return t;
    }
};


/** @brief Check order. */
template <typename T, class Comparator>
class check_order : public std::unary_function<T, void>
{
private:
    bool first, ordered;
    T last;
    Comparator & comp;

public:
    typedef T value_type;

    check_order(Comparator & comp) : first(true), ordered(true), comp(comp) { }

    const T & operator () (const T & t)
    {
        //std::cerr << t << std::endl;
        if (!first)
            ordered = ordered && !comp(t, last);
        first = false;
        last = t;
        return t;
    }

    bool result()
    {
        return ordered;
    }
};


/** @brief Delayed by a user-defined amount of time. */
template <typename T, class SecondStream>
class split2 : public std::unary_function<T, void>
{
private:
    SecondStream & second_stream;

public:
    typedef T value_type;

    split2(SecondStream & second_stream) : second_stream(second_stream)
    { }

    const T & operator () (const T & t)
    {
        second_stream.push(t);
        return t;
    }

    void stop_push()
    {
        second_stream.stop_push();
    }

    void start_push()
    {
        second_stream.start_push();
    }
};

/** @brief Exclusive or argument. */
template <typename T>
class exclusive_or : public std::unary_function<T, void>
{
private:
    unsigned long long operand;
    mutable T result;

public:
    typedef T value_type;

    exclusive_or(unsigned long long operand) : operand(operand)
    { }

    const T & operator () (const T & t) const
    {
        result = t;
        //std::cout << "before " << std::hex << result << std::endl;
        //result._key ^= operand;
        //std::cout << "after  " << std::hex << result << std::endl;
        return result;
    }

    void stop_push()
    { }

    void start_push()
    { }
};


extern stxxl::unsigned_type buffer_size;

std::ostream & operator << (std::ostream & o, const my_type & obj)
{
    o << obj._key << "/" << obj._load;
    return o;
}

inline std::ostream & operator << (std::ostream & o, const my_tuple & obj)
{
    o << obj.first << " " << obj.second;
    return o;
}

inline bool operator < (const my_type & a, const my_type & b)
{
    return a.key() < b.key();
}

inline bool operator > (const my_type & a, const my_type & b)
{
    return b < a;
}

inline bool operator != (const my_type & a, const my_type & b)
{
    return a.key() != b.key();
}

inline bool cmp_less_tuple::operator () (const my_tuple & t1, const my_tuple & t2)
{
    return t1.first < t2.first;
}
