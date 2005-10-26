#ifndef PAGER_HEADER
#define PAGER_HEADER

/***************************************************************************
 *            pager.h
 *
 *  Sun Oct  6 14:49:19 2002
 *  Copyright  2002  Roman Dementiev
 *  dementiev@mpi-sb.mpg.de
 ****************************************************************************/
#include "../common/utils.h"
#include "../common/rand.h"
#include "../common/simple_vector.h"
#include <list>

__STXXL_BEGIN_NAMESPACE

//! \weakgroup stlcont Containers
//! \ingroup stllayer
//! Containers with STL-compatible interface
//! \{
//! \}

//! \weakgroup stlcontinternals Internals
//! \ingroup stlcont
//! \{

enum pager_type
{
  random,
  lru
};

//! \brief Pager with \b random replacement strategy
template <unsigned DefNPages>
class random_pager
{
	random_number<random_uniform_fast> rnd;
	const unsigned npages_;
	
	random_pager(); // forbidden
public:
	
	enum { default_n_pages = DefNPages };

	random_pager(unsigned n_pages):  npages_(n_pages) {};
	~random_pager() {}
	int kick()
	{
		return rnd(npages_);
	};
	void hit(int ipage)
	{
		assert(ipage < int(npages_));
		assert(ipage >= 0);
	};
};

//! \brief Pager with \b LRU replacement strategy
template <unsigned DefNPages>
class lru_pager
{
	const unsigned npages_;
	typedef std::list<int> list_type;
	
	list_type history;
	simple_vector<list_type::iterator> history_entry;
	
private:
	lru_pager(const lru_pager &);
	lru_pager & operator = (const lru_pager &); // forbidden
	lru_pager();
public:
	
	enum { default_n_pages = DefNPages };
	
	lru_pager(unsigned npages): npages_(npages),history_entry(npages_)
	{
		for(unsigned i=0;i<npages_;i++)
			history_entry[i] = history.insert(history.end(),static_cast<int>(i));
	}
	~lru_pager() {}
	int kick()
	{
		return history.back();
	}
	void hit(int ipage)
	{
		assert(ipage < int(npages_));
		assert(ipage >= 0);
		history.splice(history.begin(),history,history_entry[ipage]);
	}
	void swap(lru_pager & obj)
	{
		std::swap(history,obj.history);
		std::swap(history_entry,obj.history_entry);
	}
};

//! \}

__STXXL_END_NAMESPACE

namespace std
{
	template <unsigned PgNr>
	void swap(	stxxl::lru_pager<PgNr> & a,
						stxxl::lru_pager<PgNr> & b)
	{
		a.swap(b);
	}
}

#endif
