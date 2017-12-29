// -*- C++ -*-

#ifndef _INFINIBUF_H_
#define _INFINIBUF_H_ 1

/** \file infinibuf.h
 *  \brief iostreams-friendly buffers that can grow without bounds.
 */

#include <condition_variable>
#include <list>
#include <memory>
#include <thread>

/**
 * \brief Abstract buffer-management class for unbounded buffers.
 *
 * A derived class must at a minimum override either `notempty()` (for
 * output buffers) or `gwait()` (for input buffers).
 *
 * Most methods are not thread-safe.
 */
class infinibuf {
protected:
  static constexpr int default_startpos_ = 8;
  static constexpr int chunksize_ = 0x10000;

  std::list<char *> data_;
  int gpos_;
  int ppos_;
  bool eof_{false};
  int errno_{0};
  const int startpos_;		// For putback

  /** Called to signal when the buffer transitions from empty to
   *  non-empty. */
  virtual void notempty() {}

  /** Called when sufficient bytes are consumed to free some memory. */
  virtual void notfull() {}

public:
  explicit infinibuf(int sp = default_startpos_)
    : gpos_(sp), ppos_(sp), startpos_(sp) {
    data_.push_back(new char[chunksize_]);
  }
  infinibuf(const infinibuf &) = delete;
  virtual ~infinibuf() = 0;
  infinibuf &operator= (const infinibuf &) = delete;
		   
  // These functions are never thread safe:

  bool empty() { return data_.front() == data_.back() && gpos_ == ppos_; }
  bool eof() { return eof_; }
  std::size_t buffer_size() { return data_.size() * chunksize_; }
  int err() { return errno_; }
  void err(int num) { if (!errno_) errno_ = num; peof(); }

  char *eback() { return data_.front(); }
  char *gptr() { return eback() + gpos_; }
  int gsize() {
    return (data_.front() == data_.back() ? ppos_ : chunksize_) - gpos_;
  }
  char *egptr() { return gptr() + gsize(); }
  void gbump(int n);
  /** Called to wait for the buffer to be non-empty. */
  virtual void gwait() {}

  char *pbase() { return data_.back(); }
  char *pptr() { return pbase() + ppos_; }
  int psize() { return chunksize_ - ppos_; }
  char *epptr() { return pptr() + psize(); }
  void pbump(int n);
  void peof() { eof_ = true; if (empty()) notempty(); }
  /** Called to sleep if the buffer is too full. */
  virtual void pwait() {}

  // These functions are thread safe for some subtypes:

  /** By default `lock()` and `unlock()` do nothing, but threadsafe
   *  derived classes must override these functions. */
  virtual void lock() {}
  /** See comment at lock. */
  virtual void unlock() {}

  /** \brief Drain the current contents of the buffer.
   *
   * This function is thread safe and must be called *without* locking
   * the `infinibuf`.  If the `infinibuf` is already locked, deadlock
   * will ensue.
   *
   * \param fd The file descriptor to write to.
   * \return 0 at EOF if there is no point in ever calling `output`
   * again, -1 after EAGAIN, and 1 after successful output.
   * \throws runtime_error if the `write` system call fails and
   * `errno` is not `EAGAIN`. */
  int output(int fd);

  /** Fill the buffer from a file descriptor.
   *
   * This function is thread safe and must be called *without* locking
   * the `infinibuf`.
   *
   * \param fd The file descriptor to read from.
   * \return 0 at EOF if there is no point in ever calling
   * `input` again, 1 after successful input, and -1 after EAGAIN.
   * \throws runtime_error if the `read` system call fails and
   * `errno` is not `EAGAIN`. */
  int input(int fd);

  /** Calls `output` over and over in a loop on an `infinibuf`.
   *
   * \param ib The `infinibuf` on which to call `output`.
   *
   * \param fd The file descriptor to which to write consumed data.
   *
   * \param oblocked If non-null is called with `true` whenever the
   * output is blocked by flow control, and then called again with
   * `false` when the output becomes unblocked.
   */
  static void output_loop(std::shared_ptr<infinibuf> ib, int fd,
			  std::function<void(bool)> oblocked = nullptr);
  static void input_loop(std::shared_ptr<infinibuf> ib, int fd);
};

/** \brief An `infinibuf` that synchronously reads from a file
 *  descriptor when the buffer underflows.
 *
 *  Closes the file descriptor upon destruction. */
class infinibuf_infd : public infinibuf {
  const int fd_;
public:
  explicit infinibuf_infd (int fd, int sp = default_startpos_)
    : infinibuf(sp), fd_(fd) {}
  ~infinibuf_infd();
  void gwait() override { input(fd_); }
};

/** \brief An `infinibuf` that synchronously writes to a file
 *  descriptor when the buffer overflows or is synced.
 *
 *  Closes the file descriptor upon destruction. */
class infinibuf_outfd : public infinibuf {
  const int fd_;
  std::function<void(bool)> oblocked_;
  
public:
  explicit infinibuf_outfd (int fd,
			    std::function<void(bool)> oblocked = nullptr);
  ~infinibuf_outfd();
  void notempty() override;
};

/** \brief Thread-safe infinibuf.
 *
 * This infinibuf can safely be used in an `iostream` by one thread,
 * while a different thread fills or drains the buffer (for instance
 * executing `infinibuf::output_loop` or `infinibuf::input_loop`).
 */
class infinibuf_mt : public infinibuf {
  std::mutex m_;
  std::condition_variable cv_;
  std::condition_variable flow_ctrl_cv_;
  std::size_t max_buf_size_{0};
public:
  explicit infinibuf_mt (int sp = default_startpos_) : infinibuf(sp) {}
  void lock() override { m_.lock(); }
  void unlock() override { m_.unlock(); }
  void notempty() override { cv_.notify_all(); }
  void notfull() override { flow_ctrl_cv_.notify_all(); }
  void set_max_buf_size(std::size_t val) {
    std::lock_guard<infinibuf> _lk(*this);
    if (!val || val > max_buf_size_)
      notfull();
    max_buf_size_ = val;
  }
  void gwait() override {
    if (empty() && !eof()) {
      std::unique_lock<std::mutex> ul (m_, std::adopt_lock);
      while (empty() && !eof())
	cv_.wait(ul);
      ul.release();
    }
  }
  void pwait() override {
    if (max_buf_size_ && buffer_size() > max_buf_size_) {
      if (max_buf_size_ && buffer_size() > max_buf_size_) {
	std::unique_lock<std::mutex> ul (m_, std::adopt_lock);
	flow_ctrl_cv_.wait(ul);
	ul.release();
      }
    }
  }
};

/** \brief `infinibuf`-based `streambuf`.
 *
 * This streambuf can make use of any buffer type derived from
 * `infinibuf`.  The `infinibuf` is always converted to a
 * `shared_ptr`, even if it is passed in as a raw `infinibuf*`.
 */
class infinistreambuf : public std::streambuf {
protected:
  std::shared_ptr<infinibuf> ib_;
  int_type underflow() override;
  int_type overflow(int_type ch) override;
  int sync() override;
public:
  explicit infinistreambuf(std::shared_ptr<infinibuf> ib);
  explicit infinistreambuf(infinibuf *ib)
    : infinistreambuf(std::shared_ptr<infinibuf>(ib)) {}
  infinistreambuf(infinistreambuf &&isb)
    : infinistreambuf(isb.ib_) {}
  std::shared_ptr<infinibuf> get_infinibuf() { return ib_; }
  void sputeof();
};

class ifdstream : public std::istream {
  infinistreambuf isb_;
public:
  ifdstream(int fd)
    : std::istream (nullptr), isb_ (new infinibuf_infd(fd)) {
    init(&isb_);
  }
  ~ifdstream() {
    std::lock_guard<infinibuf> _lk (*isb_.get_infinibuf());
    isb_.get_infinibuf()->err(EPIPE);
  }
};

class ofdstream : public std::ostream {
  infinistreambuf isb_;
public:
  ofdstream(int fd, std::function<void(bool)> oblocked = nullptr)
    : std::ostream (nullptr), isb_(new infinibuf_outfd(fd, oblocked)) {
    init(&isb_);
  }
  ~ofdstream() {
    if (std::uncaught_exception())
      try { isb_.sputeof(); } catch(...) {}
    else
      isb_.sputeof();
  }
};

/** \brief std::istream from file descriptor with unbounded buffer.
 *
 * Continously reads from and buffers input from a file descriptor in
 * another thread.  Closes the file descriptor after receiving EOF.
 * Kill the input thread if any further input is received, but the
 * input thread could get stuck if no input and no EOF happens.
 * Maximum buffer size defaults to infinity but can be adjusted with
 * `ifdinfinistream::set_max_buf_size`.
 */
class ifdinfinistream : public std::istream {
  std::shared_ptr<infinibuf_mt> ib_ { new infinibuf_mt() };
  infinistreambuf isb_ { ib_ };
public:
  explicit ifdinfinistream (int fd, std::size_t size = 0)
    : std::istream (nullptr) {
    set_max_buf_size(size);
    std::thread t (infinibuf::input_loop, isb_.get_infinibuf(), fd);
    t.detach();
    init(&isb_);
  }
  /** Sets maximum buffer size, above which it will stop reading from
   * the file descriptor until more is consumed locally.
   *
   * A value of 0 means no maximum buffer size.  */
  void set_max_buf_size(std::size_t size) { ib_->set_max_buf_size(size); }
  ~ifdinfinistream() {
    std::lock_guard<infinibuf> _lk (*isb_.get_infinibuf());
    // Sadly, there appears to be no portable way of waking up the
    // thread waiting in read.  I tried using dup2 to replace the file
    // descriptor with /dev/null, or using fcntl to set the O_NONBLOCK
    // flag after the read has already started, and neither works on
    // linux.  What does work is setting an empty function (not
    // SIG_IGN) as the signal handler on SIGCONT, then setting
    // O_NONBLOCK on the file descriptor, and finally calling
    // pthread_kill(t.native_handle(), SIGCONT)--but that could have
    // unintended consequences on other parts of the program following
    // a Ctrl-Z.  The only truly clean solution is to use a
    // "self-pipe" to wake up a poll call, thereby using three file
    // descriptors for the job of one (yuck).  Since we don't really
    // need to clean up the file descriptor, I'm not going to add the
    // complexity and cost of polling a second "self-pipe" file
    // descriptor or dropping down to native_handle.
    isb_.get_infinibuf()->err(EPIPE);
  }
};

#if 0
/** \brief `ostream` from file descriptor with unbounded buffer.
 *
 * Buffers unbounded amounts of data which are drained to a file
 * descriptor in another thread.  The file descriptor is closed when
 * the draining thread exits.  The class destructor waits for the
 * writer thread to flush the buffer and exit.
 */
class ofdinfinistream : public std::ostream {
  infinistreambuf isb_ { new infinibuf_mt(0) };
  std::thread t_;
public:
  ofdinfinistream (int fd) {
    std::thread t (infinibuf::output_loop, isb_.get_infinibuf(), fd, nullptr);
    t_ = std::move(t);
    rdbuf(&isb_);
  }
  // Doesn't work because std::ostream's virtual destructor is noexcept.
  ~ofdinfinistream() noexcept(false) {
    isb_.sputeof();
    if (!std::uncaught_exception()) {
      t_.join();
      std::lock_guard<infinibuf> lk (*isb_.get_infinibuf());
      if (isb_.get_infinibuf()->err())
	throw std::runtime_error (std::string("~ofdinfinistream: ") +
				  strerror(isb_.get_infinibuf()->err()));
    }
  }
};
#endif

#endif /* !_INFINIBUF_H_ */
