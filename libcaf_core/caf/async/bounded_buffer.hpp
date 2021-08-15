// This file is part of CAF, the C++ Actor Framework. See the file LICENSE in
// the main distribution directory for license terms and copyright or visit
// https://github.com/actor-framework/actor-framework/blob/master/LICENSE.

#pragma once

#include <cstdlib>
#include <mutex>

#include "caf/async/consumer.hpp"
#include "caf/async/producer.hpp"
#include "caf/config.hpp"
#include "caf/error.hpp"
#include "caf/intrusive_ptr.hpp"
#include "caf/make_counted.hpp"
#include "caf/raise_error.hpp"
#include "caf/ref_counted.hpp"
#include "caf/sec.hpp"
#include "caf/span.hpp"

namespace caf::async {

/// A bounded buffer for transmitting events from one producer to one consumer.
template <class T>
class bounded_buffer : public ref_counted {
public:
  using value_type = T;

  bounded_buffer(uint32_t max_in_flight, uint32_t min_pull_size)
    : max_in_flight_(max_in_flight), min_pull_size_(min_pull_size) {
    buf_ = reinterpret_cast<T*>(malloc(sizeof(T) * max_in_flight * 2));
  }

  ~bounded_buffer() {
    auto first = buf_ + rd_pos_;
    auto last = buf_ + wr_pos_;
    std::destroy(first, last);
    free(buf_);
  }

  /// Appends to the buffer and calls `on_producer_wakeup` on the consumer if
  /// the buffer becomes non-empty.
  void push(span<const T> items) {
    std::unique_lock guard{mtx_};
    CAF_ASSERT(producer_ != nullptr);
    CAF_ASSERT(!closed_);
    std::uninitialized_copy(items.begin(), items.end(), buf_ + wr_pos_);
    wr_pos_ += items.size();
    if (size() == items.size() && consumer_)
      consumer_->on_producer_wakeup();
  }

  /// Consumes up to `demand` items from the buffer with `fn`.
  /// @returns `true` if no more elements are available and the buffer has been
  ///          closed by the producer, `false` otherwise.
  template <class F>
  bool consume(size_t demand, F fn) {
    std::unique_lock guard{mtx_};
    CAF_ASSERT(demand > 0);
    CAF_ASSERT(consumer_ != nullptr);
    if (auto n = std::min(demand, size()); n > 0) {
      auto first = buf_ + rd_pos_;
      fn(make_span(first, n));
      std::destroy(first, first + n);
      rd_pos_ += n;
      shift_elements();
      signal_demand(n);
      return false;
    } else if (!closed_) {
      return false;
    } else {
      consumer_ = nullptr;
      return true;
    }
  }

  /// Closes the buffer by request of the producer.
  void close() {
    std::unique_lock guard{mtx_};
    CAF_ASSERT(producer_ != nullptr);
    closed_ = true;
    producer_ = nullptr;
    if (empty() && consumer_)
      consumer_->on_producer_wakeup();
  }

  /// Closes the buffer and signals an error by request of the producer.
  void abort(const error& reason) {
    std::unique_lock guard{mtx_};
    closed_ = true;
    producer_ = nullptr;
    if (consumer_) {
      consumer_->on_producer_abort(reason);
      consumer_ = nullptr;
    }
  }

  /// Closes the buffer by request of the consumer.
  void cancel() {
    std::unique_lock guard{mtx_};
    if (producer_)
      producer_->on_consumer_cancel();
    consumer_ = nullptr;
  }

  void set_consumer(consumer_ptr consumer) {
    CAF_ASSERT(consumer != nullptr);
    std::unique_lock guard{mtx_};
    if (consumer_)
      CAF_RAISE_ERROR("producer-consumer queue already has a consumer");
    consumer_ = std::move(consumer);
    if (producer_)
      ready();
  }

  void set_producer(producer_ptr producer) {
    CAF_ASSERT(producer != nullptr);
    std::unique_lock guard{mtx_};
    if (producer_)
      CAF_RAISE_ERROR("producer-consumer queue already has a producer");
    producer_ = std::move(producer);
    if (consumer_)
      ready();
  }

  size_t capacity() const noexcept {
    return max_in_flight_;
  }

private:
  void ready() {
    producer_->on_consumer_ready();
    consumer_->on_producer_ready();
  }

  size_t empty() const noexcept {
    CAF_ASSERT(wr_pos_ >= rd_pos_);
    return wr_pos_ == rd_pos_;
  }

  size_t size() const noexcept {
    CAF_ASSERT(wr_pos_ >= rd_pos_);
    return wr_pos_ - rd_pos_;
  }

  void shift_elements() {
    if (rd_pos_ >= max_in_flight_) {
      if (empty()) {
        rd_pos_ = 0;
        wr_pos_ = 0;
      } else {
        // No need to check for overlap: the first half of the buffer is
        // empty.
        auto first = buf_ + rd_pos_;
        auto last = buf_ + wr_pos_;
        std::uninitialized_move(first, last, buf_);
        std::destroy(first, last);
        wr_pos_ -= rd_pos_;
        rd_pos_ = 0;
      }
    }
  }

  void signal_demand(uint32_t new_demand) {
    demand_ += new_demand;
    if (demand_ >= min_pull_size_ && producer_) {
      producer_->on_consumer_demand(demand_);
      demand_ = 0;
    }
  }

  /// Guards access to all other member variables.
  std::mutex mtx_;

  /// Allocated to max_in_flight_ * 2, but at most holds max_in_flight_
  /// elements at any point in time. We dynamically shift elements into the
  /// first half of the buffer whenever rd_pos_ crosses the midpoint.
  T* buf_;

  /// Stores how many items the buffer may hold at any time.
  uint32_t max_in_flight_;

  /// Configures the minimum amount of free buffer slots that we signal to the
  /// producer.
  uint32_t min_pull_size_;

  /// Stores the read position of the consumer.
  uint32_t rd_pos_ = 0;

  /// Stores the write position of the producer.
  uint32_t wr_pos_ = 0;

  /// Demand that has not yet been signaled back to the producer.
  uint32_t demand_ = 0;

  /// Stores whether `close` has been called.
  bool closed_ = false;

  /// Callback handle to the consumer.
  consumer_ptr consumer_;

  /// Callback handle to the producer.
  producer_ptr producer_;
};

/// @relates bounded_buffer
template <class T>
using bounded_buffer_ptr = intrusive_ptr<bounded_buffer<T>>;

/// @relates bounded_buffer
template <class T, bool IsProducer>
struct resource_ctrl : ref_counted {
  using buffer_ptr = bounded_buffer_ptr<T>;

  explicit resource_ctrl(buffer_ptr ptr) : buf(std::move(ptr)) {
    // nop
  }

  ~resource_ctrl() {
    if (buf) {
      if constexpr (IsProducer) {
        auto err = make_error(sec::invalid_upstream,
                              "producer_resource destroyed without opening it");
        buf->abort(err);
      } else {
        buf->cancel();
      }
    }
  }

  buffer_ptr open() {
    std::unique_lock guard{mtx};
    if (buf) {
      auto res = buffer_ptr{};
      res.swap(buf);
      return res;
    }
  }

  std::mutex mtx;
  buffer_ptr buf;
};

/// Grants read access to the first consumer that calls `open` on the resource.
/// Cancels consumption of items on the buffer if the resources gets destroyed
/// before opening it.
/// @relates bounded_buffer
template <class T>
class consumer_resource {
public:
  using value_type = T;

  using buffer_type = bounded_buffer<T>;

  using buffer_ptr = bounded_buffer_ptr<T>;

  consumer_resource(buffer_ptr buf) {
    ctrl_.emplace(std::move(buf));
  }

  consumer_resource() = default;
  consumer_resource(consumer_resource&&) = default;
  consumer_resource(const consumer_resource&) = default;
  consumer_resource& operator=(consumer_resource&&) = default;
  consumer_resource& operator=(const consumer_resource&) = default;

  /// Tries to open the resource for reading from the buffer. The first `open`
  /// wins on concurrent access.
  /// @returns a pointer to the buffer on success, `nullptr` otherwise.
  buffer_ptr open() {
    if (ctrl_)
      return ctrl_->open();
    else
      return nullptr;
  }

private:
  intrusive_ptr<resource_ctrl<T, false>> ctrl_;
};

/// Grants access to a buffer to the first producer that calls `open`. Aborts
/// writes on the buffer if the resources gets destroyed before opening it.
/// @relates bounded_buffer
template <class T>
class producer_resource {
public:
  using value_type = T;

  using buffer_type = bounded_buffer<T>;

  using buffer_ptr = bounded_buffer_ptr<T>;

  producer_resource(buffer_ptr buf) {
    ctrl_.emplace(std::move(buf));
  }

  producer_resource() = default;
  producer_resource(producer_resource&&) = default;
  producer_resource(const producer_resource&) = default;
  producer_resource& operator=(producer_resource&&) = default;
  producer_resource& operator=(const producer_resource&) = default;

  /// Tries to open the resource for writing to the buffer. The first `open`
  /// wins on concurrent access.
  /// @returns a pointer to the buffer on success, `nullptr` otherwise.
  buffer_ptr open() {
    if (ctrl_)
      return ctrl_->open();
    else
      return nullptr;
  }

private:
  intrusive_ptr<resource_ctrl<T, true>> ctrl_;
};

} // namespace caf::async
