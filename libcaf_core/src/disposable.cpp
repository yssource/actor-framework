// This file is part of CAF, the C++ Actor Framework. See the file LICENSE in
// the main distribution directory for license terms and copyright or visit
// https://github.com/actor-framework/actor-framework/blob/master/LICENSE.

#include "caf/disposable.hpp"

#include <algorithm>

#include "caf/make_counted.hpp"

namespace caf {

namespace {

class composite_impl : public ref_counted, public disposable::impl {
public:
  using disposable_list = std::vector<disposable>;

  composite_impl(disposable_list entries) : entries_(std::move(entries)) {
    // nop
  }

  void dispose() {
    for (auto& entry : entries_)
      entry.dispose();
  }

  bool disposed() const noexcept {
    auto is_disposed = [](const disposable& entry) { return entry.disposed(); };
    return std::all_of(entries_.begin(), entries_.end(), is_disposed);
  }

  void ref_disposable() const noexcept {
    ref();
  }

  void deref_disposable() const noexcept {
    deref();
  }

  friend void intrusive_ptr_add_ref(const composite_impl* ptr) noexcept {
    ptr->ref();
  }

  friend void intrusive_ptr_release(const composite_impl* ptr) noexcept {
    ptr->deref();
  }

private:
  std::vector<disposable> entries_;
};

} // namespace

disposable::impl::~impl() {
  // nop
}

disposable disposable::impl::as_disposable() noexcept {
  return disposable{intrusive_ptr<impl>{this}};
}

disposable disposable::make_composite(std::vector<disposable> entries) {
  if (entries.empty())
    return {};
  else
    return disposable{make_counted<composite_impl>(std::move(entries))};
}

} // namespace caf
