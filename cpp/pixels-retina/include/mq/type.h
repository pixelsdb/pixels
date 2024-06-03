//
// Created by bian on 2020-10-22.
//

#ifndef STORAGE_MANAGER_TYPE_H
#define STORAGE_MANAGER_TYPE_H

#include <memory>

namespace stm {
typedef u_int8_t byte;

template <typename T> std::shared_ptr<T> makeSharedArray(size_t size) {
  return std::shared_ptr<T>(new T[size], std::default_delete<T[]>());
}
} // namespace stm

#endif // STORAGE_MANAGER_TYPE_H
