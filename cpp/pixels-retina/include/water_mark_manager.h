/*
 * Copyright 2017-2019 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
#pragma once
#include <atomic>

/**
 * @author mzp0514
 * @date 27/05/2022
 */

class WaterMarkManager {
 public:
  static void set_hwm(long hwm) {
    if (hwm > hwm_) hwm_ = hwm;
  }

  static void set_lwm(long lwm) {
    if (lwm > lwm_) lwm_ = lwm;
  }

  static long lwm() { return lwm_; }

  static long hwm() { return hwm_; }

 private:
  static std::atomic_long lwm_;
  static std::atomic_long hwm_;
};

std::atomic_long WaterMarkManager::lwm_(0);
std::atomic_long WaterMarkManager::hwm_(0);