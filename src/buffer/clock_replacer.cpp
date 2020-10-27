//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.cpp
//
// Identification: src/buffer/clock_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/clock_replacer.h"
#include <iostream>

namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages) {
	clock.reserve(num_pages);
	for (size_t i = 0; i < num_pages; i++) {
		clock.emplace_back(NOT_EXISTS, NO_REF);
	}
}

ClockReplacer::~ClockReplacer() = default;

bool ClockReplacer::Victim(frame_id_t *frame_id) { 
	std::lock_guard<std::shared_mutex> lock(latch);
	if (size == 0) {
		return false;
	}
	while (true) {
		assert(hand < clock.size());
		auto& entry = clock[hand];  // without &, it is a copy
		if (entry.first) { // exist
			if (entry.second) { // pinned
				entry.second = NO_REF;
			} else {
				// find a victim
				*frame_id = static_cast<frame_id_t>(hand);
				entry.first = NOT_EXISTS;
				size--;
				return true;
			}
		}
		hand = (hand + 1) % clock.size();
	}
}

void ClockReplacer::Pin(frame_id_t frame_id) {
	std::lock_guard<std::shared_mutex> lock(latch);
	assert(static_cast<size_t >(frame_id) <= clock.size());
	auto& pair = clock[frame_id];
  if (pair.first) {
    pair.first = NOT_EXISTS;
    size--;
  }
  pair.second = NO_REF;
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
	std::lock_guard<std::shared_mutex> lock(latch);
	assert(static_cast<size_t >(frame_id) <= clock.size());
	auto& pair = clock[frame_id];
	  if (!pair.first) {
	    pair.first = EXISTS;
	    size++;
	  }
	  pair.second = REF;
}

size_t ClockReplacer::Size() { 
	std::shared_lock<std::shared_mutex> shared_lock(latch);
	return size; 
}

}  // namespace bustub
