#include "concurrency/watermark.h"
#include <cstdint>
#include <ctime>
#include <exception>
#include <limits>
#include "common/exception.h"
#include "storage/table/tuple.h"

namespace bustub {

auto Watermark::AddTxn(timestamp_t read_ts) -> void {
  if (read_ts < commit_ts_) {
    throw Exception("read ts < commit ts");
  }

  // TODO(fall2023): implement me!
  if (current_reads_.empty()) {
    watermark_ = read_ts;
  }

  current_reads_[read_ts]++;
}

auto Watermark::RemoveTxn(timestamp_t read_ts) -> void {
  // TODO(fall2023): implement me!

  if (current_reads_.find(read_ts) == current_reads_.end()) {
    throw Exception("read ts not found");
  }

  if (--current_reads_[read_ts] == 0) {
    current_reads_.erase(read_ts);
    if (read_ts == watermark_) {
      // update watermark
      if (current_reads_.empty()) {
        watermark_ = commit_ts_;
      } else {
        watermark_ = current_reads_.begin()->first;
      }
    }
  }
}

}  // namespace bustub
