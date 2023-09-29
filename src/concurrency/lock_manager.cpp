//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"
#include <memory>
#include <shared_mutex>

#include "common/config.h"
#include "common/logger.h"
#include "common/rid.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

std::vector<std::vector<bool>> LockManager::lock_compatible_matrix = {
    // enum class LockMode { SHARED, EXCLUSIVE, INTENTION_SHARED, INTENTION_EXCLUSIVE, SHARED_INTENTION_EXCLUSIVE };
    //       S          X          IS        IX         SIX
    {true, false, true, false, false},    // S
    {false, false, false, false, false},  // X
    {true, false, true, true, true},      // IS
    {false, false, true, true, false},    // IX
    {false, false, true, false, false}    // SIX
};

auto LockManager::IsUpgradable(LockMode cur_mode, LockMode target_mode) -> bool {
  bool is_upgradable = false;
  switch (cur_mode) {
    case LockMode::INTENTION_SHARED:
      if (target_mode != LockMode::INTENTION_SHARED) {
        is_upgradable = true;
      }
      break;
    case LockMode::SHARED:
    case LockMode::INTENTION_EXCLUSIVE:
      if (target_mode == LockMode::EXCLUSIVE || target_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
        is_upgradable = true;
      }
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      if (target_mode == LockMode::EXCLUSIVE) {
        is_upgradable = true;
      }
      break;
    default:
      break;
  }
  return is_upgradable;
}

auto LockManager::CanGrantTableLock(std::shared_ptr<LockRequestQueue> &lock_request_queue,
                                    std::shared_ptr<LockRequest> &lock_request) -> bool {
  bool is_granted = true;
  for (const auto &request : lock_request_queue->request_queue_) {
    if (request->txn_id_ == lock_request->txn_id_) {
      // some better choices to check the equality of the requests?
      break;
    }
    auto *txn = TransactionManager::GetTransaction(request->txn_id_);
    if (txn->GetState() == TransactionState::ABORTED) {
      continue;
    }
    // if (!request->granted_) {
    //   return false;
    // }
    if (!IsCompatable(request->lock_mode_, lock_request->lock_mode_)) {
      return false;
    }
  }
  return is_granted;
}

auto LockManager::CanGrantRowLock(std::shared_ptr<LockRequestQueue> &lock_request_queue,
                                  std::shared_ptr<LockRequest> &lock_request) -> bool {
  bool is_granted = true;
  for (const auto &request : lock_request_queue->request_queue_) {
    if (request->txn_id_ == lock_request->txn_id_ && request->oid_ == lock_request->oid_) {
      // some better choices to check the equality of the requests?
      break;
    }
    auto *txn = TransactionManager::GetTransaction(request->txn_id_);
    if (txn->GetState() == TransactionState::ABORTED) {
      continue;
    }
    // if (!request->granted_) {
    //   return false;
    // }
    if (!IsCompatable(request->lock_mode_, lock_request->lock_mode_)) {
      return false;
    }
  }
  return is_granted;
}

auto LockManager::TrackTableLock(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, bool remove_lock) {
  switch (lock_mode) {
    case LockMode::SHARED:
      if (remove_lock) {
        txn->GetSharedTableLockSet()->erase(oid);
      } else {
        txn->GetSharedTableLockSet()->insert(oid);
      }
      break;
    case LockMode::EXCLUSIVE:
      if (remove_lock) {
        txn->GetExclusiveTableLockSet()->erase(oid);
      } else {
        txn->GetExclusiveTableLockSet()->insert(oid);
      }
      break;
    case LockMode::INTENTION_SHARED:
      if (remove_lock) {
        txn->GetIntentionSharedTableLockSet()->erase(oid);
      } else {
        txn->GetIntentionSharedTableLockSet()->insert(oid);
      }
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      if (remove_lock) {
        txn->GetIntentionExclusiveTableLockSet()->erase(oid);
      } else {
        txn->GetIntentionExclusiveTableLockSet()->insert(oid);
      }
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      if (remove_lock) {
        txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
      } else {
        txn->GetSharedIntentionExclusiveTableLockSet()->insert(oid);
      }
      break;
    default:
      LOG_ERROR("wrong table lock mode");
      break;
  }
}

auto LockManager::TrackRowLock(Transaction *txn, LockMode lock_mode, const oid_t &oid, const RID &rid,
                               bool remove_lock) {
  switch (lock_mode) {
    case LockMode::SHARED:
      if (remove_lock) {
        txn->GetSharedRowLockSet()->at(oid).erase(rid);
      } else {
        (*txn->GetSharedRowLockSet())[oid].insert(rid);
      }
      break;
    case LockMode::EXCLUSIVE:
      if (remove_lock) {
        txn->GetExclusiveRowLockSet()->at(oid).erase(rid);
      } else {
        (*txn->GetExclusiveRowLockSet())[oid].insert(rid);
        // txn->GetExclusiveRowLockSet()->at(oid).insert(rid);
      }
      break;
    default:
      LOG_ERROR("wrong row lock mode");
      break;
  }
}

auto LockManager::IsTableLocked(Transaction *txn, const table_oid_t &oid) -> bool {
  return txn->IsTableExclusiveLocked(oid) || txn->IsTableSharedLocked(oid) ||
         txn->IsTableIntentionExclusiveLocked(oid) || txn->IsTableIntentionSharedLocked(oid) ||
         txn->IsTableSharedIntentionExclusiveLocked(oid);
}

auto LockManager::IsRowTableLockCompatable(Transaction *txn, const table_oid_t &oid, const LockMode &row_lock_mode)
    -> bool {
  if (row_lock_mode == LockMode::EXCLUSIVE) {
    return !txn->IsTableSharedLocked(oid) && !txn->IsTableIntentionSharedLocked(oid);
  }

  return IsTableLocked(txn, oid);
}

auto LockManager::IsRowLocked(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool {
  return txn->IsRowExclusiveLocked(oid, rid) || txn->IsRowSharedLocked(oid, rid);
}

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  // need txn latch?
  auto txn_state = txn->GetState();
  auto txn_iso_level = txn->GetIsolationLevel();

  /* validate the lock mode with the isolation level. */
  switch (txn_iso_level) {
    case IsolationLevel::REPEATABLE_READ:
      if (txn_state == TransactionState::SHRINKING) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      }
      break;
    case IsolationLevel::READ_COMMITTED:
      if (txn_state == TransactionState::SHRINKING) {
        if (lock_mode != LockMode::SHARED && lock_mode != LockMode::INTENTION_SHARED) {
          txn->SetState(TransactionState::ABORTED);
          throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
        }
      }
      break;
    case IsolationLevel::READ_UNCOMMITTED:
      if (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED ||
          lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      }
      if (txn_state == TransactionState::SHRINKING) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      }
      break;
    default:
      LOG_ERROR("wrong isolation level");
      break;
  }

  auto new_lock_req = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);

  // auto new_lock_req = new LockRequest(txn->GetTransactionId(), lock_mode, oid);
  std::unique_lock<std::mutex> table_lock_map_latch(table_lock_map_latch_);
  /* get the lock request queue. If not exists, create a new one. */
  /* always grant the first lock of the table*/
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    auto new_lock_req_queue = std::make_shared<LockRequestQueue>();
    table_lock_map_[oid] = new_lock_req_queue;
    new_lock_req_queue->request_queue_.emplace_back(new_lock_req);
    new_lock_req->granted_ = true;
    TrackTableLock(txn, lock_mode, oid);
    return true;
  }

  auto table_req_queue = table_lock_map_[oid];
  std::unique_lock<std::mutex> table_latch(table_req_queue->latch_);
  table_lock_map_latch_.unlock();
  bool upgrade_lock = false;
  /* traverse the request queue in FIFO order */
  /* may be can find the req first and use for_each to check the compatibility? */

  auto req_it = table_req_queue->request_queue_.begin();
  for (; req_it != table_req_queue->request_queue_.end(); req_it++) {
    if ((*req_it)->txn_id_ == txn->GetTransactionId()) {
      /* requested lock mode is the same as that of the lock presently held */
      if ((*req_it)->lock_mode_ == lock_mode) {
        return true;
      }

      /* only one transaction should be allowed to upgrade its lock on a given resource */
      if (table_req_queue->upgrading_ != INVALID_TXN_ID) {
        /* the lock is upgrading */
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      }

      /* check whether the upgrade is considered compatible */
      if (!IsUpgradable((*req_it)->lock_mode_, lock_mode)) {
        /* the lock is upgradable */
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }

      /* all check passed, upgrade the lock, should we release all row locks? */
      table_req_queue->upgrading_ = txn->GetTransactionId();
      // new_lock_req = lock_request;
      upgrade_lock = true;
      break;
    }
  }

  if (upgrade_lock) {
    TrackTableLock(txn, (*req_it)->lock_mode_, oid, true);
    table_req_queue->request_queue_.erase(req_it);
    auto insert_it =
        std::find_if(table_req_queue->request_queue_.begin(), table_req_queue->request_queue_.end(),
                     [](const std::shared_ptr<LockRequest> &lock_request) { return !lock_request->granted_; });
    req_it = table_req_queue->request_queue_.insert(insert_it, new_lock_req);
    new_lock_req->lock_mode_ = lock_mode;
  } else {
    req_it = table_req_queue->request_queue_.insert(table_req_queue->request_queue_.end(), new_lock_req);
  }

  table_req_queue->cv_.wait(table_latch, [&]() -> bool {
    return txn->GetState() == TransactionState::ABORTED || CanGrantTableLock(table_req_queue, new_lock_req);
  });

  if (txn->GetState() == TransactionState::ABORTED) {
    table_req_queue->request_queue_.erase(req_it);
    return false;
  }

  new_lock_req->granted_ = true;
  if (upgrade_lock) {
    table_req_queue->upgrading_ = INVALID_TXN_ID;
  }
  TrackTableLock(txn, lock_mode, oid);
  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  if (!IsTableLocked(txn, oid)) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  auto shared_row_lock_set = txn->GetSharedRowLockSet();
  auto exclusive_row_lock_set = txn->GetExclusiveRowLockSet();
  if (shared_row_lock_set->find(oid) != shared_row_lock_set->end() && !shared_row_lock_set->at(oid).empty()) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }
  if (exclusive_row_lock_set->find(oid) != exclusive_row_lock_set->end() && !exclusive_row_lock_set->at(oid).empty()) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }

  auto txn_iso_level = txn->GetIsolationLevel();
  std::unique_lock<std::mutex> table_lock_map_latch(table_lock_map_latch_);
  auto table_req_queue = table_lock_map_[oid];
  LockMode lock_mode;
  {
    std::unique_lock<std::mutex> table_req_queue_latch(table_req_queue->latch_);
    table_lock_map_latch.unlock();
    auto lock_req_it = std::find_if(table_req_queue->request_queue_.begin(), table_req_queue->request_queue_.end(),
                                    [txn](const std::shared_ptr<LockRequest> &lock_request) {
                                      return lock_request->txn_id_ == txn->GetTransactionId();
                                    });

    assert(lock_req_it != table_req_queue->request_queue_.end());
    lock_mode = (*lock_req_it)->lock_mode_;
    table_req_queue->request_queue_.erase(lock_req_it);
    table_req_queue->cv_.notify_all();
  }
  if (txn->GetState() == TransactionState::GROWING) {
    switch (txn_iso_level) {
      case IsolationLevel::REPEATABLE_READ:
        if (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::SHARED) {
          txn->SetState(TransactionState::SHRINKING);
        }
        break;
      case IsolationLevel::READ_COMMITTED:
        if (lock_mode == LockMode::EXCLUSIVE) {
          txn->SetState(TransactionState::SHRINKING);
        }
        break;
      case IsolationLevel::READ_UNCOMMITTED:
        if (lock_mode == LockMode::EXCLUSIVE) {
          txn->SetState(TransactionState::SHRINKING);
        }
        if (lock_mode == LockMode::SHARED) {
          txn->SetState(TransactionState::ABORTED);
          throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
        }
        break;
      default:
        LOG_ERROR("wrong isolation level");
        break;
    }
  }

  TrackTableLock(txn, lock_mode, oid, true);
  return true;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  auto txn_state = txn->GetState();
  auto txn_iso_level = txn->GetIsolationLevel();
  /* row can't be locked by intension locks. */
  if (lock_mode == LockMode::INTENTION_SHARED || lock_mode == LockMode::INTENTION_EXCLUSIVE ||
      lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }

  /* validate the lock mode with the isolation level. */
  switch (txn_iso_level) {
    case IsolationLevel::REPEATABLE_READ:
      if (txn_state == TransactionState::SHRINKING) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      }
      break;
    case IsolationLevel::READ_COMMITTED:
      if (txn_state == TransactionState::SHRINKING) {
        if (lock_mode != LockMode::SHARED) {
          txn->SetState(TransactionState::ABORTED);
          throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
        }
      }
      break;
    case IsolationLevel::READ_UNCOMMITTED:
      if (lock_mode == LockMode::SHARED) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      }
      if (txn_state == TransactionState::SHRINKING) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      }
      break;
    default:
      LOG_ERROR("wrong isolation level");
      break;
  }

  /* if table isn't locked or the row wants x lock but the table only locked by s or is, the row data can't be locked.
   */
  if (!IsRowTableLockCompatable(txn, oid, lock_mode)) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
  }

  /* get the lock request queue. If not exists, create a new one. */
  auto new_lock_req = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);

  /* always grant the first lock of the table*/
  std::unique_lock<std::mutex> row_lock_map_latch(row_lock_map_latch_);
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    auto new_row_lock_map = std::make_shared<LockRequestQueue>();
    row_lock_map_[rid] = new_row_lock_map;
    new_row_lock_map->request_queue_.emplace_back(new_lock_req);
    new_lock_req->granted_ = true;
    TrackRowLock(txn, lock_mode, oid, rid);
    return true;
  }

  auto row_req_queue = row_lock_map_[rid];
  std::unique_lock<std::mutex> row_latch(row_req_queue->latch_);
  row_lock_map_latch.unlock();
  bool upgrade_lock = false;
  auto req_it = row_req_queue->request_queue_.begin();
  /* can be replaced by find_if */
  for (; req_it != row_req_queue->request_queue_.end(); ++req_it) {
    if ((*req_it)->txn_id_ == txn->GetTransactionId() && (*req_it)->oid_ == oid) {
      /* requested lock mode is the same as that of the lock presently held */
      if ((*req_it)->lock_mode_ == lock_mode) {
        return true;
      }

      /* only one transaction should be allowed to upgrade its lock on a given resource */
      if (row_req_queue->upgrading_ != INVALID_TXN_ID) {
        /* the lock is upgrading */
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      }

      if (!IsUpgradable((*req_it)->lock_mode_, lock_mode)) {
        /* the lock is upgradable */
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }
      /* all check passed, upgrade the lock */
      row_req_queue->upgrading_ = txn->GetTransactionId();
      upgrade_lock = true;
      break;
    }
  }

  if (upgrade_lock) {
    TrackRowLock(txn, (*req_it)->lock_mode_, oid, rid, true);
    row_req_queue->request_queue_.erase(req_it);
    auto insert_it =
        std::find_if(row_req_queue->request_queue_.begin(), row_req_queue->request_queue_.end(),
                     [](const std::shared_ptr<LockRequest> &lock_request) { return !lock_request->granted_; });
    req_it = row_req_queue->request_queue_.insert(insert_it, new_lock_req);
  } else {
    req_it = row_req_queue->request_queue_.insert(row_req_queue->request_queue_.end(), new_lock_req);
  }

  row_req_queue->cv_.wait(row_latch, [&]() -> bool {
    return txn->GetState() == TransactionState::ABORTED || CanGrantRowLock(row_req_queue, new_lock_req);
  });

  if (txn->GetState() == TransactionState::ABORTED) {
    row_req_queue->request_queue_.erase(req_it);
    return false;
  }
  new_lock_req->granted_ = true;
  if (upgrade_lock) {
    row_req_queue->upgrading_ = INVALID_TXN_ID;
  }
  TrackRowLock(txn, lock_mode, oid, rid);
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool {
  if (!IsRowLocked(txn, oid, rid)) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  auto shared_row_lock_set = txn->GetSharedRowLockSet();
  auto exclusive_row_lock_set = txn->GetExclusiveRowLockSet();
  LockMode prev_lock_mode = txn->IsRowSharedLocked(oid, rid) ? LockMode::SHARED : LockMode::EXCLUSIVE;

  auto txn_iso_level = txn->GetIsolationLevel();
  std::unique_lock<std::mutex> row_lock_map_latch(row_lock_map_latch_);
  auto row_req_queue = row_lock_map_[rid];
  // LockMode lock_mode;
  {
    std::unique_lock<std::mutex> row_req_queue_latch(row_req_queue->latch_);
    row_lock_map_latch.unlock();
    auto lock_req_it =
        std::find_if(row_req_queue->request_queue_.begin(), row_req_queue->request_queue_.end(),
                     [txn, oid](const std::shared_ptr<LockRequest> &lock_request) {
                       return lock_request->txn_id_ == txn->GetTransactionId() || lock_request->oid_ == oid;
                     });

    assert(lock_req_it != row_req_queue->request_queue_.end());
    row_req_queue->request_queue_.erase(lock_req_it);
    // lock_mode = (*lock_req_it)->lock_mode_;
    row_req_queue->cv_.notify_all();
  }
  if (txn->GetState() == TransactionState::GROWING) {
    switch (txn_iso_level) {
      case IsolationLevel::REPEATABLE_READ:
        if (prev_lock_mode == LockMode::EXCLUSIVE || prev_lock_mode == LockMode::SHARED) {
          txn->SetState(TransactionState::SHRINKING);
        }
        break;
      case IsolationLevel::READ_COMMITTED:
        if (prev_lock_mode == LockMode::EXCLUSIVE) {
          txn->SetState(TransactionState::SHRINKING);
        }
        break;
      case IsolationLevel::READ_UNCOMMITTED:
        if (prev_lock_mode == LockMode::EXCLUSIVE) {
          txn->SetState(TransactionState::SHRINKING);
        }
        if (prev_lock_mode == LockMode::SHARED) {
          txn->SetState(TransactionState::ABORTED);
          throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
        }
        break;
      default:
        LOG_ERROR("wrong isolation level");
        break;
    }
  }
  TrackRowLock(txn, prev_lock_mode, oid, rid, true);
  return true;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  auto &vec = waits_for_[t1];
  if (std::find(vec.begin(), vec.end(), t2) != vec.end()) {
    return;
  }
  vec.push_back(t2);
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  if (waits_for_.find(t1) == waits_for_.end()) {
    return;
  }
  auto &vec = waits_for_[t1];
  if (auto it = std::find(vec.begin(), vec.end(), t2); it != vec.end()) {
    vec.erase(it);
  }
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool { return false; }

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  for (const auto &[t1, vec] : waits_for_) {
    for (const auto t2 : vec) {
      edges.emplace_back(t1, t2);
    }
  }
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
    }
  }
}

}  // namespace bustub
