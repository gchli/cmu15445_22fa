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
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

std::vector<std::vector<bool>> LockManager::lock_compatible_matrix =
// enum class LockMode { SHARED, EXCLUSIVE, INTENTION_SHARED, INTENTION_EXCLUSIVE, SHARED_INTENTION_EXCLUSIVE };
{
  //       S          X          IS        IX         SIX
  {true,  false, true, false, false}, // S
  {false, false, false,false, false}, // X
  {true,  false, true, true,  true},  // IS
  {false, false, true, true,  false}, // IX
  {false, false, true, false, false}  // SIX
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
    if (target_mode == LockMode::EXCLUSIVE || target_mode == LockMode::INTENTION_EXCLUSIVE) {
      is_upgradable = true;
    }
    break;
  case LockMode::SHARED_INTENTION_EXCLUSIVE:
    if (target_mode == LockMode::INTENTION_EXCLUSIVE) {
      is_upgradable = true;
    }
    break;
  default:
    break;
  }
  return is_upgradable;
}

auto LockManager::CanGrantLock(std::shared_ptr<LockRequestQueue> lock_request_queue, LockMode lock_mode) -> bool {

}

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  auto txn_state = txn->GetState();
  auto txn_iso_level = txn->GetIsolationLevel();
  bool is_granted = false;

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
      if (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
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

  table_lock_map_latch_.lock();
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
  }
  auto table_req_queue = table_lock_map_[oid];
  table_lock_map_latch_.unlock();
  table_req_queue->latch_.lock();

  bool upgrade_lock = false;
  /* traverse the request queue in FIFO order */
  for (auto lock_request: table_req_queue->request_queue_) {
    if (lock_request->txn_id_ == txn->GetTransactionId()) {

      /* requested lock mode is the same as that of the lock presently held */
      if (lock_request->lock_mode_ == lock_mode) {
        table_req_queue->latch_.unlock();
        return true;
      }

      /* only one transaction should be allowed to upgrade its lock on a given resource */
      if (table_req_queue->upgrading_ != INVALID_TXN_ID) {
        /* the lock is upgrading */
        table_req_queue->latch_.unlock();
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      }
      /* check whether the upgrade is considered compatible */
      if (!IsUpgradable(lock_request->lock_mode_, lock_mode)) {
        /* the lock is upgradable */
        table_req_queue->latch_.unlock();
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }

      /* all check passed, upgrade the lock */
      table_req_queue->upgrading_ = txn->GetTransactionId();
      table_req_queue->latch_.unlock();
      upgrade_lock = true;
      break;
    }
  }


  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool { return true; }

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool { return true; }

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {

}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool { return false; }

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
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
