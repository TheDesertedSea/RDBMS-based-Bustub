#include "primer/trie.h"
#include <stack>
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.

  auto cur = root_;  // set the current node to the root
  if (!cur) {
    return nullptr;
  }

  while (!key.empty()) {
    auto temp_itr = cur->children_.find(key[0]);
    if (temp_itr == cur->children_.end()) {
      return nullptr;
    }
    cur = temp_itr->second;
    key.remove_prefix(1);
  }

  if (!key.empty()) {
    return nullptr;
  }

  auto value_node = dynamic_cast<const TrieNodeWithValue<T> *>(cur.get());
  if (value_node == nullptr) {
    return nullptr;
  }

  return value_node->value_.get();
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.

  std::shared_ptr<TrieNode> new_root;
  if (key.empty()) {
    // put value at root
    if (!root_) {
      new_root = std::make_shared<TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));
    } else {
      new_root = std::make_shared<TrieNodeWithValue<T>>(root_->children_, std::make_shared<T>(std::move(value)));
    }

    return Trie(new_root);
  }

  if (!root_) {
    new_root = std::make_shared<TrieNode>();
  } else {
    new_root = std::shared_ptr<TrieNode>(root_->Clone());
  }

  auto cur = new_root;
  // walk through the trie, find the upper node of the key, create new nodes if necessary
  while (key.length() > 1) {
    auto temp_itr = cur->children_.find(key[0]);
    std::shared_ptr<TrieNode> next;
    if (temp_itr == cur->children_.end()) {
      next = std::make_shared<TrieNode>();
    } else {
      next = std::shared_ptr<TrieNode>(temp_itr->second->Clone());
    }
    cur->children_[key[0]] = next;
    cur = next;
    key.remove_prefix(1);
  }
  // now cur is the upper node of the key

  auto temp_itr = cur->children_.find(key[0]);
  if (temp_itr == cur->children_.end()) {
    // create new node and attach it to the upper node
    cur->children_[key[0]] = std::make_shared<TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));
  } else {
    // overwrite the value
    cur->children_[key[0]] =
        std::make_shared<TrieNodeWithValue<T>>(temp_itr->second->children_, std::make_shared<T>(std::move(value)));
  }

  return Trie(new_root);
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.

  std::shared_ptr<TrieNode> new_root;
  if (!root_) {
    // empty trie
    return *this;
  }

  if (key.empty()) {
    // remove value at root
    if (root_->children_.empty()) {
      // root has no children, return empty trie
      return {};
    }

    // convert root to TrieNode
    new_root = std::make_shared<TrieNode>(root_->children_);

    return Trie(new_root);
  }

  new_root = std::shared_ptr<TrieNode>(root_->Clone());

  // create a stack to store the path from root to the node to be removed
  std::stack<std::pair<char, std::shared_ptr<TrieNode>>> node_stack;
  node_stack.push({0, new_root});  // the key of the root is not important

  auto cur = new_root;
  // walk through the trie, find the upper node of the key
  while (key.length() > 1) {
    auto temp_itr = cur->children_.find(key[0]);
    std::shared_ptr<TrieNode> next;
    if (temp_itr == cur->children_.end()) {
      // key not found
      return *this;
    }

    next = std::shared_ptr<TrieNode>(temp_itr->second->Clone());
    cur->children_[key[0]] = next;
    cur = next;
    node_stack.push({key[0], cur});  // store the path
    key.remove_prefix(1);
  }

  auto temp_itr = cur->children_.find(key[0]);
  if (temp_itr == cur->children_.end()) {
    // key not found
    return *this;
  }

  if (temp_itr->second->children_.empty()) {
    // need to remove the node
    auto cur_key = key[0];  // the key related to the node to be removed
    while (!node_stack.empty()) {
      auto [upper_key, upper_node] = node_stack.top();
      node_stack.pop();
      upper_node->children_.erase(cur_key);  // remove current node from the upper node
      if (!upper_node->children_.empty() || upper_node->is_value_node_) {
        // upper node needs to be kept
        break;
      }
      // upper node also needs to be removed, find the upper node of the upper node

      cur_key = upper_key;  // move the cursor to the upper node
    }

    // needs to check the root node, since no node above the root node in the stack
    if (new_root->children_.empty() && !new_root->is_value_node_) {
      // root needs to be removed
      return {};
    }
  } else {
    // convert the node to TrieNode
    cur->children_[key[0]] = std::make_shared<TrieNode>(temp_itr->second->children_);
  }

  return Trie(new_root);
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
