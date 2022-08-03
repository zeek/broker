#include "broker/alm/routing_table.hh"

namespace broker::alm {

const std::vector<endpoint_id>* shortest_path(const routing_table& tbl,
                                              const endpoint_id& peer) {
  if (auto i = tbl.find(peer);
      i != tbl.end() && !i->second.versioned_paths.empty())
    return std::addressof(i->second.versioned_paths.front().first);
  else
    return nullptr;
}

bool is_direct_connection(const routing_table_row& row) {
  return !row.versioned_paths.empty()
         && row.versioned_paths.front().first.size() == 1;
}

const routing_table_row* find_row(const routing_table& tbl,
                                  const endpoint_id& peer) {
  if (auto i = tbl.find(peer); i != tbl.end())
    return std::addressof(i->second);
  else
    return nullptr;
}

routing_table_row* find_row(routing_table& tbl, const endpoint_id& peer) {
  if (auto i = tbl.find(peer); i != tbl.end())
    return std::addressof(i->second);
  else
    return nullptr;
}

bool add_or_update_path(routing_table& tbl, const endpoint_id& peer,
                        std::vector<endpoint_id> path, vector_timestamp ts) {
  auto& row = tbl[peer];
  auto& paths = row.versioned_paths;
  auto i = std::lower_bound(paths.begin(), paths.end(), path, path_less);
  if (i == paths.end()) {
    paths.emplace_back(std::move(path), std::move(ts));
    return true;
  } else if (i->first != path) {
    paths.insert(i, std::make_pair(std::move(path), std::move(ts)));
    return true;
  } else {
    if (i->second < ts)
      i->second = std::move(ts);
    return false;
  }
}

} // namespace broker::alm
