#include "broker/alm/routing_table.hh"

namespace broker::alm {

optional<endpoint_id> get_peer_id(const routing_table& tbl,
                                  const caf::actor& hdl) {
  auto predicate = [&](const auto& kvp) { return kvp.second.hdl == hdl; };
  auto e = tbl.end();
  auto i = std::find_if(tbl.begin(), e, predicate);
  if (i != e)
    return i->first;
  return nil;
}

const std::vector<endpoint_id>* shortest_path(const routing_table& tbl,
                                              const endpoint_id& peer) {
  if (auto i = tbl.find(peer);
      i != tbl.end() && !i->second.versioned_paths.empty())
    return std::addressof(i->second.versioned_paths.front().first);
  else
    return nullptr;
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

bool add_or_update_path(routing_table& tbl,
                        const endpoint_id& peer,
                        std::vector<endpoint_id> path,
                        vector_timestamp ts) {
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
