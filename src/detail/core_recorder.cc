#include "broker/detail/core_recorder.hh"

#include <caf/actor_system_config.hpp>
#include <caf/config_value.hpp>
#include <caf/local_actor.hpp>
#include <caf/node_id.hpp>

#include "broker/defaults.hh"
#include "broker/detail/filesystem.hh"
#include "broker/logger.hh"

namespace broker::detail {

core_recorder::core_recorder(caf::local_actor* self) {
  auto& cfg = self->config();
  auto meta_dir = get_or(cfg, "broker.recording-directory",
                         defaults::recording_directory);
  if (!meta_dir.empty() && detail::is_directory(meta_dir)) {
    if (!open_file(topics_file_, meta_dir + "/topics.txt"))
      return;
    if (!open_file(topics_file_, meta_dir + "/peers.txt"))
      return;
    std::ofstream id_file;
    if (!open_file(id_file, meta_dir + "/id.txt"))
      return;
    id_file << to_string(self->node()) << '\n';
    auto messages_file_name = meta_dir + "/messages.dat";
    writer_ = make_generator_file_writer(messages_file_name);
    if (writer_ == nullptr) {
      BROKER_WARNING("cannot open recording file" << messages_file_name);
    } else {
      BROKER_DEBUG("opened file for recording:" << messages_file_name);
      remaining_records_ = get_or(cfg, "broker.output-generator-file-cap",
                                  defaults::output_generator_file_cap);
    }
  }
}

void core_recorder::record_subscription(const filter_type& what) {
  BROKER_TRACE(BROKER_ARG(what));
  if (!topics_file_)
    return;
  // Simply append to topics without de-duplication.
  if (topics_file_.is_open()) {
    for (const auto& x : what) {
      if (!(topics_file_ << x.string() << '\n')) {
        BROKER_WARNING("failed to write to topics file");
        topics_file_.close();
        break;
      }
    }
    topics_file_.flush();
  }
}

void core_recorder::record_peer(const caf::node_id& peer_id) {
  if (peers_file_)
    peers_file_ << to_string(peer_id) << std::endl;
}

bool core_recorder::open_file(std::ofstream& fs, std::string file_name) {
  fs.open(file_name);
  if (fs.is_open()) {
    BROKER_DEBUG("opened file for recording:" << file_name);
    return true;
  }
  BROKER_WARNING("cannot open recording file:" << file_name);
  return false;
}

} // namespace broker::detail
