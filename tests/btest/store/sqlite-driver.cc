// Tool for testing the SQLite backend. The tool receives a JSON input file that
// contains a config and a series of commands to execute.

#include "broker/configuration.hh"
#include "broker/detail/sqlite_backend.hh"

#include <caf/actor_system_config.hpp>
#include <caf/json_reader.hpp>

#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

using namespace broker;
using namespace std::literals;

using string_list = std::vector<std::string>;

using broker::detail::sqlite_backend;

// -- commands -----------------------------------------------------------------

void exec_pragma(sqlite_backend* backend, string_list args) {
  if (args.size() == 1)
    args.push_back(""s);
  if (args.size() != 2)
    throw std::runtime_error("exec-pragma: expected one or two arguments");
  string_list outputs;
  backend->exec_pragma(args[0], args[1], &outputs);
  std::cout << "exec-pragma '" << args[0] << "'";
  if (!args[1].empty())
    std::cout << " '" << args[1] << "'";
  if (outputs.empty()) {
    std::cout << " [no output]\n";
    return;
  }
  std::cout << ":\n";
  for (size_t index = 0; index < outputs.size(); ++index)
    std::cout << "  [" << index << "] " << outputs[index] << '\n';
}

void get(sqlite_backend* backend, string_list args) {
  if (args.size() != 1)
    throw std::runtime_error("get: expected one argument");
  auto res = backend->get(args[0]);
  if (!res) {
    std::cout << "get '" << args[0] << "' failed: " << to_string(res.error())
              << '\n';
    return;
  }
  std::cout << "get '" << args[0] << "': " << to_string(*res) << '\n';
}

void put(sqlite_backend* backend, string_list args) {
  if (args.size() != 2)
    throw std::runtime_error("put: expected two arguments");
  auto res = backend->put(args[0], args[1], std::nullopt);
  if (!res)
    throw std::runtime_error("put failed: " + to_string(res.error()));
  std::cout << "put '" << args[0] << "' '" << args[1] << "': OK\n";
}

using command = void (*)(sqlite_backend*, string_list);

namespace {

std::pair<std::string_view, command> cmd_tbl[] = {
  {"exec-pragma", exec_pragma},
  {"put", put},
  {"get", get},
};

} // namespace

void run_command(sqlite_backend* backend, std::string_view name,
                 string_list args) {
  auto i = std::find_if(std::begin(cmd_tbl), std::end(cmd_tbl),
                        [name](const auto& kvp) { return kvp.first == name; });
  if (i == std::end(cmd_tbl))
    throw std::runtime_error("no such command: " + std::string{name});
  (i->second)(backend, std::move(args));
}

// A configuration for the backend.
struct config {
  using dict_t = std::map<std::string, std::string>;
  std::string file_path;
  dict_t options;
  std::unique_ptr<sqlite_backend> make_backend() {
    auto native_otps = backend_options{};
    native_otps.emplace("path", file_path);
    for (const auto& [key, val] : options) {
      if (val.compare(0, 4, "STR:") == 0) {
        native_otps.emplace(key, val.substr(4));
      }
      if (val.compare(0, 4, "BOOL:") == 0) {
        native_otps.emplace(key, val == "BOOL:true");
      } else {
        native_otps.emplace(key, enum_value{val});
      }
    }
    auto ptr = std::make_unique<sqlite_backend>(std::move(native_otps));
    if (ptr->init_failed())
      throw std::runtime_error("failed to initialize sqlite backend");
    return ptr;
  }
};

template <class Inspector>
bool inspect(Inspector& f, config& cfg) {
  return f.object(cfg).fields(f.field("file-path", cfg.file_path),
                              f.field("options", cfg.options));
}

// A configuration for the backend plus a list of commands to execute.
struct program {
  config cfg;
  std::vector<std::pair<std::string, std::vector<std::string>>> cmds;
};

template <class Inspector>
bool inspect(Inspector& f, program& prog) {
  return f.object(prog).fields(f.field("config", prog.cfg),
                               f.field("commands", prog.cmds));
}

int main(int argc, char** argv) try {
  broker::configuration::init_global_state(); // Initialize meta obj table.
  setvbuf(stdout, NULL, _IOLBF, 0);           // Always line-buffer stdout.
  // Parse CLI to get the path to our JSON config.
  auto args = string_list{argv + 1, argv + argc};
  auto opts = caf::settings{};
  auto opts_def = caf::config_option_set{};
  opts_def.add<std::string>("program", "path to the JSON file to execute");
  if (auto pres = opts_def.parse(opts, args); pres.first != caf::pec::success) {
    std::cerr << "*** failed to parse CLI argument: " << to_string(pres.first)
              << '\n';
    return EXIT_FAILURE;
  }
  auto path = caf::get_as<std::string>(opts, "program");
  if (!path) {
    std::cerr << "*** mandatory argument program missing\n";
    return EXIT_FAILURE;
  }
  // Open and parse the file.
  // TODO: CAF 0.19 has `load_file` for this.
  auto in_file = std::ifstream{*path};
  if (!in_file) {
    std::cerr << "*** failed to open file: " << *path << "\n";
    return EXIT_FAILURE;
  }
  auto buffer = std::stringstream{};
  buffer << in_file.rdbuf();
  auto json = buffer.str();
  auto reader = caf::json_reader{};
  if (!reader.load(json)) {
    std::cerr << "*** failed to load JSON file: "
              << to_string(reader.get_error()) << '\n';
    return EXIT_FAILURE;
  }
  auto prog = program{};
  if (!reader.apply(prog)) {
    std::cerr << "*** failed to load program from JSON file: "
              << to_string(reader.get_error()) << '\n';
    return EXIT_FAILURE;
  }
  // Run the program.
  auto backend = prog.cfg.make_backend();
  for (auto& [cmd_name, cmd_args] : prog.cmds) {
    run_command(backend.get(), cmd_name, std::move(cmd_args));
  }
  return EXIT_SUCCESS;
} catch (std::exception& ex) {
  // Note: report EXIT_SUCCESS here: some tests trigger exceptions on purpose.
  std::cout << "*** exception: " << ex.what() << '\n';
  return EXIT_SUCCESS;
}
