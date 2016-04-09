#ifndef BROKER_STORE_SQLITE_BACKEND_HH
#define BROKER_STORE_SQLITE_BACKEND_HH

#include <functional>

#include "broker/detail/sqlite3.h"
#include "broker/store/backend.hh"

namespace broker {
namespace store {

/// A sqlite implementation of a storage backend.
class sqlite_backend : public backend {
public:
  /// Destructor.  Closes the database if open.
  ~sqlite_backend();

  /// Open a sqlite database.
  /// @param db_path the filesystem path of the database to create/open.
  /// @param pragmas pragma statements to execute upon opening the database.
  /// @return true if the call succeeded.  If false last_error_code() and
  /// backend::last_error() may be used to obtain more info.
  bool open(std::string db_path,
            std::deque<std::string> pragmas =
              {"pragma locking_mode = exclusive;"});

  /// Execute a pragma statement against the open database.
  /// @param p the pragma statement to execute.
  /// @return true if successful.  If false, last_error_code() and
  /// backend::last_error() may be used to obtain more info.
  bool pragma(std::string p);

  /// @return the last error code from a failed sqlite API call or a negative
  /// value if it was a non-sqlite failure.  In either case,
  /// backend::last_error() may be called to get a description of the error.
  int last_error_code() const;

private:
  void do_increase_sequence() override;

  std::string do_last_error() const override;

  bool do_init(snapshot sss) override;

  const sequence_num& do_sequence() const override;

  bool do_insert(data k, data v, maybe<expiration_time> t) override;

  modification_result do_increment(const data& k, int64_t by,
                                   double mod_time) override;

  modification_result do_add_to_set(const data& k, data element,
                                    double mod_time) override;

  modification_result do_remove_from_set(const data& k, const data& element,
                                         double mod_time) override;

  bool do_erase(const data& k) override;

  bool do_expire(const data& k, const expiration_time& expiration) override;

  bool do_clear() override;

  modification_result do_push_left(const data& k, vector items,
                                   double mod_time) override;

  modification_result do_push_right(const data& k, vector items,
                                    double mod_time) override;

  std::pair<modification_result, maybe<data>>
  do_pop_left(const data& k, double mod_time) override;

  std::pair<modification_result, maybe<data>>
  do_pop_right(const data& k, double mod_time) override;

  maybe<maybe<data>> do_lookup(const data& k) const override;

  maybe<std::pair<maybe<data>, maybe<expiration_time>>>
  do_lookup_expiry(const data& k) const;

  maybe<bool> do_exists(const data& k) const override;

  maybe<std::vector<data>> do_keys() const override;

  maybe<uint64_t> do_size() const override;

  maybe<snapshot> do_snap() const override;

  maybe<std::deque<expirable>> do_expiries() const override;

private:
  struct stmt_guard {
    stmt_guard() = default;
    stmt_guard(sqlite3_stmt* arg_stmt, std::function<int(sqlite3_stmt*)> fun);

    ~stmt_guard();

    sqlite3_stmt* stmt = nullptr;
    std::function<int(sqlite3_stmt*)> func;
  };

  struct statement {
    statement(const char* arg_sql);

    operator sqlite3_stmt*() const;

    bool prepare(sqlite3* db);

    stmt_guard guard(std::function<int(sqlite3_stmt*)> func) const;

    sqlite3_stmt* stmt = nullptr;
    std::string sql;
    stmt_guard g;
  };

  static bool insert(const statement& stmt, const data& k, const data& v,
                     const maybe<expiration_time>& e = {});

  static bool update(const statement& stmt, const data& k, const data& v);

  static bool update_expiry(const statement& stmt, const data& k,
                            const data& v, const expiration_time& e);

  static modification_result update_entry(const statement& update_stmt,
                                          const statement& update_expiry_stmt,
                                          const data& k, const data& v,
                                          maybe<expiration_time> e,
                                          int& last_rc);

  sequence_num sn_;
  int last_rc_ = 0;
  std::string our_last_error_;
  sqlite3* db_ = nullptr;
  statement insert_{"replace into store(k, v, expiry) values(?, ?, ?);"};
  statement erase_{"delete from store where k = ?;"};
  statement expire_{"delete from store where k = ? and expiry = ?;"};
  statement clear_{"delete from store;"};
  statement update_{"update store set v = ? where k = ?;"};
  statement update_expiry_{"update store set v = ?, expiry = ? where k = ?;"};
  statement lookup_{"select v from store where k = ?;"};
  statement lookup_expiry_{"select v, expiry from store where k = ?;"};
  statement exists_{"select 1 from store where k = ?;"};
  statement keys_{"select k from store;"};
  statement size_{"select count(*) from store;"};
  statement snap_{"select k, v, expiry from store;"};
  statement expiries_{"select k, expiry from store where expiry is not null;"};
};

} // namespace store
} // namespace broker

#endif // BROKER_STORE_SQLITE_BACKEND_HH
