#include <subconv.hpp>
#include <strutils.hpp>

using std::runtime_error;
using strutils::to_lower;

namespace subconv {

Directives read_config() {
  if (!args.is_test && request_values.ancillary.user_email.empty()) {
    throw runtime_error(
        "Error: fill from dssdb.dsrqst must precede subconv configuration");
  }
  Directives directives;
  std::ifstream ifs("/glade/u/home/dattore/subconv_pg/conf/local_subconv.conf");
  if (ifs.is_open()) {
    char line[256];
    ifs.getline(line, 256);
    while (!ifs.eof()) {
      auto lparts = strutils::split(line);
      if (lparts.front() == "dsrqstRoot") {
        directives.dsrqst_root = lparts.back();
      } else if (to_lower(lparts.front()) == to_lower(request_values.ancillary.
          user_email)) {
        if (lparts.size() > 1) {
          directives.dataset_block = lparts.back();
        } else {
          directives.dataset_block = "all";
        }
      } else if (lparts.front() == args.rqst_index) {
        directives.pbs_options = lparts.back();
      } else if (lparts.front() == "restrictToHost") {
        directives.host_restrict.emplace_back(lparts.back());
      } else if (lparts.front() == "objectStore") {
        directives.obj_store.fill(lparts[1], lparts[2], lparts[3], lparts[4],
            lparts[5]);
      } else if (lparts.front() == "dataRoot") {
        directives.data_root = lparts.back();
      } else if (lparts.front() == "PostgreSQLServer") {
        directives.db_config.host = lparts.back();
      } else if (lparts.front() == "PostgreSQLUsername") {
        directives.db_config.user = lparts.back();
      } else if (lparts.front() == "PostgreSQLPassword") {
        directives.db_config.password = lparts.back();
      } else if (lparts.front() == "PostgreSQLDatabase") {
        directives.db_config.dbname = lparts.back();
      }
      ifs.getline(line, 256);
    }
    ifs.close();
  }
  return directives;
}

} // end namespace subconv
