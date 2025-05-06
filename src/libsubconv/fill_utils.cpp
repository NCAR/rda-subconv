#include <subconv.hpp>
#include <metadata.hpp>
#include <PostgreSQL.hpp>
#include <strutils.hpp>

using namespace PostgreSQL;
using strutils::to_lower;

namespace subconv {

void fill_ancillary_request_values() {
  if (!args.rqst_index.empty()) {
    if (args.get_timings) {
      args.db_timer.start();
    }
    LocalQuery q("select r.dsid as dsid, r.rinfo as rinfo, r.file_format as "
        "file_format, r.email as email, r.location as location, r.tarflag as "
        "tarflag, u.country as country from dsrqst as r left join ruser as u "
        "on u.email = r.email where r.rindex = " + args.rqst_index);
    if (q.submit(rdadb_server) < 0) {
      terminate("Database error", "Error: " + q.error() + "\nQuery: " + q.
          show());
    }
    if (args.get_timings) {
      args.db_timer.stop();
      timing_data.db += args.db_timer.elapsed_time();
    }
    Row row;
    if (!q.fetch_row(row)) {
      terminate("Invalid request ID", "Error: no entry in dsrqst for rindex = "
          + args.rqst_index);
    }
    metautils::args.dsid = row["dsid"].substr(2);
    args.rinfo = row["rinfo"];
    request_values.ancillary.compression = row["file_format"];
    if (!request_values.ancillary.compression.empty()) {
      request_values.ancillary.compression = "." + to_lower(request_values.
          ancillary.compression);
    }
    request_values.ancillary.user_email = row["email"];
    request_values.ancillary.user_country = row["country"];
    request_values.ancillary.location = row["location"];
    request_values.ancillary.tarflag = row["tarflag"];
  }
}

} // end namespace subconv
