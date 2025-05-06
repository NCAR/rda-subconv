#include <subconv.hpp>
#include <strutils.hpp>
#include <PostgreSQL.hpp>

using namespace PostgreSQL;
using std::cerr;
using std::cout;
using std::endl;
using std::get;
using std::move;
using std::string;
using std::unordered_set;
using std::vector;
using strutils::append;
using strutils::to_lower;

namespace subconv {

unordered_set<string> multiple_parameter_files(const vector<InputFile>&
    input_files, string inventory_conditions) {
  unordered_set<string> multiple_parameter_files_code_set; // return value
  if (to_lower(request_values.ofmt) == "netcdf") {
    for (const auto& input_file : input_files) {
      string union_query;
      for (const auto& parameter : request_values.parameters) {
        append(union_query, "select '" + parameter + "' as p, level_code, "
            "time_range_code from \"IGrML\"." + request_values.metadata_dsid +
            "_inventory_" + parameter_code(metadata_server, parameter) +
            " where file_code = " + get<0>(input_file) + " and " +
            inventory_conditions, " union ");
      }
      LocalQuery q("select distinct p, level_code, time_range_code from (" +
          union_query + ") as u");
      if (q.submit(metadata_server) < 0) {
        terminate("Error: database error", "Error: " + q.error() + "\nQuery: " +
            q.show());
      }
      if (q.num_rows() > 1) {
        multiple_parameter_files_code_set.emplace(get<0>(input_file));
      }
    }
  }
  return multiple_parameter_files_code_set;
}

} // end namespace subconv
