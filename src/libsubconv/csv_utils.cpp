#include <subconv.hpp>
#include <PostgreSQL.hpp>
#include <metadata.hpp>
#include <metahelpers.hpp>
#include <strutils.hpp>
#include <utils.hpp>

using namespace PostgreSQL;
using metatranslations::detailed_parameter;
using std::endl;
using std::runtime_error;
using std::stoll;
using std::string;
using std::stringstream;
using std::map;
using std::vector;
using strutils::occurs;
using strutils::replace_all;
using strutils::split;
using strutils::to_lower;
using xmlutils::LevelMapper;
using xmlutils::ParameterMapper;

namespace subconv {

CSVData csv_data(ParameterMapper& parameter_mapper, LevelMapper& level_mapper,
    std::unordered_map<string, string>& unique_formats_map) {
  CSVData csv_data;
  if (to_lower(request_values.ofmt) == "csv" && request_values.parameters.size()
      == 1 && occurs(request_values.level, ",") == 0) {
    auto pparts = split(request_values.parameters.front(), "!");
    csv_data.parameter = detailed_parameter(parameter_mapper,
        unique_formats_map[pparts[0]], pparts[1]);
    auto idx = csv_data.parameter.find(" <small");
    if (idx != string::npos) {
      csv_data.parameter = csv_data.parameter.substr(0, idx);
    }
    LocalQuery query;
    if (request_values.level.empty()) {
      query.set("level_type_codes", "WGrML." + request_values.metadata_dsid +
          "_agrids_cache", "parameter = '" + pparts[1] + "'");
      Row row;
      if (query.submit(metadata_server) == 0 && query.fetch_row(row)) {
        query.set("map, type, value", "WGrML.levels", "code = " + row[0].substr(
            0, row[0].find(":")));
      }
    } else {
      query.set("map, type, value", "WGrML.levels", "code = " + request_values.
          level);
    }
    if (query.submit(metadata_server) == 0 && query.num_rows() > 0) {
      Row row;
      query.fetch_row(row);
      csv_data.level = metatranslations::detailed_level(level_mapper,
          unique_formats_map[request_values.format_codes.front()], row[0], row[
          1], row[2], false);
      replace_all(csv_data.level, "<nobr>", "");
      replace_all(csv_data.level, "</nobr>", "");
    }
  }
  return csv_data;
}

size_t combine_csv_files(std::vector<string>& wget_list, const CSVData&
    csv_data) {
  if (!rdadb_server) {
    throw runtime_error("Error: combine_csv_files() was unable to connect to "
        "RDADB");
  }
  LocalQuery csv_files_query("wfile", "dssdb.wfrqst", "rindex = " + args.
      rqst_index + " and wfile like '%.csv'");
  if (csv_files_query.submit(rdadb_server) < 0) {
    throw runtime_error("Error (csv_files_query): " + csv_files_query.error());
  }
  auto csv_file = "data.req" + args.rqst_index + "_" + request_values.lat_s +
      "_" + request_values.lon_s + ".csv";
  vector<string> line_list;
  map<string, vector<size_t>> keys;
  for (const auto& csv_files_query_result : csv_files_query) {
    std::ifstream ifs;
    char line[32768];
    ifs.open(args.download_directory + "/" + csv_files_query_result[0]);
    ifs.getline(line, 32768);
    while (!ifs.eof()) {
      line_list.emplace_back(line);
      auto key = line_list.back().substr(0, 12);
      if (keys.find(key) == keys.end()) {
        keys.emplace(key, vector<size_t>{ line_list.size() });
      } else {
        keys[key].emplace_back(line_list.size());
      }
      ifs.getline(line, 32768);
    }
    ifs.close();
    ifs.clear();
    stringstream oss, ess;
    unixutils::mysystem2("/bin/rm " + args.download_directory + "/" +
        csv_files_query_result[0], oss, ess);
  }
  std::ofstream ofs(args.download_directory + "/" + csv_file);
  ofs << "\"Date\",\"Time\",\"" << csv_data.parameter << "@" << csv_data.level
      << "\"" << endl;
  for (const auto& e : keys) {
    for (const auto& idx : e.second) {
      ofs << line_list[idx-1].substr(13) << endl;
    }
  }
  ofs.close();
  rdadb_server._delete("dssdb.wfrqst", "rindex = " + args.rqst_index);
  insert_into_wfrqst(rdadb_server, args.rqst_index, csv_file, "csv", 1);
  wget_list.clear();
  wget_list.emplace_back(csv_file);
  return 1;
}

} // end namespace subconv
