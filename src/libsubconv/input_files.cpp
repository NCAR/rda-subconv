#include <subconv.hpp>
#include <PostgreSQL.hpp>
#include <strutils.hpp>
#include <bitmap.hpp>
#include <metadata.hpp>

using namespace PostgreSQL;
using std::make_pair;
using std::make_tuple;
using std::move;
using std::pair;
using std::shared_ptr;
using std::stof;
using std::stoi;
using std::stoll;
using std::string;
using std::tie;
using std::unordered_map;
using std::unordered_set;
using std::vector;
using strutils::split;

namespace subconv {

vector<InputFile> input_files(const QueryData& query_data) {
  vector<InputFile> input_files; // return value
  LocalQuery q;
  if (!query_data.conditions.format.empty()) {
    q.set("code, format", "WGrML.formats", query_data.conditions.format);
  } else {
    q.set("code, format", "WGrML.formats");
  }
  if (q.submit(metadata_server) < 0) {
    terminate("Error: database error", "Error: " + q.error() + "\nQuery: " + q.
        show());
  }
  unordered_map<string, string> format_map;
  for (const auto& r : q) {
    format_map.emplace(r[0], r[1]);
  }
  LocalQuery input_files_query;
  if (request_values.ladiff > 0.09 || fabs(request_values.lodiff) > 0.09) {
    input_files_query.set("select distinct u.file_code, id, format_code, "
        "grid_definition_codes from (" + query_data.union_query + ") as u left "
        "join \"WGrML\"." + request_values.metadata_dsid + "_webfiles2 as w on "
        "w.code = u.file_code left join \"WGrML\"." + request_values.
        metadata_dsid + "_agrids2 as a on a.file_code = w.code");
  } else {
    input_files_query.set("select distinct u.file_code, id, format_code from "
        "(" + query_data.union_query + ") as u left join \"WGrML\"." +
        request_values.metadata_dsid + "_webfiles2 as w on w.code = u."
        "file_code");
  }
  if (input_files_query.submit(metadata_server) < 0) {
    terminate("Error: no files match the request\nYour request:\n" + args.rinfo,
        "Error: " + input_files_query.error() + "\nQuery: " + input_files_query.
        show());
  }
  if (input_files_query.num_rows() == 0) {
    terminate("Error: no files match the request\nYour request:\n" + args.rinfo,
        "Error: no files match the request\n" + args.rinfo);
  }

// fill the RDA file map
  unordered_map<string, pair<long long, string>> rdafile_map;
  q.set("wfile, data_size, tindex", "dssdb.wfile_" + metautils::args.dsid,
      "type = 'D'");
  if (args.get_timings) {
    args.db_timer.start();
  }
  if (q.submit(rdadb_server) < 0) {
    terminate("Database error", "Error: " + q.error());
  }
  if (args.get_timings) {
    args.db_timer.stop();
    timing_data.db += args.db_timer.elapsed_time();
  }
  for (const auto& r : q) {
    rdafile_map.emplace(r[0], make_pair(stoll(r[1]), r[2]));
  }

  struct BitmapEntry {
    struct Data {
      Data() : ladiffs(), lodiffs() { }

      vector<float> ladiffs, lodiffs;
    };
    BitmapEntry() : key(), data(nullptr) { }

    string key;
    shared_ptr<Data> data;
  } bitmap_entry;
  my::map<BitmapEntry> gridDefinition_bitmaps;
  unordered_set<string> input_files_set;
  for (const auto& row : input_files_query) {
    if (input_files_set.find(row[1]) == input_files_set.end() && format_map.
        find(row[2]) != format_map.end()) {
      auto format = format_map[row[2]];
      auto file_contains_spatial_selection = true;
      if (request_values.ladiff > 0.09 || fabs(request_values.lodiff) > 0.09) {
        if (!gridDefinition_bitmaps.found(row[3], bitmap_entry)) {
          bitmap_entry.key = row[3];
          bitmap_entry.data.reset(new BitmapEntry::Data);
          gridDefinition_bitmaps.insert(bitmap_entry);
          vector<size_t> gridDefinition_values;
          bitmap::uncompress_values(row[3], gridDefinition_values);
          for (const auto& gd_value : gridDefinition_values) {
            LocalQuery grid_definition_query("definition, def_params", "WGrML."
                "grid_definitions", "code = " + strutils::itos(gd_value));
            if (grid_definition_query.submit(metadata_server) < 0) {
                  terminate("Database error", "Error: " +
                      grid_definition_query.error() + "\nQuery: " +
                      grid_definition_query.show());
            }
            Row grid_definition_query_result;
            if (grid_definition_query.fetch_row(grid_definition_query_result)) {
              if (grid_definition_query_result[0] == "latLon") {
                auto sp = split(grid_definition_query_result[1], ":");
                Grid::GridDimensions grid_dim;
                grid_dim.x = stoi(sp[0]);
                grid_dim.y = stoi(sp[1]);
                Grid::GridDefinition grid_def;
                grid_def.slatitude=stof(sp[2].substr(0, sp[2].length() - 1));
                if (sp[2].back() == 'S') {
                  grid_def.slatitude =- grid_def.slatitude;
                }
                grid_def.slongitude = stof(sp[3].substr(0, sp[3].length() - 1));
                if (sp[3].back() == 'W') {
                  grid_def.slongitude =- grid_def.slongitude;
                }
                grid_def.elatitude = stof(sp[4].substr(0, sp[4].length() - 1));
                if (sp[4].back() == 'S') {
                  grid_def.elatitude =- grid_def.elatitude;
                }
                grid_def.elongitude = stof(sp[5].substr(0, sp[5].length() - 1));
                if (sp[5].back() == 'W') {
                  grid_def.elongitude =- grid_def.elongitude;
                }
                grid_def.loincrement = stof(sp[6]);
                grid_def.laincrement = stof(sp[7]);
                grid_def = gridutils::fix_grid_definition(grid_def, grid_dim);
                bitmap_entry.data->ladiffs.emplace_back(grid_def.laincrement);
                bitmap_entry.data->lodiffs.emplace_back(grid_def.loincrement);
              } else if (grid_definition_query_result[0] == "gaussLatLon") {
                auto sp = split(grid_definition_query_result[1], ":");
                Grid::GridDimensions grid_dim;
                grid_dim.x = stoi(sp[0]);
                grid_dim.y = stoi(sp[1]);
                Grid::GridDefinition grid_def;
                grid_def.slatitude = stof(sp[2].substr(0, sp[2].length() - 1));
                if (sp[2].back() == 'S') {
                  grid_def.slatitude =- grid_def.slatitude;
                }
                grid_def.slongitude = stof(sp[3].substr(0, sp[3].length() - 1));
                if (sp[3].back() == 'W') {
                  grid_def.slongitude =- grid_def.slongitude;
                }
                grid_def.elatitude = stof(sp[4].substr(0, sp[4].length() - 1));
                if (sp[4].back() == 'S') {
                  grid_def.elatitude = -grid_def.elatitude;
                }
                grid_def.elongitude = stof(sp[5].substr(0, sp[5].length() - 1));
                if (sp[5].back() == 'W') {
                  grid_def.elongitude =- grid_def.elongitude;
                }
                grid_def.loincrement = stof(sp[6]);
                grid_def.num_circles = stoi(sp[7]);
                grid_def=gridutils::fix_grid_definition(grid_def, grid_dim);
                my::map<Grid::GLatEntry> gaus_lats;
                gridutils::filled_gaussian_latitudes(args.SHARE_DIRECTORY +
                    "/GRIB", gaus_lats, grid_def.num_circles, (grid_def.
                    slatitude > grid_def.elatitude));
                Grid::GLatEntry gle;
                if (gaus_lats.found(grid_def.num_circles, gle)) {
                  auto nlat_index = -1;
                  auto slat_index = -1;
                  for (size_t n = 0; n < grid_def.num_circles*2; ++n) {
                      if (request_values.nlat <= gle.lats[n]) {
                        nlat_index = n;
                      }
                      if (request_values.slat <= gle.lats[n]) {
                        slat_index = n;
                      }
                  }
                  if (nlat_index > slat_index) {
                    bitmap_entry.data->ladiffs.emplace_back(0.);
                  } else {
                    bitmap_entry.data->ladiffs.emplace_back(request_values.
                        ladiff * 2.);
                  }
                } else {
                  bitmap_entry.data->ladiffs.emplace_back(request_values.ladiff
                      * 2.);
                }
                bitmap_entry.data->lodiffs.emplace_back(grid_def.loincrement);
              } else {
                terminate("Error: bad request\nYour request:\n" + args.rinfo,
                    "Error: grid_definition " + grid_definition_query_result[0]
                    + " not understood");
              }
            }
          }
        }
        file_contains_spatial_selection = false;
        auto it_lat = bitmap_entry.data->ladiffs.begin();
        auto it_lon = bitmap_entry.data->lodiffs.begin();
        auto end_it = bitmap_entry.data->ladiffs.end();
        for (; it_lat != end_it; ++it_lat, ++it_lon) {
          if ( (*it_lat - request_values.ladiff) < 0.0001 || (*it_lon - fabs(
              request_values.lodiff)) < 0.0001) {
            file_contains_spatial_selection = true;
          }
        }
      }
      if (file_contains_spatial_selection) {
        long long data_size;
        string tindex;
        tie(data_size, tindex) = rdafile_map[row[1]];
        if (request_values.tindex.empty() || tindex == request_values.tindex) {
          input_files.emplace_back(make_tuple(row[0], row[1], data_size, format,
              row[2]));
          input_files_set.emplace(row[1]);
        }
      }
    }
  }
  rdafile_map.clear();
  return input_files;
}

} // end namespace subconv
