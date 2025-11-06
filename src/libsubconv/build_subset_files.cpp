#include <unordered_map>
#include <unordered_set>
#include <sstream>
#include <thread>
#include <unistd.h>
#include <sys/stat.h>
#include <signal.h>
#include <subconv.hpp>
#include <PostgreSQL.hpp>
#include <strutils.hpp>
#include <utils.hpp>
#include <metadata.hpp>
#include <bits.hpp>
#include <timer.hpp>

using namespace PostgreSQL;
using grid_to_netcdf::GridData;
using NCTime = NetCDF::Time;
using NCType = NetCDF::NCType;
using VariableData = NetCDF::VariableData;
using std::endl;
using std::ofstream;
using std::ref;
using std::regex;
using std::regex_search;
using std::runtime_error;
using std::stoi;
using std::stoll;
using std::string;
using std::stringstream;
using std::thread;
using std::tie;
using std::unique_ptr;
using std::unordered_map;
using std::unordered_set;
using strutils::append;
using strutils::itos;
using strutils::replace_all;
using strutils::split;
using strutils::substitute;
using strutils::to_lower;
using unixutils::mysystem2;

namespace subconv {

const string CSV_EXT = ".csv";
const string NC_EXT = ".nc";
const regex NC_END = regex("\\" + NC_EXT + "$");
const string TMP_EXT = ".TMP";

class SpatialBitmap {
public:
  SpatialBitmap() : buffer(nullptr), len(0) {}
  int length() const { return len; }
  unsigned char& operator[](int index) {
    if (index >= len) {
      throw runtime_error("spatial bitmap index (" + itos(index) + ") out of "
          "bounds");
    }
    return buffer[index];
  }
  void resize(int length) {
    if (length <= len) {
      return;
    }
    len = length;
    buffer.reset(new unsigned char[len]);
  }

private:
  unique_ptr<unsigned char[]> buffer;
  int len;
};

struct OutputStream {
  OutputStream() : ofs(), onc() {}

  ofstream ofs;
  OutputNetCDFStream onc;
};

bool check_for(string filename, ThreadData& thread_data) {
  struct stat buf;
  if (stat(filename.c_str(), &buf) == 0) {
    thread_data.write_bytes = buf.st_size;
    thread_data.fcount = 1;
    return true;
  }
  return false;
}

bool file_exists(ThreadData& thread_data, unique_ptr<unordered_set<string>>&
    nts_table, OutputStream& outs, bool is_multi) {
  bool file_exists = false;
  if (request_values.ststep) {
    nts_table.reset(new unordered_set<string>);
  }
  auto output_file = args.download_directory + thread_data.filename;
  if ((to_lower(request_values.ofmt) == "netcdf" && !is_multi) || (to_lower(
      thread_data.data_format) == "netcdf" && request_values.ofmt.empty())) {
    if (!regex_search(output_file, NC_END)) {
      thread_data.filename += NC_EXT;
      output_file += NC_EXT;
    }
    if (!file_exists) {
      file_exists = check_for(output_file, thread_data);
    }
    if (!file_exists) {
      file_exists = check_for(output_file +
          request_values.ancillary.compression, thread_data);
    }
    if (!file_exists) {
      if (to_lower(request_values.ofmt) == "netcdf" && !outs.onc.open(
          output_file + TMP_EXT)) {
        throw runtime_error("Error opening " + output_file + " for output");
      }
    }
  } else {
    if (to_lower(request_values.ofmt) == "netcdf") {
      if (!file_exists) {
        file_exists = check_for(output_file + NC_EXT,thread_data);
      }
      if (!file_exists) {
        file_exists = check_for(output_file + NC_EXT +
            request_values.ancillary.compression, thread_data);
      }
      if (file_exists) {
        auto s = thread_data.filename + NC_EXT;
        thread_data.insert_filenames.emplace_back(s.substr(1));
        thread_data.wget_filenames.emplace_back(s.substr(1) +
            request_values.ancillary.compression);
      }
    } else {
      if (to_lower(request_values.ofmt) == "csv") {
        thread_data.filename += CSV_EXT;
        output_file += CSV_EXT;
      }
      if (!file_exists) {
        file_exists = check_for(output_file, thread_data);
      }
      if (!file_exists) {
        file_exists = check_for(output_file +
            request_values.ancillary.compression, thread_data);
      }
    }
    if (!file_exists) {
      if (!request_values.ststep) {
        outs.ofs.open((output_file + TMP_EXT).c_str());
        if (!outs.ofs.is_open()) {
          throw runtime_error("Error opening " + output_file + " for output");
        }
      }
      if (is_multi) {
        thread_data.f_attach = thread_data.filename + "<!>" + thread_data
            .data_format;
      }
    }
  }
  if (!request_values.ststep && !is_multi && (!request_values.topt_mo[0] ||
      file_exists)) {
    thread_data.insert_filenames.emplace_back(thread_data.filename.substr(1));
    thread_data.wget_filenames.emplace_back(thread_data.filename.substr(1) +
        request_values.ancillary.compression);
  }
  return file_exists;
}

void build_queries(Server& server, const ThreadData& thread_data, LocalQuery&
    byte_query, LocalQuery& byte_query_no_dates, LocalQuery&
    query_count_files) {
  string union_query = "";
  string union_query_no_dates = "";
  for (const auto& parameter : request_values.parameters) {
    if (request_values.ststep || request_values.topt_mo[0] ||
        to_lower(request_values.ofmt) == "csv") {
      append(union_query, "select byte_offset, byte_length, valid_date from "
          "\"IGrML\"." + metautils::args.dsid + "_inventory_" + parameter_code(
          server, parameter) + " where file_code = " + thread_data.file_code +
          " and " + thread_data.uConditions, " union ");
      append(union_query_no_dates, "select byte_offset, byte_length, "
          "valid_date from \"IGrML\"." + metautils::args.dsid + "_inventory_" +
          parameter_code(server, parameter) + " where file_code = " +
          thread_data.file_code, " union ");
      if (!thread_data.uConditions_no_dates.empty()) {
        union_query_no_dates += " and " + thread_data.uConditions_no_dates;
      }
      if (parameter == request_values.parameters.back()) {
        byte_query.set("select distinct byte_offset, byte_length, valid_date "
            "from (" + union_query + ") as u order by valid_date, byte_offset");
        byte_query_no_dates.set("select distinct byte_offset, byte_length, "
            "valid_date from (" + union_query_no_dates + ") as u order by "
            "valid_date, byte_offset");
      }
    } else if (to_lower(thread_data.data_format) == "netcdf" &&
        request_values.ofmt.empty()) {
      append(union_query, "select byte_offset, byte_length, valid_date, "
          "process from \"IGrML\"." + metautils::args.dsid + "_inventory_" +
          parameter_code(server, parameter) + " where file_code = " +
          thread_data.file_code + " and " + thread_data.uConditions, " union ");
      append(union_query_no_dates, "select byte_offset, byte_length, "
          "valid_date, process from \"IGrML\"." + metautils::args.dsid +
          "_inventory_" + parameter_code(server, parameter) + " where file_code "
          "= " + thread_data.file_code, " union ");
      if (!thread_data.uConditions_no_dates.empty()) {
        union_query_no_dates += " and " + thread_data.uConditions_no_dates;
      }
      if (parameter == request_values.parameters.back()) {
        byte_query.set("select distinct byte_offset, byte_length, valid_date, "
            "process from (" + union_query + ") as u order by valid_date");
        byte_query_no_dates.set("select distinct byte_offset, byte_length, "
            "valid_date, process from (" + union_query_no_dates + ") as u "
            "order by valid_date");
        if (request_values.nlat > 99.) {
          query_count_files.set("select count(byte_offset) from (" +
              union_query_no_dates + ") as u");
        }
      }
    } else {
      append(union_query, "select byte_offset, byte_length from \"IGrML\"." +
          metautils::args.dsid + "_inventory_" + parameter_code(server,
          parameter) + " where file_code = " + thread_data.file_code + " and " +
          thread_data.uConditions, " union ");
      append(union_query_no_dates, "select byte_offset, byte_length from "
          "\"IGrML\"." + metautils::args.dsid + "_inventory_" + parameter_code(
          server, parameter) + " where file_code = " + thread_data.file_code,
          " union ");
      if (!thread_data.uConditions_no_dates.empty()) {
        union_query_no_dates += " and " + thread_data.uConditions_no_dates;
      }
      if (parameter == request_values.parameters.back()) {
        byte_query.set("select distinct byte_offset, byte_length from (" +
            union_query + ") as u order by byte_offset");
        byte_query_no_dates.set("select distinct byte_offset, byte_length from "
            "(" + union_query_no_dates + ") as u order by byte_offset");
      }
    }
  }
}

void write_netcdf_subset_header(string request_index, string input_file,
    OutputNetCDFStream& onc, NCTime& nc_time, SpatialBitmap& spatial_bitmap,
    int& num_values_in_subset) {
  InputNetCDFStream inc;
  if (!inc.open(input_file)) {
    throw runtime_error("Error opening " + input_file + " for input");
  }
  auto attrs = inc.global_attributes();
  for (size_t n = 0; n < attrs.size(); ++n) {
    onc.add_global_attribute(attrs[n].name, attrs[n].nc_type,
        attrs[n].num_values, attrs[n].values);
  }
  onc.add_global_attribute("Creation date and time",
      dateutils::current_date_time().to_string());
  onc.add_global_attribute("Creator",
      "NCAR - CISL RDA (dattore); data request #" + request_index);
  auto dims = inc.dimensions();
  num_values_in_subset = -1;
  for (size_t n = 0; n < dims.size(); ++n) {
    onc.add_dimension(dims[n].name, dims[n].length);
    if (to_lower(dims[n].name) == "lat" || to_lower(dims[n].name) ==
        "latitude" || to_lower(dims[n].name) == "lon" ||
        to_lower(dims[n].name) == "longitude") {
      if (num_values_in_subset < 0) {
        num_values_in_subset = 1;
      }
      num_values_in_subset *= dims[n].length;
    }
  }
  auto vars = inc.variables();
  VariableData sub_lat_data, sub_lon_data;
  unordered_map<size_t, size_t> new_dims_map;
  unordered_map<string, string> new_coords_map;
  if (request_values.nlat < 99.) {
    spatial_bitmap.resize(num_values_in_subset);
    num_values_in_subset = 1;
    auto dim_cnt = dims.size();
    VariableData lat_data, lon_data;
    auto found_lon_gap = false;
    for (size_t n = 0; n < vars.size(); ++n) {
      if (vars[n].is_coord) {
        if (to_lower(vars[n].name) == "lat" || to_lower(vars[n].name) ==
            "latitude") {
          inc.variable_data(vars[n].name, lat_data);
          auto dim_len = 0;
          sub_lat_data.resize(lat_data.size(), vars[n].nc_type);
          for (size_t l = 0; l < lat_data.size(); ++l) {
            auto lat_val = lat_data[l];
            if (lat_val >= request_values.slat && lat_val <=
                request_values.nlat) {
              sub_lat_data.set(dim_len, lat_val);
              ++dim_len;
            }
          }
          if (dim_len == 0) {
            throw runtime_error(
                "Error: no latitudes match the request - can't continue");
          }
          auto new_var = vars[n].name + "0";
          onc.add_dimension(new_var, dim_len);
          new_dims_map.emplace(vars[n].dimids[0], dim_cnt++);
          new_coords_map.emplace(vars[n].name, new_var);
          num_values_in_subset *= dim_len;
        } else if (to_lower(vars[n].name) == "lon" || to_lower(vars[n].name) ==
            "longitude") {
          inc.variable_data(vars[n].name, lon_data);
          auto dim_len = 0;
          sub_lon_data.resize(lon_data.size(), vars[n].nc_type);
          size_t last_lon = 0;
          for (size_t l = 0; l < lon_data.size(); ++l) {
            auto lon_val = lon_data[l];
            if ((lon_val >= request_values.wlon && lon_val <=
                request_values.elon) || (lon_val >= (request_values.wlon +
                360.) && lon_val <= (request_values.elon + 360.))) {
              sub_lon_data.set(dim_len, lon_val);
              ++dim_len;
              if (last_lon == 0) {
                last_lon = l;
              } else if (l > 0 && l != (last_lon + 1)) {
                found_lon_gap = true;
              }
              last_lon = l;
            }
          }
          if (found_lon_gap) {
            throw runtime_error(
                "Error: found a gap in the longitudes - can't continue");
          }
          if (dim_len == 0) {
            throw runtime_error(
                "Error: no longitudes match the request - can't continue");
          }
          auto new_var = vars[n].name + "0";
          onc.add_dimension(new_var, dim_len);
          new_dims_map.emplace(vars[n].dimids[0], dim_cnt++);
          new_coords_map.emplace(vars[n].name, new_var);
          num_values_in_subset *= dim_len;
        }
      }
    }
    auto l = 0;
    for (size_t n = 0; n < lat_data.size(); ++n) {
      for (size_t m = 0; m < lon_data.size(); m++) {
        if (lat_data[n] >= request_values.slat && lat_data[n] <=
            request_values.nlat && ((lon_data[m] >= request_values.wlon &&
            lon_data[m] <= (request_values.elon)) || (lon_data[m] >=
            request_values.wlon + 360. && lon_data[m] <= (request_values.elon +
            360.)))) {
          spatial_bitmap[l++] = 1;
        } else {
          spatial_bitmap[l++] = 0;
        }
      }
    }
    if (l == 0) {
      throw runtime_error(
          "Error: no spatial bitmap locations were set - can't continue");
    }
  }
  unordered_set<string> unique_variable_set;
  for (auto parameter : request_values.parameters) {
    auto idx = parameter.find(":");
    if (idx != string::npos) {
      parameter = parameter.substr(idx + 1);
    }
    if (unique_variable_set.find(parameter) == unique_variable_set.end()) {
      unique_variable_set.emplace(parameter);
    }
  }
  for (size_t n = 0; n < vars.size(); ++n) {
    if (vars[n].is_coord || unique_variable_set.find(vars[n].name) !=
        unique_variable_set.end()) {
      unique_ptr<size_t[]> dimension_ids(new size_t[vars[n].dimids.size()]);
      for (size_t m = 0; m < vars[n].dimids.size(); m++) {
        if (new_dims_map.find(vars[n].dimids[m]) != new_dims_map.end()) {
          dimension_ids[m] = new_dims_map[vars[n].dimids[n]];
        } else {
          dimension_ids[m] = vars[n].dimids[m];
        }
      }
      string var_name;
      if (new_coords_map.find(vars[n].name) == new_coords_map.end()) {
        var_name = vars[n].name;
      } else {
        var_name = new_coords_map[vars[n].name];
      }
      onc.add_variable(var_name, vars[n].nc_type, vars[n].dimids.size(),
          dimension_ids.get());
      for (size_t m = 0; m < vars[n].attrs.size(); ++m) {
        onc.add_variable_attribute(var_name, vars[n].attrs[m].name,
            vars[n].attrs[m].nc_type, vars[n].attrs[m].num_values,
            vars[n].attrs[m].values);
        if (to_lower(vars[n].name) == "time" && vars[n].attrs[m].name ==
            "units") {
          auto uparts = split(*(reinterpret_cast<string *>(
              vars[n].attrs[m].values)));
          if (uparts.size() > 2 && uparts[1] == "since") {
            nc_time.units = uparts[0];
            auto s = uparts[2];
            for (size_t n = 3; n < uparts.size(); ++n) {
              s += uparts[n];
            }
            replace_all(s, "-", "");
            replace_all(s, ":", "");
            nc_time.base.set(stoll(s));
            nc_time.nc_type = vars[n].nc_type;
          }
          if (nc_time.units.empty()) {
            throw runtime_error("Error: unable to locate time units");
          }
        }
      }
    }
  }
  onc.write_header();
  for (size_t n = 0; n < vars.size(); ++n) {
    if (vars[n].is_coord && !vars[n].is_rec && new_coords_map.find(
        vars[n].name) == new_coords_map.end()) {
      VariableData var_data;
      inc.variable_data(vars[n].name, var_data);
      onc.write_non_record_data(vars[n].name, var_data.get());
    }
  }
  if (sub_lat_data.size() > 0) {
    onc.write_non_record_data("lat0", sub_lat_data.get());
  }
  if (sub_lon_data.size() > 0) {
    onc.write_non_record_data("lon0", sub_lon_data.get());
  }
  inc.close();
}

bool linked_to_full_file(const ThreadData& thread_data, OutputStream& outs,
    NCTime& nc_time, SpatialBitmap& spatial_bitmap, int& num_values_in_subset,
    Server& srv, LocalQuery& query_count_files, size_t byte_query_num_rows) {
  bool linked_to_full_file = false;
  num_values_in_subset = 0;
  if (!args.is_test) {
    if (query_count_files) {
      if (query_count_files.submit(srv) < 0) {
        throw runtime_error("linked_to_full_file(): " +
            query_count_files.error() + " for query '" +
            query_count_files.show() + "'");
      }
      Row row;
      if (!query_count_files.fetch_row(row) || row[0].empty()) {
        throw runtime_error("linked_to_full_file(): error getting file count");
      }
      if (stoi(row[0]) == static_cast<int>(byte_query_num_rows)) {
std::cerr << "**linked '" << thread_data.webhome+"/"+thread_data.file_id << "' to '" << args.download_directory+thread_data.filename << "'" << endl;
        stringstream oss, ess;
        mysystem2("/bin/ln -s " + thread_data.webhome + "/" +
            thread_data.file_id + " " + args.download_directory +
            thread_data.filename,oss,ess);
        if (!ess.str().empty()) {
          throw runtime_error("linked_to_full_file(): link error: '" +
              ess.str() + "'");
        }
        linked_to_full_file = true;
      }
    }
    if (!linked_to_full_file) {
      if (to_lower(thread_data.data_format) == "netcdf" && request_values.ofmt
          .empty()) {
        if (!outs.onc.open(args.download_directory + thread_data.filename +
            TMP_EXT)) {
          throw runtime_error("linked_to_full_file(): error opening " +
              args.download_directory + thread_data.filename + " for output");
        }
        write_netcdf_subset_header(args.rqst_index, thread_data.webhome + "/" +
            thread_data.file_id, outs.onc, nc_time, spatial_bitmap,
            num_values_in_subset);
      }
    }
  }
  return linked_to_full_file;
}

void build_netcdf_subset(InputDataSource& input_data, long long offset_to_chunk,
    int chunk_len, void *msg, OutputStream& outs, GridData& grid_data,
    ThreadData& thread_data, bool is_multi) {
  if (is_multi) {
    input_data.read(offset_to_chunk, chunk_len);
    Timer write_timer;
    if (args.get_timings) {
      write_timer.start();
    }
    outs.ofs.write(reinterpret_cast<char *>(input_data.get()), chunk_len);
    if (args.get_timings) {
      write_timer.stop();
      thread_data.timing_data.write += write_timer.elapsed_time();
    }
  } else {
    if (request_values.topt_mo[0]) {
      thread_data.insert_filenames.emplace_back(thread_data.filename.substr(1));
      thread_data.wget_filenames.emplace_back(thread_data.filename.substr(1) +
          request_values.ancillary.compression);
    }
    input_data.read(offset_to_chunk, chunk_len);
    if (thread_data.data_format == "WMO_GRIB1" || thread_data.data_format ==
        "WMO_GRIB2") {
      Timer grib2u_timer;
      if (args.get_timings) {
        grib2u_timer.start();
      }
      size_t num_grids = 0;
      if (thread_data.data_format == "WMO_GRIB1") {
        reinterpret_cast<GRIBMessage *>(msg)->fill(input_data.get(), false);
        num_grids = 1;
      } else if (thread_data.data_format == "WMO_GRIB2") {
        reinterpret_cast<GRIB2Message *>(msg)->fill(input_data.get(), false);
        num_grids = reinterpret_cast<GRIB2Message *>(msg)->number_of_grids();
      }
      if (args.get_timings) {
        grib2u_timer.stop();
        thread_data.timing_data.grib2u += grib2u_timer.elapsed_time();
      }
      for (size_t n = 0; n < num_grids; ++n) {
        Grid *grid = nullptr;
        if (thread_data.data_format == "WMO_GRIB1") {
          grid = reinterpret_cast<GRIBMessage *>(msg)->grid(n);
        } else if (thread_data.data_format == "WMO_GRIB2") {
          grid = reinterpret_cast<GRIB2Message *>(msg)->grid(n);
        }
        if (is_selected_parameter(thread_data, grid)) {
          Timer nc_timer;
          if (args.get_timings) {
            nc_timer.start();
          }
          Grid::Format grid_format{ Grid::Format::not_set };
          if (thread_data.data_format == "WMO_GRIB1") {
            grid_format = Grid::Format::grib;
          } else if (thread_data.data_format == "WMO_GRIB2") {
            grid_format = Grid::Format::grib2;
          }
          convert_grid_to_netcdf(grid, grid_format, &outs.onc, grid_data,
              args.SHARE_DIRECTORY + "/metadata/LevelTables");
          if (args.get_timings) {
            nc_timer.stop();
            thread_data.timing_data.nc += nc_timer.elapsed_time();
          }
        }
      }
    } else {
      throw runtime_error("build_netcdf_subset(): unable to convert from '" +
          thread_data.data_format + "'");
    }
  }
}

void build_csv_subset(InputDataSource& input_data, long long offset_to_chunk,
    int chunk_len, void *msg, my::map<Grid::GLatEntry> **glats, ofstream& ofs,
    ThreadData& thread_data) {
  input_data.read(offset_to_chunk, chunk_len);
  if (thread_data.data_format == "WMO_GRIB1" || thread_data.data_format ==
      "WMO_GRIB2") {
    size_t num_grids = 0;
    if (thread_data.data_format == "WMO_GRIB1") {
      reinterpret_cast<GRIBMessage *>(msg)->fill(input_data.get(), false);
      num_grids = 1;
    } else if (thread_data.data_format == "WMO_GRIB2") {
      reinterpret_cast<GRIB2Message *>(msg)->fill(input_data.get(), false);
      num_grids = reinterpret_cast<GRIB2Message *>(msg)->number_of_grids();
    }
    for (size_t n = 0; n < num_grids; ++n) {
      Grid *grid = nullptr;
      if (thread_data.data_format == "WMO_GRIB1") {
        grid = reinterpret_cast<GRIBMessage *>(msg)->grid(n);
      } else if (thread_data.data_format == "WMO_GRIB2") {
        grid = reinterpret_cast<GRIB2Message *>(msg)->grid(n);
      }
      if (is_selected_parameter(thread_data, grid)) {
        if (grid->definition().type == Grid::Type::gaussianLatitudeLongitude) {
          if (*glats == nullptr) {
            *glats = new my::map<Grid::GLatEntry>;
            gridutils::filled_gaussian_latitudes(args.SHARE_DIRECTORY + "/GRIB",
                **glats, grid->definition().num_circles, true);
          }
          ofs << grid->valid_date_time().to_string("%Y%m%d%H%MM,") <<
              grid->valid_date_time().to_string("%mm/%dd/%Y,%HH:%MM") << "," <<
              grid->gridpoint(grid->longitude_index_of(request_values.wlon),
              (reinterpret_cast<GRIBGrid *>(grid))->latitude_index_of(
              request_values.nlat, *glats)) << endl;
        } else {
          ofs << grid->valid_date_time().to_string("%Y%m%d%H%MM,") <<
              grid->valid_date_time().to_string("%mm/%dd/%Y,%HH:%MM") << "," <<
              grid->gridpoint_at(request_values.nlat,request_values.wlon) <<
              endl;
        }
      }
    }
  }
}

void build_subset(ThreadData& thread_data, GridData& grid_data, const
    NCTime& nc_time, SpatialBitmap& spatial_bitmap, int num_values_in_subset,
    LocalQuery& byte_query, unique_ptr<unordered_set<string>>& nts_table,
    OutputStream& outs, bool is_multi) {
  if (args.is_test) {

    // if this is a test run, report the number of grids that would need to be
    //   accessed, and return
    thread_data.timing_data.num_reads = byte_query.num_rows();
    return;
  }
  const string THIS_FUNC = __func__;
  my::map<Grid::GLatEntry> *glats = nullptr;
  string stsfil, last_valid_date;
  void *msg = nullptr;
  if (thread_data.data_format == "WMO_GRIB1") {
    msg = new GRIBMessage;
  } else if (thread_data.data_format == "WMO_GRIB2") {
    msg = new GRIB2Message;
  }
  thread_data.fcount = 0;
  thread_data.write_bytes = 0;

  // initialize the input data source
  InputDataSource input_data;
  if (locflag == 'O') {
    input_data.initialize(thread_data.s3_session, "rda-data", metautils::args.
        dsid + "/" + thread_data.file_id);
  } else {
    input_data.initialize(thread_data.webhome + "/" + thread_data.file_id);
  }
  for (const auto& row : byte_query) {
    if (!request_values.topt_mo[0] || request_values.topt_mo[stoi(row[2].substr(
        4, 2))]) {
      if (!request_values.ofmt.empty()) {

        // convert to a different data format
        if (to_lower(request_values.ofmt) == "netcdf") {
          build_netcdf_subset(input_data, stoll(row[0]), stoi(row[1]), msg,
              outs, grid_data, thread_data, is_multi);
        } else if (to_lower(request_values.ofmt) == "csv") {
          build_csv_subset(input_data, stoll(row[0]), stoi(row[1]), msg, &glats,
              outs.ofs, thread_data);
        } else {
          throw runtime_error(THIS_FUNC + "(): unable to convert to '" +
              request_values.ofmt + "'");
        }
      } else {

        // no format conversion; subset is same as native data format
        if (request_values.ststep) {
          if (row[2] != last_valid_date && outs.ofs.is_open()) {
            outs.ofs.close();
            system(("mv " + args.download_directory + "/" + stsfil + TMP_EXT +
                " " + args.download_directory + "/" + stsfil).c_str());
            ++thread_data.fcount;
          }
          stsfil = row[2] + "." + thread_data.filename.substr(1);
          if (!outs.ofs.is_open()) {
            if (nts_table->find(stsfil) == nts_table->end()) {
              nts_table->emplace(stsfil);
              thread_data.insert_filenames.emplace_back(stsfil);
              thread_data.wget_filenames.emplace_back(stsfil +
                  request_values.ancillary.compression);
            }
            struct stat buf;
            if (stat((args.download_directory + "/" + stsfil).c_str(), &buf) !=
                0) {
              outs.ofs.open((args.download_directory + "/" + stsfil + TMP_EXT)
                  .c_str());
              if (!outs.ofs.is_open()) {
                throw runtime_error("Error opening " + args.download_directory +
                    "/" + stsfil + " for output");
              } else {
                if (nts_table->find(stsfil) == nts_table->end()) {
                  nts_table->emplace(stsfil);
                  ++thread_data.fcount;
                  thread_data.insert_filenames.emplace_back(stsfil);
                  thread_data.wget_filenames.emplace_back(stsfil +
                      request_values.ancillary.compression);
                }
              }
            }
          }
          last_valid_date = row[2];
        } else if (request_values.topt_mo[0] && thread_data.insert_filenames
            .size() == 0) {
          thread_data.insert_filenames.emplace_back(
              thread_data.filename.substr(1));
          thread_data.wget_filenames.emplace_back(thread_data.filename.substr(
              1) + request_values.ancillary.compression);
        }
        if (outs.ofs.is_open()) {
          auto num_bytes = stoi(row[1]);
          input_data.read(stoll(row[0]), stoi(row[1]));
          auto is_spatial_subset = false;
          if (request_values.nlat < 9999. && request_values.elon < 9999. &&
              request_values.slat > -9999. && request_values.wlon > -9999.) {
            if (thread_data.data_format == "WMO_GRIB1") {
              reinterpret_cast<GRIBMessage *>(msg)->fill(input_data.get(),
                  false);
              auto grid = reinterpret_cast<GRIBMessage *>(msg)->grid(0);
              if (is_selected_parameter(thread_data, grid)) {
                GRIBMessage smsg;
                smsg.initialize(1, nullptr, 0, true, true);
                GRIBGrid sgrid;
                grid->set_path_to_gaussian_latitude_data(args.SHARE_DIRECTORY +
                    "/GRIB");
                sgrid = create_subset_grid(*(reinterpret_cast<GRIBGrid *>(
                    grid)), request_values.slat, request_values.nlat,
                    request_values.wlon, request_values.elon);
                if (sgrid.is_filled()) {
                  smsg.append_grid(&sgrid);
                  if (thread_data.obuffer == nullptr) {
                    thread_data.obuffer.reset(new unsigned char[
                        OBUFFER_LENGTH]);
                  }
                  num_bytes = smsg.copy_to_buffer(thread_data.obuffer.get(),
                      OBUFFER_LENGTH);
                } else {
                  num_bytes = 0;
                }
              }
            } else if (thread_data.data_format == "WMO_GRIB2") {
              Timer grib2u_timer;
              if (args.get_timings) {
                grib2u_timer.start();
              }
              reinterpret_cast<GRIB2Message *>(msg)->fill(input_data.get(),
                  false);
              Timer grib2c_timer;
              if (args.get_timings) {
                grib2u_timer.stop();
                thread_data.timing_data.grib2u += grib2u_timer.elapsed_time();
                grib2c_timer.start();
              }
              GRIB2Message smsg2;
              smsg2.initialize(2, nullptr, 0, true, true);
              for (size_t n = 0; n < reinterpret_cast<GRIB2Message *>(msg)->
                  number_of_grids(); ++n) {
                auto grid = reinterpret_cast<GRIB2Message *>(msg)->grid(n);
                grid->set_path_to_gaussian_latitude_data(args.SHARE_DIRECTORY +
                    "/GRIB");
                if (is_selected_parameter(thread_data, grid)) {
                  GRIB2Grid sgrid2;
                  sgrid2 = (reinterpret_cast<GRIB2Grid *>(grid))->
                      create_subset(request_values.slat, request_values.nlat, 1,
                      request_values.wlon, request_values.elon, 1);
                  switch (sgrid2.data_representation()) {
                    case 2:
                    case 3: {
                      sgrid2.set_data_representation(0);
                      break;
                    }
                  }
                  smsg2.append_grid(&sgrid2);
                }
              }
              if (thread_data.obuffer == nullptr) {
                thread_data.obuffer.reset(new unsigned char[OBUFFER_LENGTH]);
              }
              num_bytes = smsg2.copy_to_buffer(thread_data.obuffer.get(),
                  OBUFFER_LENGTH);
              if (args.get_timings) {
                grib2c_timer.stop();
                thread_data.timing_data.grib2c += grib2c_timer.elapsed_time();
              }
            } else {
              throw runtime_error(THIS_FUNC + "(): unable to create subset for "
                  "format '" + thread_data.data_format + "'");
            }
            is_spatial_subset = true;
          }
          Timer write_timer;
          if (args.get_timings) {
            write_timer.start();
          }
          if (is_spatial_subset) {
            if (num_bytes > 0) {
              outs.ofs.write(reinterpret_cast<char *>(thread_data.obuffer
                  .get()), num_bytes);
            }
          } else {
            outs.ofs.write(reinterpret_cast<char *>(input_data.get()),
                num_bytes);
          }
          thread_data.write_bytes += num_bytes;
          if (args.get_timings) {
            write_timer.stop();
            thread_data.timing_data.write += write_timer.elapsed_time();
          }
        } else if (outs.onc.is_open()) {
          if (request_values.parameters.size() > 1) {
            throw runtime_error(THIS_FUNC + "(): found more than one parameter "
                "- can't continue");
          }
          auto tval = DateTime(stoll(row[2]) * 100).seconds_since(nc_time.base);
          if (nc_time.units == "hours") {
            tval /= 3600.;
          } else if (nc_time.units == "days") {
            tval /= 86400.;
          } else {
            throw runtime_error(THIS_FUNC + "(): can't handle nc time units of "
                "'" + nc_time.units + "'");
          }
          VariableData time_data;
          if (time_data.size() == 0) {
            time_data.resize(1, nc_time.nc_type);
          }
          time_data.set(0, tval);
          outs.onc.add_record_data(time_data);
          input_data.read(stoll(row[0]), stoi(row[1]));
          Timer nc_timer;
          if (args.get_timings) {
            nc_timer.start();
          }
          if (!row[3].empty()) {
            VariableData var_data;
            if (var_data.size() == 0) {
              var_data.resize(num_values_in_subset,
                  static_cast<NCType>(stoi(row[3])));
            }
            auto m = 0;
            for (int n = 0; n < spatial_bitmap.length(); ++n) {
              if (spatial_bitmap[n] == 1) {
                switch (static_cast<NCType>(stoi(row[3]))) {
                  case NCType::FLOAT: {
                    union {
                      int i;
                      float f;
                    } b4_data;
                    bits::get(&(input_data.get())[n * 4], b4_data.i, 0, 32);
                    var_data.set(m++, b4_data.f);
                    break;
                  }
                  default: {
                    throw runtime_error(THIS_FUNC + "(): can't handle nc "
                        "variable type " + row[3]);
                  }
                }
              }
            }
            outs.onc.add_record_data(var_data);
            if (args.get_timings) {
              nc_timer.stop();
              thread_data.timing_data.nc += nc_timer.elapsed_time();
            }
          } else {
            throw runtime_error(THIS_FUNC + "(): incomplete inventory "
                "information - can't continue");
          }
        }
      }
    }
  }
  if (msg != nullptr) {
    if (thread_data.data_format == "WMO_GRIB1") {
      delete reinterpret_cast<GRIBMessage *>(msg);
    } else if (thread_data.data_format == "WMO_GRIB2") {
      delete reinterpret_cast<GRIB2Message *>(msg);
    }
  }
  auto output_file = args.download_directory + thread_data.filename;
  auto temp_file = output_file + TMP_EXT;
  if ((to_lower(request_values.ofmt) == "netcdf" && !is_multi) || (to_lower(
      thread_data.data_format) == "netcdf" && request_values.ofmt.empty())) {
    if (!outs.onc.close()) {
      auto e = "Error: " + myerror + " for file " + output_file;
      myerror = "";
      throw runtime_error(e);
    }
    struct stat buf;
    stat(temp_file.c_str(), &buf);
    if (buf.st_size == 8) {
      system(("rm -f " + temp_file).c_str());
      if (thread_data.insert_filenames.size() == 1) {
        thread_data.insert_filenames.clear();
      }
      thread_data.filename = "";
      thread_data.f_attach = "";
    } else {
      system(("mv " + temp_file + " " + output_file).c_str());
      thread_data.write_bytes += buf.st_size;
      ++thread_data.fcount;
    }
  } else if (outs.ofs.is_open()) {
    auto offset = outs.ofs.tellp();
    outs.ofs.close();
    if (offset == 0) {
      system(("rm -f " + temp_file).c_str());
      if (thread_data.insert_filenames.size() == 1) {
        thread_data.insert_filenames.clear();
      }
      thread_data.filename = "";
      thread_data.f_attach = "";
    } else if (thread_data.insert_filenames.size() > 0) {
      system(("mv " + args.download_directory + "/" +
          thread_data.insert_filenames.back() + TMP_EXT + " " +
          args.download_directory + "/" +
          thread_data.insert_filenames.back()).c_str());
      ++thread_data.fcount;
    } else {
      if (regex_search(thread_data.data_format, regex("grib2", regex::icase)) &&
          regex_search(request_values.ofmt, regex("netcdf", regex::icase)) &&
          !regex_search(thread_data.filename, NC_END)) {
        sort_to_nc_order(temp_file, output_file + ".sorted");
        system(("mv " + output_file + ".sorted " + temp_file).c_str());
      }
      system(("mv " + temp_file + " " + output_file).c_str());
      ++thread_data.fcount;
    }
  }
}

void build_file(ThreadData& thread_data, bool& is_temporal_subset) {

  // initializations
  Timer thread_timer;
  if (args.get_timings) {
    thread_timer.start();
  }
  GridData grid_data;
  grid_data.wrote_header = false;
  grid_data.parameter_mapper = thread_data.parameter_mapper;
  thread_data.f_attach = "";
  thread_data.insert_filenames.clear();
  thread_data.wget_filenames.clear();
  if (!request_values.ofmt.empty()) {
    thread_data.output_format = request_values.ofmt;
  } else {
    thread_data.output_format = thread_data.data_format;
  }

  OutputStream outs;
  unique_ptr<unordered_set<string>> nts_table;
  auto is_multi = !(thread_data.multi_set->find(thread_data.file_code) ==
      thread_data.multi_set->end());
  if (args.is_test || !file_exists(thread_data, nts_table, outs, is_multi)) {

    // connect to the database and submit the main byte query
    Server srv(metautils::directives.metadb_config, 300);
    if (!srv) {
      throw runtime_error("Error: build_file() unable to connect to metadata "
          "server: '" + srv.error() + "'");
    }

    // if this is a test run or the file doesn't already exist:
    //   need to process byte data for a test run
    //   need to build the file for an actual subset run
    // build the necessary database queries
    LocalQuery byte_query, byte_query_no_dates, query_count_files;
    build_queries(srv, thread_data, byte_query, byte_query_no_dates,
        query_count_files);

    if (byte_query.submit(srv) < 0) {
      throw runtime_error("Error: " + byte_query.error() + "\nQuery: " +
          byte_query.show());
    }

    // check for temporal subsetting
    if (!args.is_test && !is_temporal_subset) {
      if (byte_query_no_dates.submit(srv) < 0) {
        throw runtime_error("Error: " + byte_query_no_dates.error() + "\n"
            "Query: " + byte_query_no_dates.show());
      }
      if (byte_query.num_rows() < byte_query_no_dates.num_rows()) {
        is_temporal_subset = true;
      }
    }
    NCTime nc_time;
    SpatialBitmap spatial_bitmap;
    int num_values_in_subset;
    if (!linked_to_full_file(thread_data, outs, nc_time, spatial_bitmap,
        num_values_in_subset, srv, query_count_files, byte_query.num_rows())) {

      // if the request does not ask for the full file, proceed with processing
      //   of the subset
      grid_data.subset_definition.latitude.south = request_values.slat;
      grid_data.subset_definition.latitude.north = request_values.nlat;
      grid_data.subset_definition.longitude.west = request_values.wlon;
      grid_data.subset_definition.longitude.east = request_values.elon;
      grid_data.path_to_gauslat_lists = args.SHARE_DIRECTORY + "/GRIB";
      build_subset(thread_data, grid_data, nc_time, spatial_bitmap,
          num_values_in_subset, byte_query, nts_table, outs, is_multi);
    } else {
      thread_data.fcount = 1;
    }
    if (!args.is_test && thread_data.output_format != thread_data.data_format &&
        is_multi && !thread_data.f_attach.empty()) {

      // convert the data format, if necessary
      do_conversion(thread_data);
    }

    // disconnect from the database
    srv.disconnect();
  }
  if (!thread_data.insert_filenames.empty()) {

    // update wfrqst with any file names reported by the thread
    Server srv(metautils::directives.rdadb_config, 300);
    if (!srv) {
      throw runtime_error("build_file(): unable to connect to RDADB server: '" +
          srv.error() + "'");
    }
    for (const auto& fname : thread_data.insert_filenames) {
      insert_into_wfrqst(srv, args.rqst_index, fname, thread_data.output_format,
          thread_data.filelist_display_order);
    }
    srv.disconnect();
  }
  if (args.get_timings) {

    // record timings, if requested
    thread_timer.stop();
    thread_data.timing_data.thread = thread_timer.elapsed_time();
  }

  // mark the thread as completed
  thread_data.has_finished = true;
}

void build_subset_files(const std::vector<InputFile>& input_files,
    const unordered_set<string>& multiple_parameter_files_code_set,
    ThreadData *thread_data, std::vector<string>& wget_list, long long&
    size_input, size_t& fcount, bool& is_temporal_subset) {
  fcount = 0;
  auto num_threads_to_create = args.num_threads;
  if (args.is_test) {

    // force test runs to only use two threads
    num_threads_to_create = 2;
  } else {

    // if not a test run, remove any core files that might have been left from a
    //   previously-failed run
    stringstream oss, ess;
    mysystem2("/bin/rm -f " + args.download_directory + "/core*", oss, ess);
  }
  thread thread_list[num_threads_to_create];
  size_t num_created_threads = 0;
  auto thread_index = 0;
  auto filelist_display_order = 1;
  size_input = 0;
  long long write_bytes = 0;
  for (const auto& input_file : input_files) {
    while (num_created_threads == num_threads_to_create) {
      for (size_t n = 0; n < num_created_threads; ++n) {
        if (thread_data[n].has_started && thread_data[n].has_finished) {
          thread_list[n].join();
          thread_data[n].has_started = false;
          for (const auto& fname : thread_data[n].wget_filenames) {
            wget_list.emplace_back(fname);
          }
          fcount += thread_data[n].fcount;
          if (args.is_test || args.get_timings) {
            timing_data.add(thread_data[n].timing_data);
          }
          if (args.is_test) {
            write_bytes += thread_data[n].timing_data.read_bytes;
          } else {
            write_bytes += thread_data[n].write_bytes;
          }
          if (!args.ignore_volume && write_bytes > 900000000000) {
// join all remaining running threads before terminating
for (size_t m = 0; m < num_created_threads; ++m) {
if (thread_data[m].has_started) {
thread_list[m].join();
}
}
              terminate("Error: requested volume is too large", "Error: "
                  "request volume too large");
          }
          thread_index = n;
          thread_data[n].timing_data.reset();
          --num_created_threads;
          break;
        }
      }
    }
    string file_code, file_id, data_format, data_format_code;
    long long data_size;
    tie(file_code, file_id, data_size, data_format, data_format_code) =
        input_file;
    auto idx = file_id.rfind("/");
    string filename;
    if (idx != string::npos) {
      filename = file_id.substr(idx);
    } else {
      filename = "/" + file_id;
    }
    replace_all(filename, ".tar", "");
    if (data_format == "WMO_GRIB2") {
      if (!regex_search(filename, regex(".grb2")) && !regex_search(filename,
          regex(".grib2")))
        filename += ".grb2";
    }
    thread_data[thread_index].file_code = file_code;
    thread_data[thread_index].file_id = file_id;
    thread_data[thread_index].data_format = data_format;
    thread_data[thread_index].data_format_code = data_format_code;
    thread_data[thread_index].filename = filename;
    thread_data[thread_index].size_input = data_size;
    thread_data[thread_index].filelist_display_order = filelist_display_order;
    thread_data[thread_index].has_started = true;
    thread_data[thread_index].has_finished = false;
    thread_list[thread_index] = thread(build_file, ref(thread_data[
        thread_index]), ref(is_temporal_subset));
    size_input += data_size;
    ++thread_index;
    ++num_created_threads;
    ++filelist_display_order;
  }
  for (size_t n = 0; n < num_threads_to_create; ++n) {
    if (thread_data[n].has_started) {
      thread_list[n].join();
      for (const auto& fname : thread_data[n].wget_filenames) {
        wget_list.emplace_back(fname);
      }
      fcount += thread_data[n].fcount;
      if (args.is_test || args.get_timings) {
        timing_data.add(thread_data[n].timing_data);
      }
      if (args.is_test) {
        write_bytes += thread_data[n].timing_data.read_bytes;
      } else {
        write_bytes += thread_data[n].write_bytes;
      }
      thread_data[n].timing_data.reset();
    }
  }
  if (!args.ignore_volume && write_bytes > 900000000000) {
      terminate("Error: requested volume is too large", "Error: request volume "
          "too large");
  }
}

} // end namespace subconv
