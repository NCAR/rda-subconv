#include <subconv.hpp>
#include <metadata.hpp>
#include <PostgreSQL.hpp>
#include <strutils.hpp>
#include <utils.hpp>
#include <metahelpers.hpp>
#include <myerror.hpp>

using namespace PostgreSQL;
using std::cout;
using std::endl;
using std::runtime_error;
using std::string;
using strutils::ftos;
using strutils::itos;
using strutils::lltos;
using strutils::replace_all;
using strutils::split;
using strutils::sql_ready;
using strutils::substitute;

namespace subconv {

extern "C" void clean_up() {
  print_myerror();
}

bool ignore_volume() {
  if (args.ignore_volume) {
    return true;
  } else {
    if (request_values.nlat < 99990. && request_values.nlat ==
        request_values.slat && request_values.wlon == request_values.elon) {
      return true;
    }
    return false;
  }
}

void update_subflag_bit(short bit, short& subflag, void *data) {
  switch (bit) {
    case 1: {
      LocalQuery query("count(distinct parameter)", "WGrML." + request_values.
          metadata_dsid + "_agrids_cache");
      if (query.submit(metadata_server) < 0) {
        throw runtime_error("update_subflag_bit(): query error: '" +
            query.error() + "'");
      } else if (query.num_rows() == 1) {
        Row row;
        query.fetch_row(row);
        auto num_parameters = reinterpret_cast<int *>(data);
        if (*num_parameters < std::stoi(row[0])) {
          subflag |= 0x1;
        }
      }
      break;
    }
    case 2: {
      if (*(reinterpret_cast<bool *>(data))) {
        subflag |= 0x2;
      }
      break;
    }
    default: {
      throw runtime_error("update_subflag_bit(): can't set bit " + itos(bit) +
          " for subflag");
    }
  }
}

bool is_selected_parameter(const ThreadData& thread_data, Grid *grid) {
  auto grib = reinterpret_cast<GRIBGrid *>(grid);
  auto grib2 = reinterpret_cast<GRIB2Grid *>(grid);
  std::stringstream ss;
  auto key = thread_data.data_format_code + "!";
  if (thread_data.data_format == "WMO_GRIB2") {
    ss << grid->source() << "-" << grib->sub_center_id() << "." << grib->
        parameter_table_code() << "-" << grib2->local_table_code() << ":" <<
        grib2->discipline() << "." << grib2->parameter_category() << "." <<
        grid->parameter();
  } else if (thread_data.data_format == "WMO_GRIB1") {
    ss << grid->source() << "-" << grib->sub_center_id() << "." << grib->
        parameter_table_code() << ":" << grid->parameter();
  }
  key += ss.str();
  return !(std::find(request_values.parameters.begin(),
      request_values.parameters.end(), key) == request_values.parameters.end());
}

void do_grid_fixups() {

  if (std::regex_search(metautils::args.dsid, std::regex("^d09300[01]")) &&
      (request_values.grid_definition == "62" ||
      request_values.grid_definition == "64")) {

    // grids 62 and 64 were consolidated to 83 for ds093.0 and ds093.1
    request_values.grid_definition = "83";
  }
}

void insert_into_wfrqst(Server& server, string request_index, string filename,
    string data_format, size_t filelist_display_order) {
  auto disp_order = itos(filelist_display_order);
  auto insert_s = request_index + ", " + disp_order + ", '" + data_format +
      "', '', '" + filename + "', 'O'";
  auto on_conflict_s = "(rindex, wfile) do update set disp_order = excluded."
      "disp_order, data_format = excluded.data_format";
  if (server.insert(
        "dssdb.wfrqst",
        "rindex, disp_order, data_format, file_format, wfile, status",
        insert_s,
        on_conflict_s
        ) < 0) {
    throw runtime_error("insert_into_wfrqst(): inserting '" + insert_s + "', "
        "insert error: " + server.error());
  }
}

string create_user_email_notice(xmlutils::ParameterMapper& parameter_mapper,
    xmlutils::LevelMapper& level_mapper, std::unordered_map<string, string>&
    unique_formats_map) {
  std::ofstream email_notice_fs((args.download_directory +
      "/.email_notice").c_str());
  email_notice_fs << "From: <SENDER>" << endl;
  email_notice_fs << "To: <RECEIVER>" << endl;
  email_notice_fs << "Cc: <CCD>" << endl;
  email_notice_fs << "Subject: Your <DSID> Data Request <RINDEX> is Ready" <<
      endl;
  email_notice_fs << "The subset of <DSID> - '<DSTITLE>' that you requested is "
      "ready for you to download." << endl << endl;
  auto dsrqst_note = string("Subset details:")+"\n";
  if (!request_values.startdate.empty() && !request_values.enddate.empty()) {
    dsrqst_note += "  Date range: " + request_values.startdate + " to " +
        request_values.enddate + "\n";
  }
  if (request_values.ststep) {
    dsrqst_note += "    *Each timestep in its own file\n";
  }
  if (request_values.topt_mo[0]) {
    std::vector<string> months{ "", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
        "Jul", "Aug", "Sep", "Oct", "Nov", "Dec" };
    dsrqst_note += "  Include these months only:";
    for (size_t n = 1; n <= 12; ++n) {
      if (request_values.topt_mo[n]) {
        dsrqst_note += " " + months[n];
      }
    }
    dsrqst_note += "\n";
  }
  if (request_values.parameters.size() > 0) {
    std::unordered_set<string> unique_parameter_set;
    dsrqst_note += "  Parameter(s): \n";
    for (const auto& parameter : request_values.parameters) {
      auto pparts = split(parameter, "!");
      auto detailed_parameter = metatranslations::detailed_parameter(
            parameter_mapper, unique_formats_map[pparts[0]], pparts[1]);
      auto idx = detailed_parameter.find(" <small");
      if (idx != string::npos) {
        detailed_parameter = detailed_parameter.substr(0, idx);
      }
      if (unique_parameter_set.find(detailed_parameter) ==
            unique_parameter_set.end()) {
        unique_parameter_set.emplace(detailed_parameter);
        dsrqst_note += "     " + detailed_parameter+"\n";
      }
    }
  }
  if (!request_values.level.empty()) {
    dsrqst_note += "  Level(s):\n";
    if (request_values.format_codes.size() > 0) {
      std::unordered_set<string> unique_level_map;
      auto levels = split(request_values.level, ",");
      for (const auto& level : levels) {
        LocalQuery level_information_query("map, type, value", "WGrML.levels",
            "code = " + level);
        if (level_information_query.submit(metadata_server) == 0 &&
              level_information_query.num_rows() > 0) {
          Row level_information_query_result;
          level_information_query.fetch_row(level_information_query_result);
          auto detailed_level = metatranslations::detailed_level(
                level_mapper, unique_formats_map[
                request_values.format_codes.front()],
                level_information_query_result[0],
                level_information_query_result[1],
                level_information_query_result[2], false);
          replace_all(detailed_level, "<nobr>", "");
          replace_all(detailed_level, "</nobr>", "");
          if (unique_level_map.find(detailed_level) ==
                unique_level_map.end()) {
            unique_level_map.emplace(detailed_level);
            dsrqst_note += "     " + detailed_level + "\n";
          }
        } else
          dsrqst_note += "     " + level + "\n";
      }
    } else
      dsrqst_note += "     " + request_values.level + "\n";
  }
  if (!request_values.product.empty()) {
    dsrqst_note += "  Product(s):\n";
    auto pparts = split(request_values.product, ",");
    for (const auto& part : pparts) {
      LocalQuery time_range_query("time_range", "WGrML.time_ranges", "code = " +
          part);
      if (time_range_query.submit(metadata_server) == 0 &&
            time_range_query.num_rows() > 0) {
        Row time_range_query_result;
        time_range_query.fetch_row(time_range_query_result);
        dsrqst_note += "    " + time_range_query_result[0] + "\n";
      } else
        dsrqst_note += "    " + part + "\n";
    }
  }
  if (!request_values.grid_definition.empty()) {
    LocalQuery grid_information_query("definition, def_params", "WGrML."
        "grid_definitions", "code = " + request_values.grid_definition);
    auto grid_definition=request_values.grid_definition;
    if (grid_information_query.submit(metadata_server) == 0 &&
        grid_information_query.num_rows() > 0) {
      Row grid_information_query_result;
      grid_information_query.fetch_row(grid_information_query_result);
      grid_definition=gridutils::convert_grid_definition(
          grid_information_query_result[0] + "<!>" +
          grid_information_query_result[1]);
      replace_all(grid_definition, "&deg;", "-deg");
      replace_all(grid_definition, "<small>", "");
      replace_all(grid_definition, "</small>", "");
    }
    dsrqst_note += "  Grid: " + grid_definition + "\n";
  }
  if (!request_values.ofmt.empty()) {
    dsrqst_note += "  Output format conversion: " + request_values.ofmt + "\n";
  }
  if (request_values.nlat < 9999. && request_values.elon < 9999. &&
      request_values.slat > -9999. && request_values.wlon > -9999.) {
    if (request_values.nlat == request_values.slat && request_values.wlon ==
        request_values.elon) {
      dsrqst_note += "  Spatial subsetting (single gridpoint):\n";
      dsrqst_note += "    Latitude: " + ftos(request_values.nlat, 8) + "\n";
      dsrqst_note += "    Longitude: " + ftos(request_values.wlon, 8) + "\n";
    } else {
      dsrqst_note += "  Spatial subsetting (bounding box):\n";
      dsrqst_note += "    Latitudes (top/bottom): " + ftos(request_values.nlat,
          0) + " / " + ftos(request_values.slat, 0) + "\n";
      dsrqst_note += "    Longitudes (left/right): " + ftos(request_values.wlon,
          0) + " / " + ftos(request_values.elon, 0) + "\n";
    }
  }
  email_notice_fs << dsrqst_note << endl;
  if (request_values.ancillary.location.empty()) {
    email_notice_fs << "You will need to be signed in to the RDA web server at "
        "https://rda.ucar.edu/.  Then you will find your data at: "
        "<DSSURL><WHOME>/<RQSTID>/" << endl;
    email_notice_fs << endl;
    email_notice_fs << "Your data will remain on our system for <DAYS> days. If "
        "this is not sufficient time for you to retrieve your data, you can "
        "extend the time from the user dashboard (link appears at the top of "
        "our web pages when you are signed in). Expand the \"Customized Data "
        "Requests\" section to manage your data requests." << endl;
  } else {
    email_notice_fs << "You will find your data in:  <WHOME>/<RQSTID>/" << endl;
  }
  email_notice_fs << endl;
  email_notice_fs << "If you have any questions related to this data request, "
      "please let me know by replying to this email." << endl;
  email_notice_fs << endl;
  email_notice_fs << "Sincerely," << endl;
  email_notice_fs << "<SPECIALIST>" << endl;
  email_notice_fs << "NCAR/CISL RDA" << endl;
  email_notice_fs << "<PHONENO>" << endl;
  email_notice_fs << "<SENDER>" << endl;
  email_notice_fs.close();
  return dsrqst_note;
}

void update_rdadb(long long size_input, size_t fcount, string dsrqst_note,
    short subflag) {
  std::unique_ptr<Timer> timer(nullptr);
  if (args.get_timings) {
    timer.reset(new Timer);
    timer->start();
  }
  if (request_values.ancillary.tarflag != "Y") {
    if (rdadb_server.insert(
          "dssdb.wfrqst",
          "rindex, disp_order, data_format, file_format, wfile, type, status",
          args.rqst_index + ", -1, NULL, NULL, 'unix-wget." + args.rqst_index +
              ".csh', 'S', 'O'",
          "(rindex, wfile) do update set disp_order = -1, data_format = NULL"
          ) < 0) {
      throw runtime_error("update_rdadb(): insert error: " +
          rdadb_server.error());
    }
    ++fcount;
    if (rdadb_server.insert(
          "dssdb.wfrqst",
          "rindex, disp_order, data_format, file_format, wfile, type, status",
          args.rqst_index + ", -1, NULL, NULL, 'unix-curl." + args.rqst_index +
              ".csh', 'S', 'O'",
          "(rindex, wfile) do update set disp_order = -1, data_format = NULL"
          ) < 0) {
      throw runtime_error("update_rdadb(): insert error: " +
          rdadb_server.error());
    }
    ++fcount;
    if (rdadb_server.insert(
          "dssdb.wfrqst",
          "rindex, disp_order, data_format, file_format, wfile, type, status",
          args.rqst_index + ", -1, NULL, NULL, 'dos-wget." + args.rqst_index +
              ".bat', 'S', 'O'",
          "(rindex, wfile) do update set disp_order = -1, data_format = NULL"
          ) < 0) {
      throw runtime_error("update_rdadb(): insert error: " +
          rdadb_server.error());
    }
    ++fcount;
  }
  auto rqsttype = "S";
  if (!request_values.ofmt.empty()) {
    rqsttype = "T";
  }
  if (rdadb_server.update("dssdb.dsrqst", "size_input = " + lltos(size_input) +
      ", fcount = " + itos(fcount) + ", rqsttype = '" + rqsttype + "', note = '"
      + sql_ready(dsrqst_note) + "', subflag = " + itos(subflag), "rindex = " +
      args.rqst_index) < 0) {
    throw runtime_error("update_rdadb(): update error: " + rdadb_server.
        error());
  }
  if (args.get_timings) {
    timer->stop();
    timing_data.db += timer->elapsed_time();
  }
}

void print_timings() {
  cout.setf(std::ios::fixed);
  cout.precision(2);
  if (args.get_timings) {
    args.main_timer.stop();
  }
  cout << "Date/time: " << dateutils::current_date_time().to_string(
      "%Y-%m-%d %H:%MM:%SS %Z") << endl;
  cout << "Host: " << strutils::token(unixutils::host_name(), ".", 0) << endl;
  cout << "# of threads: " << args.num_threads << endl;
  cout << "Total wallclock time: " << args.main_timer.elapsed_time() <<
      " seconds" << endl;
  cout << "Total database time: " << timing_data.db << " seconds" << endl;
  cout << "Total time in threads: " << timing_data.thread << " seconds" << endl;
  cout << "Total read time: " << timing_data.read << " seconds" << endl;
  cout << "  Total bytes read: " << timing_data.read_bytes << " bytes" << endl;
  cout << "  Read rate: " << timing_data.read_bytes / 1000000. /
      timing_data.read << " MB/sec" << endl;
  cout << "  Average record length: " << static_cast<double>(
      timing_data.read_bytes) / timing_data.num_reads << " bytes" << endl;
  cout << "Total write time: " << timing_data.write << " seconds" << endl;
  cout << "Total GRIB2 uncompress time: " << timing_data.grib2u << " seconds" <<
      endl;
  cout << "Total GRIB2 compress time: " << timing_data.grib2c << " seconds" <<
      endl;
  cout << "Total netCDF conversion time: " << timing_data.nc << " seconds" <<
      endl;
}

string parameter_code(Server& server, string parameter) {
  LocalQuery q("code", "IGrML.parameters", "parameter = '" + parameter + "'");
  Row r;
  if (q.submit(server) < 0 || !q.fetch_row(r)) {
    terminate("Error: database error", "Error: " + q.error() + "\nQuery: " + q.
        show());
  }
  return r[0];
}

} // end namespace subconv
