#include <iostream>
#include <sstream>
#include <subconv.hpp>
#include <PostgreSQL.hpp>
#include <metadata.hpp>
#include <strutils.hpp>
#include <utils.hpp>

using namespace PostgreSQL;
using std::runtime_error;
using std::shared_ptr;
using std::stof;
using std::stoi;
using std::stoll;
using std::string;
using std::stringstream;
using std::unordered_map;
using std::unordered_set;
using strutils::ng_gdex_id;
using strutils::replace_all;
using strutils::split;
using strutils::substitute;
using strutils::trim;
using unixutils::mysystem2;

namespace subconv {

void parse_subset_request(string dataset_block, int& num_parameters,
    short& subflag, unordered_map<string, string>& unique_formats_map,
    shared_ptr<unordered_set<string>>& include_parameter_codes_set) {
  include_parameter_codes_set.reset(new unordered_set<string>);
  auto info_parts = split(args.rinfo, ";");
  for (auto part : info_parts) {
    trim(part);
    auto nvp = split(part, "=");
    if (nvp[0] == "dsnum") {
      metautils::args.dsid = ng_gdex_id(nvp[1]);
      if (!args.is_test && !args.ignore_restrictions) {

        // restrictions
        if (!dataset_block.empty() && (dataset_block == "all" ||
            dataset_block == metautils::args.dsid)) {
          stringstream oss, ess;
          mysystem2("/bin/tcsh -c 'dsrqst -sr -ri " + args.rqst_index + " -rs "
              "E'", oss, ess);
          throw runtime_error("Refuse to process data request " +
              args.rqst_index + " for " +
              request_values.ancillary.user_email + "\n" + args.rinfo);
        }
        LocalQuery query("select sum(size_request) from dsrqst where email = "
            "'" + request_values.ancillary.user_email + "' and status = 'O'");
        Row row;
        if (query.submit(metadata_server) == 0 && query.fetch_row(row) &&
            !row[0].empty() && stoll(row[0]) > 1000000000000) {
          stringstream oss, ess;
          mysystem2("/bin/tcsh -c 'dsrqst -sr -ri " + args.rqst_index + " -rs "
              "E'", oss, ess);
            terminate("Error: quota exceeded - you will need to purge some of "
                "your existing requests first", "User has exceeded quota");
        }
      }
      request_values.metadata_dsid = metautils::args.dsid;
    } else if (nvp[0] == "startdate") {
      request_values.startdate = substitute(nvp[1], "-", "");
      replace_all(request_values.startdate, ":", "");
      replace_all(request_values.startdate, " ", "");
      if (request_values.startdate.length() != 12) {
          terminate("Error: start date not specified properly\nYour request:"
              "\n" + args.rinfo, "Error: start date not specified properly");
      }
    } else if (nvp[0] == "enddate") {
      request_values.enddate = substitute(nvp[1], "-", "");
      replace_all(request_values.enddate, ":", "");
      replace_all(request_values.enddate, " ", "");
      if (request_values.enddate.length() != 12) {
          terminate("Error: end date not specified properly\nYour request:\n" +
              args.rinfo, "Error: end date not specified properly");
      }
    } else if (nvp[0] == "dates" && nvp[1] == "init") {
      request_values.dates_are_init = true;
    } else if (nvp[0] == "parameters") {
      if (!nvp[1].empty()) {
        auto params = split(nvp[1], ",");
        num_parameters = params.size();
        for (const auto& param : params) {
          auto pparts = split(param, "!");
          if (pparts.size() != 2) {
              terminate("Error: parameter(s) not specified properly\nYour "
                  "request:\n" + args.rinfo,
                  "Error: parameter(s) not specified properly");
          }
          if (unique_formats_map.find(pparts[0]) == unique_formats_map.end()) {
            LocalQuery query("format", "WGrML.formats", "code = " + pparts[0]);
            string format;
            if (query.submit(metadata_server) < 0) {
              terminate("Database error", query.error());
            } else if (query.num_rows() == 1) {
              Row row;
              query.fetch_row(row);
              format = row[0];
            }
            unique_formats_map.emplace(pparts[0], format);
            request_values.format_codes.emplace_back(pparts[0]);
          }
          request_values.parameters.emplace_back(param);
          pparts = split(param, ":");
          if (pparts.size() == 2) {
            if (include_parameter_codes_set->find(pparts[1]) ==
                include_parameter_codes_set->end()) {
              include_parameter_codes_set->emplace(pparts[1]);
            }
          }
        }
      }
    } else if (nvp[0] == "product") {
      request_values.product = nvp[1];
    } else if (nvp[0] == "grid_definition") {
      request_values.grid_definition = nvp[1];
    } else if (nvp[0] == "level") {
      request_values.level = nvp[1];
    } else if (nvp[0] == "tindex") {
      request_values.tindex = nvp[1];
    } else if (nvp[0] == "ofmt") {
      request_values.ofmt = nvp[1];
    } else if (nvp[0] == "nlat") {
      if (!nvp[1].empty()) {
        request_values.lat_s = nvp[1];
        if (request_values.lat_s.front() == '-') {
          request_values.lat_s = request_values.lat_s.substr(1) + "S";
        } else {
          request_values.lat_s.push_back('N');
        }
        request_values.nlat = stof(nvp[1]);
        subflag |= 0x4;
        if (request_values.nlat < -90. || request_values.nlat > 90.) {
            terminate("Error: north latitude (nlat) must be in the range of "
              "-90 to 90\nYour request:\n" + args.rinfo,
              "Error: north latitude out of range");
        }
      }
    } else if (nvp[0] == "slat") {
      if (!nvp[1].empty()) {
        request_values.slat = stof(nvp[1]);
        if (request_values.slat < -90. || request_values.slat > 90.) {
            terminate("Error: south latitude (slat) must be in the range of "
                "-90 to 90\nYour request:\n" + args.rinfo,
                "Error: south latitude out of range");
        }
      }
    } else if (nvp[0] == "wlon") {
      if (!nvp[1].empty()) {
        request_values.lon_s = nvp[1];
        if (request_values.lon_s.front() == '-') {
          request_values.lon_s = request_values.lon_s.substr(1) + "W";
        } else {
          request_values.lon_s.push_back('E');
        }
        request_values.wlon = stof(nvp[1]);
        if (request_values.wlon < -180. || request_values.wlon > 360.) {
            terminate("Error: west longitude (wlon) must be in the range of "
                "-180 to 360\nYour request:\n" + args.rinfo,
                "Error: west longitude out of range");
        }
      }
    } else if (nvp[0] == "elon") {
      if (!nvp[1].empty()) {
        request_values.elon = stof(nvp[1]);
        if (request_values.elon < -180. || request_values.elon > 360.) {
            terminate("Error: east longitude (elon) must be in the range of "
                "-180 to 360\nYour request:\n" + args.rinfo,
                "Error: east longitude out of range");
        }
      }
    } else if (nvp[0] == "ststep" && nvp[1] == "yes") {
      request_values.ststep = true;
    } else if (nvp[0].substr(0,7) == "topt_mo" && nvp[1] == "on") {
      request_values.topt_mo[stoi(substitute(nvp[0], "topt_mo", ""))] = true;
      request_values.topt_mo[0] = true;
    }
  }
  if (metautils::args.dsid.empty()) {
    terminate("Error: bad request\nYour request:\n" + args.rinfo,
        "Error: no dataset number given");
  }
  if (request_values.nlat < 99. && request_values.slat > -99.) {
    request_values.ladiff = request_values.nlat - request_values.slat;
  }
  if (request_values.elon < 999. && request_values.wlon > -999.) {
    request_values.lodiff = request_values.elon - request_values.wlon;
  }
}

} // end namespace subconv
