/*
** subconv.cpp - subset and convert
**
**   This is the 'dsrqst' back-end subsetter/converter primarily for the CFSR
**   and CFSv2 datasets. It will subset GRIB2 files and convert GRIB2 to netCDF
**   and CSV output.
*/
#include "../include/subconv.hpp"
#include <iostream>
#include <thread>
#include <PostgreSQL.hpp>
#include <utils.hpp>
#include <metadata.hpp>
#include <myexception.hpp>

using namespace PostgreSQL;
using floatutils::myequalf;
using std::cout;
using std::endl;
using std::make_shared;
using std::shared_ptr;
using std::string;
using std::to_string;
using std::unordered_set;
using std::vector;

metautils::Directives metautils::directives;
metautils::Args metautils::args;
string myerror = "";
string mywarning = "";
string myoutput = "";
subconv::Args subconv::args;
subconv::RequestValues subconv::request_values;
subconv::TimingData subconv::timing_data;
Server subconv::metadata_server;
Server subconv::rdadb_server;
char subconv::locflag;

int main(int argc, char **argv) {
  try {

    // check the command usage
    subconv::check_usage(argc);

    // read the master configuration for handling metadata
    metautils::read_config("subconv", "", false);

    // parse the arguments to the subconv command
    subconv::parse_args(argc, argv);

    // set the exit function
    atexit(subconv::clean_up);

    // connect to the database servers
    subconv::metadata_server.connect(metautils::directives.metadb_config);
    if (!subconv::metadata_server) {
      throw my::OpenFailed_Error("unable to connect to the metadata server at "
          "startup: '" + subconv::metadata_server.error() + "'");
    }
    subconv::rdadb_server.connect(metautils::directives.rdadb_config);
    if (!subconv::rdadb_server) {
      throw my::OpenFailed_Error("unable to connect to the RDADB server at "
          "startup: '" + subconv::rdadb_server.error() + "'");
    }

    // fill any ancillary request values (RequestValues.ancillary)
    subconv::fill_ancillary_request_values();

    // read the configuration for subconv
    auto subconv_directives = subconv::read_config();
    if (subconv::args.batch_type != 0x0 && !subconv_directives.pbs_options.
        empty()) {
      cout << "-l " << subconv_directives.pbs_options << endl;
      return 0;
    }

    // parse the request string
    int num_parameters = 0;
    // subflag is the cumulative subset type bit flag:
    //   1-parameter, 2-temporal, 4-spatial
    short subflag = 0x0;
    std::unordered_map<string, string> unique_formats_map;
    shared_ptr<unordered_set<string>> include_parameter_codes_set;
    subconv::parse_subset_request(subconv_directives.dataset_block,
        num_parameters, subflag, unique_formats_map,
        include_parameter_codes_set);

/*
// fill the map of web files for the requested dataset
  std::unordered_map<string,subconv::RDAFileInfo> rdafile_map;
  subconv::fill_rdafile_map(rdafile_map);
*/

    // check for out-of-order latitudes
    if (subconv::request_values.nlat < subconv::request_values.slat) {
      subconv::terminate("Error: the north latitude must be north of the south "
          "latitude\nYour request: \n" + subconv::args.rinfo, "Error: the "
          "north latitude must be north of the south latitude\n" +
          subconv::args.rinfo);
    }

    // modify any grid definition codes that need to be fixed
    subconv::do_grid_fixups();

    // check for conditions where an excessively large request would be allowed
    //   to proceed
    subconv::args.ignore_volume = subconv::ignore_volume();

    // set csv data for csv requests
    xmlutils::ParameterMapper parameter_mapper(subconv::args.SHARE_DIRECTORY +
        "/metadata/ParameterTables");
    xmlutils::LevelMapper level_mapper(subconv::args.SHARE_DIRECTORY +
        "/metadata/LevelTables");
    auto csv_data = subconv::csv_data(parameter_mapper, level_mapper,
        unique_formats_map);

    // update subflag for parameter subsetting
    subconv::update_subflag_bit(1, subflag, &num_parameters);

    // make sure that a global spatial selection is not interpreted as a smaller
    //   spatial selection just because of decimal precision
    if (myequalf(subconv::request_values.nlat, 90., 0.001) && myequalf(subconv::
        request_values.slat, -90., 0.001) && myequalf(subconv::request_values.
        wlon, -180., 0.001) && myequalf(subconv::request_values.elon, 180.,
        0.001)) {
      subconv::request_values.nlat = subconv::request_values.elon = 99999.;
      subconv::request_values.slat = subconv::request_values.wlon = -99999.;
    }

    // check to see if user individually selected all months and if so, turn off
    //   individual month subsetting
    if (subconv::request_values.topt_mo[0] == 1) {
      auto num_months_selected = 0;
      for (size_t n = 1; n < 13; ++n) {
        if (subconv::request_values.topt_mo[n] == 1)
          ++num_months_selected;
      }
      if (num_months_selected == 12) {
        subconv::request_values.topt_mo[0] = 0;
      }
    }

    // build the query constructs that will be used for the DB queries required
    //   to fulfill the request
    subconv::QueryData query_data;
    subconv::build_query_constructs(query_data);

    // get the list of input files
    auto input_files = subconv::input_files(query_data);

    // if the input file list is empty, then no data match the request
    if (input_files.empty()) {
      subconv::terminate("Error: no data match the request\nYour request: \n" +
          subconv::args.rinfo, "Error: no data match the request\n " +
          subconv::args.rinfo);
    }

    // set the email notice template and initialize fcount
    if (!subconv::args.is_test) {
      subconv::rdadb_server.update("dssdb.dsrqst", "enotice = '" + subconv::
          args.download_directory + "/.email_notice', fcount = " + to_string(
          input_files.size()), "rindex = " + subconv::args.rqst_index);
    }

    // if the number of threads was not specified at startup, compute the
    //   number, with a maximum of 6 and a minimum of 2
    if (subconv::args.num_threads == 0) {
      subconv::args.num_threads = std::min(std::max(static_cast<int>((
          input_files.size() + 9) / 10), 2), 6);
    }

    // if this is a sbatch options run and there are a large number of input
    //   files, then set num_reads to a very large number, compute the dynamic
    //   sbatch options from that, and exit
    if (subconv::args.batch_type != 0x0 && input_files.size() > 1000) {
      subconv::timing_data.num_reads = 0x40000000 | input_files.size();
      cout << subconv::batch_options(subconv_directives) << endl;
      return 0;
    }

    // identify files that have multiple parameters in them
    auto multiple_parameter_files_code_set = subconv::multiple_parameter_files(
        input_files, query_data.conditions.inventory);

/*
// done with RDA files hash, so clear it
  rdafile_map.clear();
*/

    // clear dssdb.wfrqst
    if (subconv::args.batch_type == 0x0) {
      subconv::rdadb_server._delete("dssdb.wfrqst", "rindex = " + subconv::args.
          rqst_index);
    }

    // get the location flag for the input files
    LocalQuery locflag_query("locflag", "dssdb.dataset", "dsid = '" +
        metautils::args.dsid + "'");
    if (locflag_query.submit(subconv::rdadb_server) < 0) {
      throw my::BadOperation_Error("unable to get input files location flag: '"
          + locflag_query.error() + "'");
    }
    Row locflag_row;
    if (!locflag_query.fetch_row(locflag_row)) {
      throw my::BadOperation_Error("error reading location flag result: '" + 
          locflag_query.error() + "'");
      return 1;
    }
    subconv::locflag = locflag_row[0].front();

    // done with the database servers, so disconnect
    subconv::metadata_server.disconnect();
    subconv::rdadb_server.disconnect();

    // initialize the data for each thread
    subconv::ThreadData thread_data[subconv::args.num_threads];
    if (subconv::args.get_timings) {
      subconv::args.db_timer.start();
    }
    auto webhome = metautils::directives.data_root + "/" + metautils::args.dsid;
    if (subconv::args.get_timings) {
      subconv::args.db_timer.stop();
      subconv::timing_data.db += subconv::args.db_timer.elapsed_time();
    }
    for (size_t n = 0; n < subconv::args.num_threads; ++n) {
      thread_data[n].webhome = webhome;
      thread_data[n].include_parameter_codes_set = include_parameter_codes_set;
      thread_data[n].uConditions = query_data.conditions.union_;
      thread_data[n].uConditions_no_dates = query_data.conditions.
          union_non_date;
      thread_data[n].multi_set = make_shared<unordered_set<string>>(
          multiple_parameter_files_code_set);
      thread_data[n].parameter_mapper.reset(new xmlutils::ParameterMapper(
          subconv::args.SHARE_DIRECTORY + "/metadata/ParameterTables"));
      if (subconv::locflag == 'O') {
        thread_data[n].s3_session.reset(new s3::Session(subconv_directives.
            obj_store.host, subconv_directives.obj_store.access_key,
            subconv_directives.obj_store.secret_key, subconv_directives.
            obj_store.region, subconv_directives.obj_store.terminal));
      }
    }
    std::thread thread_list[subconv::args.num_threads];

    // build the subset files
    vector<string> wget_list;
    long long size_input;
    size_t fcount = 0;
    bool is_temporal_subset = false;
    subconv::build_subset_files(input_files, multiple_parameter_files_code_set,
        thread_data, wget_list, size_input, fcount, is_temporal_subset);

    // deallocate memory
    for (size_t n = 0; n < subconv::args.num_threads; ++n) {
      thread_data[n].obuffer = nullptr;
    }

    // if the was a test run (not for sbatch info), then we are done
    if (subconv::args.is_test && subconv::args.batch_type == 0x0) {
      cout << "Success: " << subconv::timing_data.num_reads << " grids" << endl;
      return 0;
    }

    // re-connect to the database servers
    subconv::metadata_server.connect(metautils::directives.metadb_config);
    if (!subconv::metadata_server) {
      throw my::OpenFailed_Error("unable to connect to the metadata server "
          "after files built: '" + subconv::metadata_server.error() + "'");
    }
    subconv::rdadb_server.connect(metautils::directives.rdadb_config);
    if (!subconv::rdadb_server) {
      throw my::OpenFailed_Error("unable to connect to the RDADB server after "
          "files built: '" + subconv::rdadb_server.error() + "'");
    }

    // if this was an sbatch options run, we are done
    if (subconv::args.batch_type != 0x0) {
      cout << subconv::batch_options(subconv_directives) << endl;
      return 0;
    }

    // combine multiple files into one file for CSV output format
    if (subconv::request_values.ofmt == "csv") {
      fcount = subconv::combine_csv_files(wget_list, csv_data);
    }

    // create the download scripts
    subconv::create_download_scripts(wget_list, subconv_directives.dsrqst_root);

    // create the email notice to the user
    auto dsrqst_note = subconv::create_user_email_notice(parameter_mapper,
        level_mapper, unique_formats_map);

    // update subflag for temporal subsetting
    subconv::update_subflag_bit(2, subflag, &is_temporal_subset);

    // update RDADB
    subconv::update_rdadb(size_input, fcount, dsrqst_note, subflag);

    // done with the database servers, so disconnect
    subconv::metadata_server.disconnect();
    subconv::rdadb_server.disconnect();

    // if internal timings were turned on, print them
    if (subconv::args.get_timings) {
      subconv::print_timings();
    }

    // set the exit status code
    if (!myerror.empty()) {
      return 1;
    }
    return 0;
  }
  catch (std::exception& e) {
    std::cerr << e.what() << endl;
    return 1;
  }
}
