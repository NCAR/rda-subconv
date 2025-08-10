#ifndef SUBCONV_H
#define   SUBCONV_H

#include <list>
#include <vector>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <PostgreSQL.hpp>
#include <gridutils.hpp>
#include <timer.hpp>
#include <s3.hpp>

namespace subconv {

struct Directives {
  Directives() : dsrqst_root(), dataset_block(), pbs_options(), host_restrict(),
      obj_store(), data_root(), db_config() { }

  std::string dsrqst_root, dataset_block, pbs_options;
  std::vector<std::string> host_restrict;
  struct ObjectStore {
    ObjectStore() : host(), access_key(), secret_key(), region(), terminal() { }
    void fill(std::string h, std::string ak, std::string sk, std::string r,
        std::string t) { host = h; access_key = ak; secret_key = sk; region = r;
        terminal = t; }

    std::string host, access_key, secret_key, region, terminal;
  } obj_store;
  std::string data_root;
  PostgreSQL::DBconfig db_config;
};

struct Args {
  Args() : SHARE_DIRECTORY(
      "/glade/campaign/collections/rda/work/rdadata/share"), num_threads(0),
      rinfo(), rqst_index(), download_directory(), batch_type(0x0),
      main_timer(), db_timer(), is_test(false), ignore_volume(false),
      ignore_restrictions(false), get_timings(false) { }

  const std::string SHARE_DIRECTORY;
  size_t num_threads;
  std::string rinfo, rqst_index, download_directory;
  char batch_type;
  Timer main_timer, db_timer;
  bool is_test, ignore_volume, ignore_restrictions, get_timings;
};

struct RequestValues {
  RequestValues() : metadata_dsid(), startdate(),enddate(), parameters(),
      format_codes(), product(), grid_definition(), level(), tindex(), ofmt(),
      nlat(99999.), slat(-99999.), wlon(-99999.), elon(99999.), ladiff(-99.),
      lodiff(-99.), lat_s(), lon_s(), topt_mo{ false, false, false, false,
      false, false, false, false, false, false, false, false, false },
      dates_are_init(false), ststep(false), ancillary() { }

  std::string metadata_dsid;
  std::string startdate, enddate;
  std::vector<std::string> parameters, format_codes;
  std::string product, grid_definition, level;
  std::string tindex, ofmt;
  float nlat, slat, wlon, elon;
  float ladiff, lodiff;
  std::string lat_s, lon_s;
  std::vector<bool> topt_mo;
  bool dates_are_init, ststep;
  struct Ancillary {
    Ancillary() : user_email(), user_country(), compression(), location(),
        tarflag() { }

    std::string user_email, user_country, compression, location, tarflag;
  } ancillary;
};

class TimingData
{
public:
  TimingData() : thread(0.), db(0.), read(0.), write(0.), grib2u(0.),
      grib2c(0.), nc(0.), read_bytes(0), num_reads(0) { }

  void add(const TimingData& source) {
    thread += source.thread;
    db += source.db;
    read += source.read;
    write += source.write;
    grib2u += source.grib2u;
    grib2c += source.grib2c;
    nc += source.nc;
    read_bytes += source.read_bytes;
    num_reads += source.num_reads;
  }
  void reset() {
    thread = db = read = write = grib2u = grib2c = nc = 0.;
    read_bytes = num_reads = 0;
  }

  double thread, db, read, write, grib2u, grib2c, nc;
  long long read_bytes;
  int num_reads;
};

struct QueryData {
  struct Conditions {
    Conditions() : format(), union_(), union_non_date(), level(),
        inventory() { }

    std::string format, union_, union_non_date, level, inventory;
  };

  QueryData() : conditions(), union_query() { }

  Conditions conditions;
  std::string union_query;
};

struct CSVData {
  CSVData() : parameter(), level() { }

  std::string parameter, level;
};

const size_t OBUFFER_LENGTH = 2000000;
struct ThreadData {
  ThreadData() : file_code(), file_id(), data_format(), data_format_code(),
      output_format(), webhome(),filename(), uConditions(),
      uConditions_no_dates(), wget_filenames(), insert_filenames(),
      multi_set(nullptr), include_parameter_codes_set(nullptr),
      filelist_display_order(0), f_attach(), size_input(0), fcount(0),
      parameter_mapper(nullptr), timing_data(), write_bytes(0),
      obuffer(nullptr), s3_session(nullptr), has_started(false),
      has_finished(false) { }

  std::string file_code, file_id, data_format, data_format_code, output_format;
  std::string webhome, filename, uConditions, uConditions_no_dates;
  std::list<std::string> wget_filenames, insert_filenames;
  std::shared_ptr<std::unordered_set<std::string>> multi_set,
      include_parameter_codes_set;
  size_t filelist_display_order;
  std::string f_attach;
  long long size_input;
  size_t fcount;
  std::shared_ptr<xmlutils::ParameterMapper> parameter_mapper;
  subconv::TimingData timing_data;
  long long write_bytes;
  std::unique_ptr<unsigned char[]> obuffer;
  std::shared_ptr<s3::Session> s3_session;
  bool has_started, has_finished;
};

class InputDataSource
{
public:
  enum class Type {_NULL, _POSIX, _S3};

  InputDataSource() : type(Type::_NULL), posix(), s3(), read_buffer(nullptr),
      BUF_LEN(0) { }
  unsigned char *get() const { return read_buffer.get(); }
  void initialize(std::string posix_filename);
  void initialize(std::shared_ptr<s3::Session>& s3_session, std::string bucket,
      std::string key);
  void read(off_t offset, size_t num_bytes);

private:
  Type type;
  struct Posix {
    Posix() : filename(), ifs(nullptr) { }

    std::string filename;
    std::unique_ptr<std::ifstream> ifs;
  } posix;
  struct S3 {
    S3() : bucket(), key(), session(nullptr) { }

    std::string bucket,key;
    std::shared_ptr<s3::Session> session;
  } s3;
  std::unique_ptr<unsigned char[]> read_buffer;
  size_t BUF_LEN;
};

struct SortData {
  SortData() : m_datetime{ }, m_lvl{ }, m_param{ }, m_offset{ },
      m_num_bytes{ } { }
  SortData(DateTime datetime, size_t lvl, size_t param, off_t offset,
      size_t num_bytes) : SortData() {
    m_datetime = datetime;
    m_lvl = lvl;
    m_param = param;
    m_offset = offset;
    m_num_bytes = num_bytes;
  }

  DateTime m_datetime;
  size_t m_lvl, m_param;
  off_t m_offset;
  size_t m_num_bytes;
};

/* InputFile is a tuple containing input file information:
**   file code from database
**   file id (relative path)
**   data size in bytes
**   data data format
**   data format code from database
*/
typedef std::tuple<std::string, std::string, long long, std::string,
    std::string> InputFile;

extern Args args;
extern RequestValues request_values;
extern TimingData timing_data;
extern PostgreSQL::Server metadata_server, rdadb_server;
extern char locflag;

extern "C" void clean_up();

extern void build_query_constructs(QueryData& query_data);
extern void build_subset_files(const std::vector<InputFile>& input_files,
    const std::unordered_set<std::string>& multiple_parameter_files_code_set,
    ThreadData *thread_data, std::vector<std::string>& wget_list, long long&
    size_input, size_t& fcount, bool& is_temporal_subset);
extern void check_usage(int argc);
extern void create_download_scripts(const std::vector<std::string>& wget_list,
    std::string dsrqst_root);
extern void do_conversion(ThreadData& thread_data);
extern void do_grid_fixups();
extern void fill_ancillary_request_values();
extern void get_chunk(std::ifstream& ifs, off_t offset, size_t num_bytes,
    std::unique_ptr<unsigned char[]>& buffer, size_t& BUF_LEN);
extern void get_chunk(s3::Session& session, std::string bucket, std::string key,
    off_t offset, size_t num_bytes, std::unique_ptr<unsigned char[]>& buffer,
    size_t& BUF_LEN);
extern void insert_into_wfrqst(PostgreSQL::Server& server, std::string
    request_index, std::string filename, std::string data_format, size_t
    filelist_display_order);
extern void parse_args(int argc, char **argv);
extern void parse_subset_request(std::string dataset_block, int& num_parameters,
    short& subflag, std::unordered_map<std::string, std::string>&
    unique_formats_map, std::shared_ptr<std::unordered_set<std::string>>&
    include_parameter_codes_set);
extern void print_timings();
extern void set_fcount(std::string request_index, size_t fcount);
extern void sort_to_nc_order(std::string input_filename, std::string
    output_filename);
extern void terminate(std::string stdout_message, std::string stderr_message);
extern void update_subflag_bit(short bit, short& subflag, void *data);
extern void update_rdadb(long long size_input, size_t fcount, std::string
    dsrqst_note, short subflag);

extern size_t combine_csv_files(std::vector<std::string>& wget_list, const
    CSVData& csv_data);

extern std::string create_user_email_notice(xmlutils::ParameterMapper&
    parameter_mapper, xmlutils::LevelMapper& level_mapper,
    std::unordered_map<std::string, std::string>& unique_formats_map);
extern std::string batch_options();
extern std::string parameter_code(PostgreSQL::Server& server, std::string
    parameter);

extern std::unordered_set<std::string> multiple_parameter_files(const
    std::vector<InputFile>& input_files, std::string inventory_conditions);

extern std::vector<InputFile> input_files(const QueryData& query_data);

extern CSVData csv_data(xmlutils::ParameterMapper& parameter_mapper,
    xmlutils::LevelMapper& level_mapper, std::unordered_map<std::string,
    std::string>& unique_formats_map);

extern Directives read_config();

extern bool ignore_volume();
extern bool is_selected_parameter(const ThreadData& thread_data, Grid *grid);

} // end namespace subconv

#endif
