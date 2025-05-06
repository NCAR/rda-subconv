#include <subconv.hpp>
#include <PostgreSQL.hpp>
#include <strutils.hpp>

using namespace PostgreSQL;
using std::string;
using std::unordered_map;
using std::unordered_set;
using strutils::append;
using strutils::split;

namespace subconv {

void build_query_constructs(QueryData& query_data)
{
  if (!request_values.startdate.empty()) {
    if (request_values.dates_are_init) {
      append(query_data.conditions.inventory, "init_date", " and ");
    } else {
      append(query_data.conditions.inventory, "valid_date", " and ");
    }
    query_data.conditions.inventory += " >= '" + request_values.startdate + "'";
  }
  if (!request_values.enddate.empty()) {
    if (request_values.dates_are_init) {
      append(query_data.conditions.inventory, "init_date", " and ");
    } else {
      append(query_data.conditions.inventory, "valid_date", " and ");
    }
    query_data.conditions.inventory += " <= '" + request_values.enddate + "'";
  }
  if (!request_values.product.empty()) {
    if (request_values.product.find(",") != string::npos) {
      auto sp = split(request_values.product, ",");
      string tr_set;
      for (const auto& p : sp) {
          append(tr_set, p, ", ");
      }
      append(query_data.conditions.inventory, "time_range_code in (" + tr_set +
          ")", " and ");
      append(query_data.conditions.union_non_date, "time_range_code in (" +
          tr_set + ")", " and ");
    } else {
      append(query_data.conditions.inventory, "time_range_code = " +
          request_values.product, " and ");
      append(query_data.conditions.union_non_date, "time_range_code = " +
          request_values.product, " and ");
    }
  }
  if (!request_values.grid_definition.empty()) {
    if (request_values.grid_definition.find(",") != string::npos) {
      auto sp = split(request_values.grid_definition, ",");
      string g_set;
      for (const auto& p : sp) {
          append(g_set, p, ", ");
      }
      append(query_data.conditions.inventory, "grid_definition_code in (" +
          g_set + ")", " and ");
      append(query_data.conditions.union_non_date, "grid_definition_code in (" +
          g_set + ")", " and ");
    } else {
      append(query_data.conditions.inventory, "grid_definition_code = " +
          request_values.grid_definition, " and ");
      append(query_data.conditions.union_non_date, "grid_definition_code = " +
          request_values.grid_definition, " and ");
    }
  }
  query_data.conditions.union_ = query_data.conditions.inventory;
  if (!request_values.format_codes.empty()) {
    string s;
    for (const auto& format_code : request_values.format_codes) {
      append(s, "code = " + format_code, " or ");
    }
    append(query_data.conditions.format, "(" + s + ")", " and ");
  }
  unordered_map<string, string> level_map;
  if (!request_values.level.empty()) {
    if (request_values.level.find(",") != string::npos) {
      auto sp = split(request_values.level, ",");
      for (const auto& p : sp) {
        LocalQuery q("map", "WGrML.levels", "code = " + p);
        if (q.submit(metadata_server) < 0) {
            terminate("Error: database error - '" + q.error() + "'", "Error: " +
                q.error() + "\nQuery: " + q.show());
        }
        if (q.num_rows() == 0) {
            terminate("Error: bad request\nYour request:\n" + args.rinfo,
                "Error: no entry in WGrML.levels for " + p);
        }
        Row r;
        q.fetch_row(r);
        if (level_map.find(r[0]) == level_map.end()) {
          level_map.emplace(r[0], p);
        } else {
          level_map[r[0]] += "," + p;
        }
      }
    } else {
      append(query_data.conditions.union_, "level_code = " + request_values.
          level, " and ");
      append(query_data.conditions.union_non_date, "level_code = " +
          request_values.level, " and ");
      append(query_data.conditions.inventory, "level_code = " + request_values.
          level, " and ");
    }
  }
  if (!request_values.parameters.empty()) {
    unordered_set<string> unique_level_set;
    string level_conditions;
    for (const auto& parameter : request_values.parameters) {
      append(query_data.union_query, "select distinct file_code from \"IGrML\"."
          + request_values.metadata_dsid + "_inventory_" + parameter_code(
          metadata_server, parameter), " union ");
      if (!level_map.empty()) {
        level_conditions = "";
        auto level_key = strutils::token(parameter, ".", 0);
        if (level_key.find("!") != string::npos) {
          level_key = strutils::token(level_key, "!", 1);
        }
        if (level_map.find(level_key) != level_map.end()) {
          auto sp = split(level_map[level_key], ",");
          for (const auto& p : sp) {
            append(level_conditions, "level_code = " + p, " or ");
            if (unique_level_set.find(p) == unique_level_set.end()) {
              append(query_data.conditions.level, "level_code = " + p, " or ");
              unique_level_set.emplace(p);
            }
          }
          level_conditions = "(" + level_conditions + ")";
          if (!query_data.conditions.union_.empty()) {
            level_conditions = " and " + level_conditions;
          }
        }
      }
      if (!query_data.conditions.union_.empty() || !level_conditions.empty()) {
        query_data.union_query += " where ";
      }
      if (!query_data.conditions.union_.empty()) {
        query_data.union_query += query_data.conditions.union_;
      }
      if (!level_conditions.empty()) {
        query_data.union_query += level_conditions;
      }
    }
    if (!query_data.conditions.level.empty()) {
      query_data.conditions.level = "(" + query_data.conditions.level + ")";
      if (!query_data.conditions.union_.empty()) {
        query_data.conditions.level = " and " + query_data.conditions.level;
      }
    }
  }
  if (!query_data.conditions.level.empty()) {
    query_data.conditions.union_ += query_data.conditions.level;
  }
}

} // end namespace subconv
