#include "flight_starrocks/flight_scan_reader.h"

#include <cstdlib>
#include <string>

int main() {
  auto origin = arrow::flight::Location::ForGrpcTcp("localhost", 8815).ValueOrDie();
  arrow::flight::FlightEndpoint endpoint;
  if (flight_starrocks::EndpointLocation(origin, endpoint).ToString() !=
      origin.ToString()) {
    return 1;
  }

  endpoint.locations.push_back(arrow::flight::Location::ReuseConnection());
  if (flight_starrocks::EndpointLocation(origin, endpoint).ToString() !=
      origin.ToString()) {
    return 1;
  }
  endpoint.locations.clear();

  endpoint.locations.push_back(
      arrow::flight::Location::ForGrpcTls("worker.example.org", 8816).ValueOrDie());
  if (flight_starrocks::EndpointLocation(origin, endpoint).ToString() !=
      endpoint.locations.front().ToString()) {
    return 1;
  }

  auto reader = flight_starrocks::FlightScanReader::Connect(origin, "").ValueOrDie();
  arrow::flight::Ticket ticket;
  if (reader->ScanTicket(ticket, {}).ok()) {
    return 1;
  }
  if (reader->Scan("", [](const arrow::RecordBatch&) {
        return arrow::Status::OK();
      }).ok()) {
    return 1;
  }

  const char* host = std::getenv("FLIGHT_IT_HOST");
  const char* port = std::getenv("FLIGHT_IT_PORT");
  const char* query = std::getenv("FLIGHT_IT_QUERY");
  if (host != nullptr || port != nullptr || query != nullptr) {
    if (host == nullptr || port == nullptr || query == nullptr) {
      return 1;
    }
    auto live_location = arrow::flight::Location::ForGrpcTcp(
        host, static_cast<int>(std::stoi(port))).ValueOrDie();
    auto live_reader = flight_starrocks::FlightScanReader::Connect(live_location, "");
    if (!live_reader.ok()) {
      return 1;
    }
    int64_t rows = 0;
    auto status = live_reader.ValueOrDie()->Scan(query,
        [&rows](const arrow::RecordBatch& batch) {
          rows += batch.num_rows();
          return arrow::Status::OK();
        });
    if (!status.ok() || rows == 0) {
      return 1;
    }
  }
  return 0;
}
