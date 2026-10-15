#include "flight_starrocks/flight_scan_reader.h"

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
  return 0;
}
