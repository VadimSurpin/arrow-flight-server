#include "flight_starrocks/flight_scan_reader.h"

#include <cassert>

int main() {
  auto origin = arrow::flight::Location::ForGrpcTcp("localhost", 8815).ValueOrDie();
  arrow::flight::FlightEndpoint endpoint;
  assert(flight_starrocks::EndpointLocation(origin, endpoint).ToString() ==
         origin.ToString());

  endpoint.locations.push_back(
      arrow::flight::Location::ForGrpcTls("worker.example.org", 8816).ValueOrDie());
  assert(flight_starrocks::EndpointLocation(origin, endpoint).ToString() ==
         endpoint.locations.front().ToString());

  auto reader = flight_starrocks::FlightScanReader::Connect(origin, "").ValueOrDie();
  arrow::flight::Ticket ticket;
  assert(!reader->ScanTicket(ticket, {}).ok());
  assert(!reader->Scan("", [](const arrow::RecordBatch&) {
    return arrow::Status::OK();
  }).ok());
  return 0;
}
