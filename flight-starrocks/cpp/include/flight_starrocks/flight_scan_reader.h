#pragma once

#include <functional>
#include <memory>
#include <string>

#include <arrow/flight/client.h>
#include <arrow/flight/sql/client.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <arrow/status.h>

namespace flight_starrocks {

// Selects the server where a Flight endpoint's opaque ticket can be redeemed.
arrow::flight::Location EndpointLocation(const arrow::flight::Location& origin,
                                         const arrow::flight::FlightEndpoint& endpoint);

// Streams Flight SQL record batches to a StarRocks backend scan adapter.
class FlightScanReader {
 public:
  using BatchConsumer = std::function<arrow::Status(const arrow::RecordBatch&)>;

  // Opens the Flight control connection; bearer_token is scoped to this scan.
  static arrow::Result<std::unique_ptr<FlightScanReader>> Connect(
      const arrow::flight::Location& location, const std::string& bearer_token);

  // Executes one read query and invokes consume for every batch of every endpoint.
  arrow::Status Scan(const std::string& query, const BatchConsumer& consume);

  // Redeems one FE-planned ticket assigned to this backend scanner.
  arrow::Status ScanTicket(const arrow::flight::Ticket& ticket,
                           const BatchConsumer& consume);

 private:
  FlightScanReader(arrow::flight::Location location,
                   std::shared_ptr<arrow::flight::FlightClient> client,
                   arrow::flight::FlightCallOptions options);

  arrow::flight::Location location_;
  std::shared_ptr<arrow::flight::FlightClient> client_;
  arrow::flight::FlightCallOptions options_;
};

}  // namespace flight_starrocks
