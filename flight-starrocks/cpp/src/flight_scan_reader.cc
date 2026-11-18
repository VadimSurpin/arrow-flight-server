#include "flight_starrocks/flight_scan_reader.h"

#include <utility>

namespace flight_starrocks {

arrow::flight::Location EndpointLocation(const arrow::flight::Location& origin,
                                         const arrow::flight::FlightEndpoint& endpoint) {
  if (endpoint.locations.empty() ||
      endpoint.locations.front().ToString() ==
          arrow::flight::Location::ReuseConnection().ToString()) {
    return origin;
  }
  return endpoint.locations.front();
}

FlightScanReader::FlightScanReader(arrow::flight::Location location,
                                   std::shared_ptr<arrow::flight::FlightClient> client,
                                   arrow::flight::FlightCallOptions options)
    : location_(std::move(location)),
      client_(std::move(client)),
      options_(std::move(options)) {}

arrow::Result<std::unique_ptr<FlightScanReader>> FlightScanReader::Connect(
    const arrow::flight::Location& location, const std::string& bearer_token) {
  ARROW_ASSIGN_OR_RAISE(auto owned_client, arrow::flight::FlightClient::Connect(location));
  std::shared_ptr<arrow::flight::FlightClient> client(std::move(owned_client));
  arrow::flight::FlightCallOptions options;
  if (!bearer_token.empty()) {
    options.headers.emplace_back("authorization", "Bearer " + bearer_token);
  }
  return std::unique_ptr<FlightScanReader>(
      new FlightScanReader(location, std::move(client), std::move(options)));
}

arrow::Status FlightScanReader::Scan(const std::string& query,
                                     const BatchConsumer& consume) {
  if (query.empty() || !consume) {
    return arrow::Status::Invalid("A scan requires a query and a batch consumer");
  }

  arrow::flight::sql::FlightSqlClient sql_client(client_);
  ARROW_ASSIGN_OR_RAISE(auto info, sql_client.Execute(options_, query));
  for (const auto& endpoint : info->endpoints()) {
    const auto location = EndpointLocation(location_, endpoint);
    auto stream_client = client_;
    if (location.ToString() != location_.ToString()) {
      ARROW_ASSIGN_OR_RAISE(auto owned_client, arrow::flight::FlightClient::Connect(location));
      stream_client = std::shared_ptr<arrow::flight::FlightClient>(std::move(owned_client));
    }
    FlightScanReader endpoint_reader(location, stream_client, options_);
    ARROW_RETURN_NOT_OK(endpoint_reader.ScanTicket(endpoint.ticket, consume));
  }
  return arrow::Status::OK();
}

TicketStream::TicketStream(std::unique_ptr<arrow::flight::FlightStreamReader> reader)
    : reader_(std::move(reader)) {}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> TicketStream::Next() {
  while (true) {
    auto chunk = reader_->Next();
    if (!chunk.ok()) {
      return arrow::Status::IOError("Flight stream failed: ", chunk.status().message());
    }
    if (!chunk->data && !chunk->app_metadata) {
      return std::shared_ptr<arrow::RecordBatch>();
    }
    if (chunk->data) {
      return chunk->data;
    }
  }
}

void TicketStream::Cancel() { reader_->Cancel(); }

arrow::Result<std::unique_ptr<TicketStream>> FlightScanReader::OpenTicket(
    const arrow::flight::Ticket& ticket) {
  auto reader = client_->DoGet(options_, ticket);
  if (!reader.ok()) {
    return arrow::Status::IOError("Flight DoGet failed at ", location_.ToString(), ": ",
                                  reader.status().message());
  }
  return std::make_unique<TicketStream>(std::move(*reader));
}

arrow::Status FlightScanReader::ScanTicket(const arrow::flight::Ticket& ticket,
                                           const BatchConsumer& consume) {
  if (!consume) {
    return arrow::Status::Invalid("A scan requires a batch consumer");
  }
  ARROW_ASSIGN_OR_RAISE(auto reader, client_->DoGet(options_, ticket));
  while (true) {
    ARROW_ASSIGN_OR_RAISE(auto chunk, reader->Next());
    if (!chunk.data && !chunk.app_metadata) {
      break;
    }
    if (chunk.data) {
      ARROW_RETURN_NOT_OK(consume(*chunk.data));
    }
  }
  return arrow::Status::OK();
}

}  // namespace flight_starrocks
